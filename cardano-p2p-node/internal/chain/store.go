// Package chain maintains a rolling window of recent chain state.
// This is the core data structure that enables fast header/block relay.
package chain

import (
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// Point represents a position on the chain.
type Point struct {
	SlotNo uint64
	Hash   []byte // block header hash, nil for origin
}

// IsOrigin returns true for the genesis / origin point.
func (p Point) IsOrigin() bool { return p.Hash == nil }

// Header is a stored block header with its raw CBOR encoding.
type Header struct {
	Point  Point
	Raw    cbor.RawMessage // raw CBOR of the header as received
	TipRaw cbor.RawMessage // raw CBOR of the tip as received
	No     uint64          // block number
}

// Block is a stored full block body.
type Block struct {
	Point Point
	Raw   cbor.RawMessage
}

// defaultRingSize is the number of recent headers to retain.
// 2160 matches Cardano's security parameter k (the finality depth on mainnet).
// A larger ring means:
//   - More candidates in MsgFindIntersect → intersection succeeds with more peers → fewer genesis rollbacks
//   - ChainSync server can serve deeper headers to slow inbound peers
//   - Each header is ~500 B, so 2160 headers ≈ 1 MB RAM
const defaultRingSize = 2160

// RollbackEvent is broadcast to downstream ChainSync servers on a chain reorg.
type RollbackEvent struct {
	Point   Point
	TipRaw  cbor.RawMessage
	BlockNo uint64
}

// Store is a thread-safe rolling buffer of recent chain state.
// When new headers arrive from upstream, it notifies all subscribers
// so downstream ChainSync servers can forward them immediately.
//
// Only the last `defaultRingSize` headers are kept in memory.
// Block bodies are kept for only the last `maxBlockCache` headers.
// No historical data is persisted — this is purely a relay cache.
type Store struct {
	mu      sync.RWMutex
	headers []Header         // ring buffer of recent headers (max defaultRingSize)
	blocks  map[string]Block // hash → block body (only recent blocks)

	tip      Point
	tipBlock uint64

	// maxBlockCache controls how many recent block bodies to keep.
	maxBlockCache int

	// Subscribers waiting for new headers (forward signal)
	subsMu   sync.Mutex
	subs     []chan struct{}
	rollSubs []chan RollbackEvent

	// lastGenesisRollback rate-limits genesis rollbacks.
	// With 50 outbound peers all reconnecting simultaneously, each one
	// sends MsgRollBackward(origin), wiping the ring buffer before the
	// previous peer's fast-forward sync has accumulated enough headers
	// for subsequent peers to find an intersection.
	// A short cooldown (5s) allows the first peer to stream ~thousands
	// of headers into the ring before other peers can wipe it again.
	lastGenesisRollback time.Time
}

// defaultMaxBlockCache is the number of recent block bodies to keep.
// 256 blocks × ~50 KB average ≈ 12.8 MB RAM.
// A larger cache directly improves fetchynessBlocks peer quality scores:
// inbound peers requesting any of the last 256 blocks will be served immediately
// rather than receiving MsgNoBlocks and scoring us zero for that request.
const defaultMaxBlockCache = 256

// New creates a new Store.
func New() *Store {
	return &Store{
		headers:       make([]Header, 0, defaultRingSize),
		blocks:        make(map[string]Block),
		maxBlockCache: defaultMaxBlockCache,
	}
}

// Tip returns the current chain tip.
func (s *Store) Tip() (Point, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip, s.tipBlock
}

// CurrentSlot returns the chain tip slot (0 if unknown).
func (s *Store) CurrentSlot() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip.SlotNo
}

// genesisRollbackCooldown prevents cascading genesis rollbacks when many
// peers reconnect simultaneously. 5 seconds is enough for the first peer to
// stream thousands of headers so subsequent peers can find an intersection.
const genesisRollbackCooldown = 5 * time.Second

// Rollback truncates the header ring to the given exact point (slot + hash)
// and cleans the block cache of entries beyond that point.
// All downstream ChainSync servers are notified so they issue MsgRollBackward.
//
// Returns false if a genesis rollback was suppressed by the rate-limiter.
// The caller (ChainSyncClient) should close the peer session in that case;
// the peer manager will reconnect, and by then the ring has enough headers
// for intersection to succeed without another genesis rollback.
func (s *Store) Rollback(pt Point, tipRaw cbor.RawMessage, tipBlockNo uint64) bool {
	s.mu.Lock()

	if pt.IsOrigin() {
		now := time.Now()
		if !s.lastGenesisRollback.IsZero() && now.Sub(s.lastGenesisRollback) < genesisRollbackCooldown {
			s.mu.Unlock()
			return false // rate-limited; caller should close this peer session
		}
		s.lastGenesisRollback = now
	}

	// Keep only headers that precede the rollback point.
	// Headers AT the exact rollback point are also kept (inclusive).
	// Headers at the same slot but a different hash (orphan fork) are dropped.
	newHeaders := s.headers[:0]
	for _, h := range s.headers {
		if h.Point.SlotNo < pt.SlotNo {
			newHeaders = append(newHeaders, h)
			continue
		}
		if h.Point.SlotNo == pt.SlotNo && hashKey(h.Point.Hash) == hashKey(pt.Hash) {
			newHeaders = append(newHeaders, h)
		}
		// Headers past the rollback slot (or at the same slot with wrong hash)
		// are rolled back; stop here.
		break
	}
	s.headers = newHeaders

	// Clean block bodies for headers that were rolled back.
	keepHashes := make(map[string]bool, len(newHeaders))
	for _, h := range newHeaders {
		keepHashes[hashKey(h.Point.Hash)] = true
	}
	for k := range s.blocks {
		if !keepHashes[k] {
			delete(s.blocks, k)
		}
	}

	if pt.IsOrigin() {
		s.tip = Point{}
		s.tipBlock = 0
	} else {
		s.tip = pt
		s.tipBlock = tipBlockNo
	}
	evt := RollbackEvent{Point: pt, TipRaw: tipRaw, BlockNo: tipBlockNo}
	s.mu.Unlock()

	// Notify rollback subscribers
	s.subsMu.Lock()
	for _, ch := range s.rollSubs {
		select {
		case ch <- evt:
		default:
			// If the channel is full, overwrite with the latest event.
			// The receiver will see the most recent rollback point.
			select {
			case <-ch:
			default:
			}
			ch <- evt
		}
	}
	// Also wake forward-header subscribers (they need to re-sync their position)
	for _, ch := range s.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	s.subsMu.Unlock()
	return true
}

// AddHeader adds a new block header, advancing the tip.
// It notifies all waiting subscribers immediately.
//
// Deduplication: with 50 outbound peers all relaying the same new block
// header within milliseconds of each other, the same (slot, hash) pair
// would otherwise be appended 50 times.  Each duplicate causes the
// ChainSync server to send an extra MsgRollForward to inbound peers,
// wasting bandwidth and potentially confusing them.
//
// We skip the write (and skip subscriber notification) if the incoming
// header exactly matches the current tip — the common case when many
// peers simultaneously announce the same new block.
func (s *Store) AddHeader(h Header) {
	s.mu.Lock()

	// Deduplicate: skip if already at this exact (slot, hash) tip.
	if s.tip.SlotNo == h.Point.SlotNo && hashKey(s.tip.Hash) == hashKey(h.Point.Hash) {
		s.mu.Unlock()
		return // already have this header; suppress duplicate notification
	}

	if len(s.headers) >= defaultRingSize {
		// Drop oldest
		s.headers = s.headers[1:]
	}
	s.headers = append(s.headers, h)
	s.tip = h.Point
	s.tipBlock = h.No
	s.mu.Unlock()

	// Wake all subscribers
	s.subsMu.Lock()
	for _, ch := range s.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	s.subsMu.Unlock()
}

// AddBlock stores a full block body.
// Only the most recent maxBlockCache blocks are retained; older ones are evicted
// using the header ring buffer order (oldest header's block is removed first).
func (s *Store) AddBlock(b Block) {
	s.mu.Lock()
	key := hashKey(b.Point.Hash)
	s.blocks[key] = b

	// Evict blocks for headers that have scrolled out of our cache window.
	// We keep blocks only for the newest maxBlockCache headers.
	if len(s.blocks) > s.maxBlockCache {
		// Walk the header ring from the front (oldest) and delete their blocks
		// until we're within budget.
		for _, h := range s.headers {
			if len(s.blocks) <= s.maxBlockCache {
				break
			}
			oldKey := hashKey(h.Point.Hash)
			delete(s.blocks, oldKey)
		}
	}
	s.mu.Unlock()
}

// GetBlock returns a stored block by its hash, or false if not found.
func (s *Store) GetBlock(hash []byte) (Block, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[hashKey(hash)]
	return b, ok
}

// FindIntersect finds the most recent common point from a list of candidate points.
// Returns the found point and headers after it, or nil if none found.
func (s *Store) FindIntersect(candidates []Point) (found *Point, headersAfter []Header) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, cand := range candidates {
		if cand.IsOrigin() {
			// Origin always intersects; all headers are "after"
			p := cand
			return &p, s.headers
		}
		// Find this point in our ring
		for i, h := range s.headers {
			if h.Point.SlotNo == cand.SlotNo && hashKey(h.Point.Hash) == hashKey(cand.Hash) {
				p := h.Point
				return &p, s.headers[i+1:]
			}
		}
	}
	return nil, nil
}

// HeadersAfter returns all headers in our ring buffer that come after the given point.
// Returns all headers if point is origin.
func (s *Store) HeadersAfter(p Point) []Header {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if p.IsOrigin() {
		cp := make([]Header, len(s.headers))
		copy(cp, s.headers)
		return cp
	}
	for i, h := range s.headers {
		if h.Point.SlotNo == p.SlotNo && hashKey(h.Point.Hash) == hashKey(p.Hash) {
			cp := make([]Header, len(s.headers)-i-1)
			copy(cp, s.headers[i+1:])
			return cp
		}
	}
	return nil
}

// Subscribe returns a channel that receives a signal whenever a new header is added.
// The caller should call Unsubscribe when done.
func (s *Store) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	s.subsMu.Lock()
	s.subs = append(s.subs, ch)
	s.subsMu.Unlock()
	return ch
}

// Unsubscribe removes a subscription channel.
func (s *Store) Unsubscribe(ch chan struct{}) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for i, c := range s.subs {
		if c == ch {
			s.subs = append(s.subs[:i], s.subs[i+1:]...)
			return
		}
	}
}

// SubscribeRollback returns a channel that receives a RollbackEvent whenever
// the chain rolls back. The caller must call UnsubscribeRollback when done.
func (s *Store) SubscribeRollback() chan RollbackEvent {
	ch := make(chan RollbackEvent, 2)
	s.subsMu.Lock()
	s.rollSubs = append(s.rollSubs, ch)
	s.subsMu.Unlock()
	return ch
}

// UnsubscribeRollback removes a rollback subscription.
func (s *Store) UnsubscribeRollback(ch chan RollbackEvent) {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for i, c := range s.rollSubs {
		if c == ch {
			s.rollSubs = append(s.rollSubs[:i], s.rollSubs[i+1:]...)
			return
		}
	}
}

// AllHeaders returns a copy of all headers in the ring buffer.
func (s *Store) AllHeaders() []Header {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]Header, len(s.headers))
	copy(cp, s.headers)
	return cp
}

func hashKey(hash []byte) string {
	return string(hash)
}
