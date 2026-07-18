// Package chain maintains a rolling window of recent chain state.
// This is the core data structure that enables fast header/block relay.
package chain

import (
	"sync"

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

const defaultRingSize = 256

// Store is a thread-safe rolling buffer of recent chain state.
// When new headers arrive from upstream, it notifies all subscribers
// so downstream ChainSync servers can forward them immediately.
type Store struct {
	mu      sync.RWMutex
	headers []Header   // ring buffer of recent headers
	blocks  map[string]Block // hash (hex) → block

	tip      Point
	tipBlock uint64

	// Subscribers waiting for new headers
	subsMu sync.Mutex
	subs   []chan struct{}
}

// New creates a new Store.
func New() *Store {
	return &Store{
		headers: make([]Header, 0, defaultRingSize),
		blocks:  make(map[string]Block),
	}
}

// Tip returns the current chain tip.
func (s *Store) Tip() (Point, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tip, s.tipBlock
}

// AddHeader adds a new block header, advancing the tip.
// It notifies all waiting subscribers immediately.
func (s *Store) AddHeader(h Header) {
	s.mu.Lock()
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
func (s *Store) AddBlock(b Block) {
	s.mu.Lock()
	key := hashKey(b.Point.Hash)
	s.blocks[key] = b
	// Prune blocks that are more than 256 slots behind tip
	if len(s.blocks) > defaultRingSize*2 {
		// Simple cleanup: remove oldest entries
		// In production we'd track insertion order
		for k := range s.blocks {
			delete(s.blocks, k)
			if len(s.blocks) <= defaultRingSize {
				break
			}
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
