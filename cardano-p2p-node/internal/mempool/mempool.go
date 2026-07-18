// Package mempool implements an in-memory transaction pool.
package mempool

import (
	"encoding/hex"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardano-p2p-node/internal/cardano"
)

// TxID uniquely identifies a transaction.
type TxID []byte

func (id TxID) String() string { return hex.EncodeToString(id) }

// TxEntry holds a transaction and its metadata.
type TxEntry struct {
	ID          TxID
	Size        uint32
	Raw         cbor.RawMessage
	ReceivedAt  time.Time          // when we added this tx (UTC)
	FromPeer    string             // remote addr of the peer that provided this tx
	Parsed      *cardano.ParsedTx  // decoded transaction fields; nil if unparseable
}

// Mempool is a thread-safe in-memory transaction pool.
// Transactions are stored in insertion order (FIFO) for serving peers.
type Mempool struct {
	mu      sync.RWMutex
	txs     map[string]*TxEntry // txid hex → entry
	order   []string            // insertion order (txid hex)
	maxSize int

	// onAdd is called (in the calling goroutine) when a new tx is added.
	// Used for logging; must be non-blocking.
	onAdd func(*TxEntry)

	// Subscribers waiting for new transactions
	subsMu sync.Mutex
	subs   []chan struct{}
}

const defaultMaxSize = 10000

// New creates a new Mempool.
func New() *Mempool {
	return &Mempool{
		txs:     make(map[string]*TxEntry),
		maxSize: defaultMaxSize,
	}
}

// SetOnAdd installs a hook called synchronously whenever a new transaction
// is added. Must not block. Pass nil to remove.
func (m *Mempool) SetOnAdd(fn func(*TxEntry)) {
	m.mu.Lock()
	m.onAdd = fn
	m.mu.Unlock()
}

// Add adds a transaction to the pool. Returns true if it was new.
// fromPeer identifies which peer provided this transaction (for logging).
func (m *Mempool) Add(entry *TxEntry) bool {
	key := entry.ID.String()
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.txs[key]; exists {
		return false
	}
	if entry.ReceivedAt.IsZero() {
		entry.ReceivedAt = time.Now().UTC()
	}
	// Parse CBOR fields if not already done
	if entry.Parsed == nil && len(entry.Raw) > 0 {
		entry.Parsed = cardano.Parse(entry.Raw)
	}

	// Evict oldest if at capacity
	if len(m.txs) >= m.maxSize {
		oldest := m.order[0]
		m.order = m.order[1:]
		delete(m.txs, oldest)
	}

	m.txs[key] = entry
	m.order = append(m.order, key)

	// Call hook while still holding the lock so the caller sees a consistent view.
	if m.onAdd != nil {
		m.onAdd(entry)
	}

	// Notify subscribers
	m.subsMu.Lock()
	for _, ch := range m.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	m.subsMu.Unlock()

	return true
}

// Has returns true if the tx is in the pool.
func (m *Mempool) Has(id TxID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.txs[id.String()]
	return ok
}

// Get returns a transaction by ID.
func (m *Mempool) Get(id TxID) (*TxEntry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.txs[id.String()]
	return e, ok
}

// GetAll returns a snapshot of all transactions in insertion order.
func (m *Mempool) GetAll() []*TxEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*TxEntry, 0, len(m.order))
	for _, key := range m.order {
		if e, ok := m.txs[key]; ok {
			out = append(out, e)
		}
	}
	return out
}

// TxIDsAfter returns up to n txids that come after the given index.
// Returns the txids and the new index to continue from.
func (m *Mempool) TxIDsAfter(afterIdx int, n int) ([]*TxEntry, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if afterIdx >= len(m.order) {
		return nil, afterIdx
	}

	end := afterIdx + n
	if end > len(m.order) {
		end = len(m.order)
	}

	result := make([]*TxEntry, 0, end-afterIdx)
	for _, key := range m.order[afterIdx:end] {
		if e, ok := m.txs[key]; ok {
			result = append(result, e)
		}
	}
	return result, end
}

// Size returns the number of transactions in the pool.
func (m *Mempool) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.txs)
}

// Subscribe returns a channel that signals when new txs arrive.
func (m *Mempool) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	m.subsMu.Lock()
	m.subs = append(m.subs, ch)
	m.subsMu.Unlock()
	return ch
}

// Unsubscribe removes a subscription.
func (m *Mempool) Unsubscribe(ch chan struct{}) {
	m.subsMu.Lock()
	defer m.subsMu.Unlock()
	for i, c := range m.subs {
		if c == ch {
			m.subs = append(m.subs[:i], m.subs[i+1:]...)
			return
		}
	}
}
