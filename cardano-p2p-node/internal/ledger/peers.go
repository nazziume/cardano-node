package ledger

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Config controls the ledger peers manager.
type Config struct {
	// SnapshotFile is a path to a local peer snapshot JSON file.
	// Checked on startup and on every refresh.
	SnapshotFile string

	// SnapshotURL is an HTTP(S) URL to fetch the snapshot from.
	// Used when SnapshotFile is empty or as a fallback.
	// Official mainnet snapshot:
	//   https://book.world.dev.cardano.org/environments/mainnet/peer-snapshot.json
	SnapshotURL string

	// RefreshInterval controls how often the snapshot is reloaded.
	// Default: 1 hour.
	RefreshInterval time.Duration

	// MaxPeers is the maximum number of relay addresses to yield per refresh.
	// Pools are sorted by RelativeStake descending; highest-stake pools go first.
	// Default: 100.
	MaxPeers int

	// DNSConcurrency is the number of concurrent DNS lookups.
	// Default: 20.
	DNSConcurrency int
}

func (c *Config) setDefaults() {
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = time.Hour
	}
	if c.MaxPeers <= 0 {
		c.MaxPeers = 100
	}
	if c.DNSConcurrency <= 0 {
		c.DNSConcurrency = 20
	}
}

// ResolvedPeer is a single endpoint ready to dial.
type ResolvedPeer struct {
	Addr          string  // "ip:port"
	RelativeStake float64 // fraction of total stake (0.0–1.0)
}

// candidate is a relay endpoint waiting to be DNS-resolved.
type candidate struct {
	relay Relay
	stake float64
}

// Manager periodically loads the ledger peer snapshot and resolves relay
// addresses, calling OnPeers with the resulting address list.
type Manager struct {
	cfg Config
	log *zap.Logger

	mu      sync.RWMutex
	current []ResolvedPeer // latest resolved peers, sorted by stake desc

	// OnPeers is called (in a new goroutine) after each successful refresh
	// with the full set of resolved peer addresses.
	OnPeers func(peers []ResolvedPeer)
}

// NewManager creates a LedgerPeers Manager.
func NewManager(cfg Config, log *zap.Logger) *Manager {
	cfg.setDefaults()
	return &Manager{cfg: cfg, log: log}
}

// Run starts the refresh loop. It performs an immediate refresh on startup,
// then repeats every RefreshInterval. Blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	// Immediate first refresh
	m.refresh(ctx)

	ticker := time.NewTicker(m.cfg.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.refresh(ctx)
		}
	}
}

// Peers returns the latest resolved peer list (snapshot, safe to read).
func (m *Manager) Peers() []ResolvedPeer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ResolvedPeer, len(m.current))
	copy(out, m.current)
	return out
}

// refresh loads the snapshot, resolves DNS, and updates the peer list.
func (m *Manager) refresh(ctx context.Context) {
	snap, err := m.loadSnapshot(ctx)
	if err != nil {
		m.log.Warn("ledger peers: failed to load snapshot", zap.Error(err))
		return
	}

	m.log.Info("ledger peers: snapshot loaded",
		zap.Int("version", snap.Version),
		zap.Uint64("slot", snap.SlotNo),
		zap.Int("bigPools", len(snap.BigPools)),
		zap.Int("allPools", len(snap.AllPools)),
	)

	// Use BigPools if available; fall back to AllPools.
	pools := snap.BigPools
	if len(pools) == 0 {
		pools = snap.AllPools
	}

	// Sort pools by relative stake descending so we connect to the most
	// representative pools first.
	sort.Slice(pools, func(i, j int) bool {
		return pools[i].RelativeStake > pools[j].RelativeStake
	})

	// Collect all relay endpoints, highest-stake pools first.
	var candidates []candidate
	for _, p := range pools {
		for _, r := range p.Relays {
			if r.Port == 0 {
				// SRV relay — skip for now (requires SRV DNS lookup)
				continue
			}
			candidates = append(candidates, candidate{relay: r, stake: p.RelativeStake})
		}
	}

	// Resolve DNS names concurrently, cap at MaxPeers results.
	resolved := m.resolveAll(ctx, candidates, m.cfg.MaxPeers)

	m.log.Info("ledger peers: refresh complete",
		zap.Int("resolved", len(resolved)),
		zap.Int("candidates", len(candidates)),
	)

	m.mu.Lock()
	m.current = resolved
	m.mu.Unlock()

	if m.OnPeers != nil && len(resolved) > 0 {
		go m.OnPeers(resolved)
	}
}

// loadSnapshot tries the local file first, then the remote URL.
func (m *Manager) loadSnapshot(ctx context.Context) (*Snapshot, error) {
	if m.cfg.SnapshotFile != "" {
		snap, err := LoadFromFile(m.cfg.SnapshotFile)
		if err == nil {
			return snap, nil
		}
		m.log.Warn("ledger peers: local file failed, trying URL",
			zap.String("file", m.cfg.SnapshotFile),
			zap.Error(err))
	}
	if m.cfg.SnapshotURL != "" {
		return FetchFromURL(ctx, m.cfg.SnapshotURL)
	}
	return nil, fmt.Errorf("ledger peers: no snapshot source configured (set snapshotFile or snapshotURL)")
}

// resolveAll resolves DNS names concurrently using a worker pool.
// Results are returned in stake-descending order (same as input candidates).
// At most maxPeers unique "ip:port" addresses are returned.
func (m *Manager) resolveAll(ctx context.Context, candidates []candidate, maxPeers int) []ResolvedPeer {
	type work struct {
		idx int
		c   candidate
	}
	type result struct {
		idx   int
		addrs []string // resolved "ip:port" addresses
		stake float64
	}

	workCh := make(chan work, len(candidates))
	for i, c := range candidates {
		workCh <- work{i, c}
	}
	close(workCh)

	resultCh := make(chan result, len(candidates))

	var wg sync.WaitGroup
	for i := 0; i < m.cfg.DNSConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				addrs := resolveRelay(ctx, w.c.relay)
				if len(addrs) > 0 {
					resultCh <- result{w.idx, addrs, w.c.stake}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect, deduplicate, preserve index order for stake sorting.
	type indexed struct {
		idx   int
		addrs []string
		stake float64
	}
	var all []indexed
	for r := range resultCh {
		all = append(all, indexed{idx: r.idx, addrs: r.addrs, stake: r.stake})
	}

	// Sort by original index (which is stake-descending from the sorted pools).
	sort.Slice(all, func(i, j int) bool { return all[i].idx < all[j].idx })

	seen := make(map[string]bool)
	var out []ResolvedPeer
	for _, item := range all {
		for _, addr := range item.addrs {
			if seen[addr] {
				continue
			}
			seen[addr] = true
			out = append(out, ResolvedPeer{Addr: addr, RelativeStake: item.stake})
			if len(out) >= maxPeers {
				return out
			}
		}
	}
	return out
}

// resolveRelay turns a Relay into a list of "ip:port" dial strings.
// For IP relays, returns immediately. For DNS names, performs a lookup.
func resolveRelay(ctx context.Context, r Relay) []string {
	portStr := fmt.Sprintf("%d", r.Port)

	// Check if address is already an IP
	if ip := net.ParseIP(r.Address); ip != nil {
		return []string{net.JoinHostPort(r.Address, portStr)}
	}

	// DNS lookup with context timeout
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ips, err := net.DefaultResolver.LookupHost(lookupCtx, r.Address)
	if err != nil {
		return nil
	}

	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip, portStr))
	}
	return out
}
