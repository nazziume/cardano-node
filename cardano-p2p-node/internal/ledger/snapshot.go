// Package ledger implements Cardano Ledger Peers discovery.
//
// Ledger peers are SPO (Stake Pool Operator) relay nodes whose addresses are
// registered on the Cardano blockchain. They are distributed as a JSON
// "peer snapshot" file that any node operator can download and use.
//
// Snapshot format (v2, the most widely used):
//
//	{
//	  "version": 2,
//	  "slotNo": 138450210,
//	  "bigLedgerPools": [
//	    {
//	      "accumulatedStake": 0.002345,
//	      "relativeStake":    0.001234,
//	      "relays": [
//	        { "address": "relay.mypool.io", "port": 3001 },
//	        { "address": "1.2.3.4",         "port": 3001 }
//	      ]
//	    }
//	  ]
//	}
//
// Also supports v23 format with "NodeToClientVersion":23 header.
//
// Source: Ouroboros.Network.PeerSelection.LedgerPeers.Type (ouroboros-network)
package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Relay is a single endpoint (IP or DNS name) for a stake pool.
type Relay struct {
	// Address is either an IPv4/IPv6 address string or a DNS hostname.
	// For SRV records, Address is the SRV domain and Port is 0.
	Address string
	Port    uint16
}

// Pool is one stake pool entry from the snapshot.
type Pool struct {
	AccumulatedStake float64 // cumulative stake fraction (for weighted selection)
	RelativeStake    float64 // this pool's own stake fraction
	Relays           []Relay
}

// Snapshot is a parsed ledger peer snapshot.
type Snapshot struct {
	Version  int    // 1, 2, or 23
	SlotNo   uint64 // chain slot at time of snapshot
	BigPools []Pool // high-stake pools (>= bigLedgerPeerQuota of total stake)
	AllPools []Pool // all registered pools (v23 only)
}

// ── JSON parsing ─────────────────────────────────────────────────────────────

// rawRelay mirrors the JSON relay object.
type rawRelay struct {
	Address string `json:"address"`
	Port    uint16 `json:"port"` // 0 for SRV
}

// rawPool mirrors a bigLedgerPools / allLedgerPools entry.
type rawPool struct {
	AccumulatedStake float64    `json:"accumulatedStake"`
	RelativeStake    float64    `json:"relativeStake"`
	Relays           []rawRelay `json:"relays"`
}

// rawSnapshotV2 is the v1/v2 format (fields: version, slotNo, bigLedgerPools).
type rawSnapshotV2 struct {
	Version      int       `json:"version"`
	SlotNo       uint64    `json:"slotNo"`
	BigPools     []rawPool `json:"bigLedgerPools"`
}

// rawSnapshotV23 is the v23 format (NodeToClientVersion: 23).
type rawSnapshotV23 struct {
	NtCVersion   int       `json:"NodeToClientVersion"`
	NetworkMagic uint32    `json:"NetworkMagic"`
	BigPools     []rawPool `json:"bigLedgerPools"`
	AllPools     []rawPool `json:"allLedgerPools"`
}

// ParseSnapshot decodes a ledger peer snapshot from JSON bytes.
// It handles both v2 and v23 formats transparently.
func ParseSnapshot(data []byte) (*Snapshot, error) {
	// Peek at the version discriminator
	var probe struct {
		Version    int `json:"version"`
		NtCVersion int `json:"NodeToClientVersion"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("ledger snapshot: probe version: %w", err)
	}

	switch {
	case probe.NtCVersion == 23:
		var raw rawSnapshotV23
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("ledger snapshot v23: %w", err)
		}
		return &Snapshot{
			Version:  23,
			BigPools: convertPools(raw.BigPools),
			AllPools: convertPools(raw.AllPools),
		}, nil

	case probe.Version == 1 || probe.Version == 2:
		var raw rawSnapshotV2
		if err := json.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("ledger snapshot v%d: %w", probe.Version, err)
		}
		return &Snapshot{
			Version:  raw.Version,
			SlotNo:   raw.SlotNo,
			BigPools: convertPools(raw.BigPools),
		}, nil

	default:
		return nil, fmt.Errorf("ledger snapshot: unsupported version (version=%d, ntc=%d)",
			probe.Version, probe.NtCVersion)
	}
}

func convertPools(raw []rawPool) []Pool {
	out := make([]Pool, 0, len(raw))
	for _, rp := range raw {
		relays := make([]Relay, 0, len(rp.Relays))
		for _, rr := range rp.Relays {
			if rr.Address == "" {
				continue
			}
			relays = append(relays, Relay{
				Address: rr.Address,
				Port:    rr.Port,
			})
		}
		if len(relays) == 0 {
			continue
		}
		out = append(out, Pool{
			AccumulatedStake: rp.AccumulatedStake,
			RelativeStake:    rp.RelativeStake,
			Relays:           relays,
		})
	}
	return out
}

// ── Loading ───────────────────────────────────────────────────────────────────

// LoadFromFile reads and parses a snapshot from a local file.
func LoadFromFile(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ledger snapshot: read file %q: %w", path, err)
	}
	return ParseSnapshot(data)
}

// FetchFromURL downloads and parses a snapshot from an HTTP/HTTPS URL.
func FetchFromURL(ctx context.Context, url string) (*Snapshot, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("ledger snapshot: build request: %w", err)
	}
	req.Header.Set("User-Agent", "cardano-p2p-node/1.0")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ledger snapshot: fetch %q: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ledger snapshot: HTTP %d from %q", resp.StatusCode, url)
	}

	// Cap at 32 MB to avoid OOM on malicious responses
	const maxBytes = 32 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return nil, fmt.Errorf("ledger snapshot: read body: %w", err)
	}
	return ParseSnapshot(data)
}
