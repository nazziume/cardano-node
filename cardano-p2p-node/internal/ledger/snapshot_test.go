package ledger_test

import (
	"testing"

	"github.com/cardano-p2p-node/internal/ledger"
)

const snapshotV2 = `{
  "version": 2,
  "slotNo": 138450210,
  "bigLedgerPools": [
    {
      "accumulatedStake": 0.002345,
      "relativeStake":    0.001234,
      "relays": [
        { "address": "relay1.mypool.io", "port": 3001 },
        { "address": "1.2.3.4",          "port": 3002 }
      ]
    },
    {
      "accumulatedStake": 0.005000,
      "relativeStake":    0.002655,
      "relays": [
        { "address": "5.6.7.8", "port": 6000 }
      ]
    }
  ]
}`

const snapshotV23Big = `{
  "NodeToClientVersion": 23,
  "NetworkMagic": 764824073,
  "Point": { "tag": "BlockPoint", "slot": 138450210, "hash": "abcdef" },
  "bigLedgerPools": [
    {
      "accumulatedStake": 0.003,
      "relativeStake": 0.003,
      "relays": [
        { "address": "relay.v23pool.com", "port": 3001 }
      ]
    }
  ],
  "allLedgerPools": [
    {
      "relativeStake": 0.001,
      "relays": [
        { "address": "all.relay.com", "port": 3000 }
      ]
    }
  ]
}`

func TestParseSnapshotV2(t *testing.T) {
	snap, err := ledger.ParseSnapshot([]byte(snapshotV2))
	if err != nil {
		t.Fatalf("parse v2: %v", err)
	}
	if snap.Version != 2 {
		t.Errorf("version: got %d, want 2", snap.Version)
	}
	if snap.SlotNo != 138450210 {
		t.Errorf("slotNo: got %d, want 138450210", snap.SlotNo)
	}
	if len(snap.BigPools) != 2 {
		t.Fatalf("bigPools count: got %d, want 2", len(snap.BigPools))
	}

	// First pool
	p0 := snap.BigPools[0]
	if len(p0.Relays) != 2 {
		t.Errorf("pool[0] relays: got %d, want 2", len(p0.Relays))
	}
	if p0.Relays[0].Address != "relay1.mypool.io" {
		t.Errorf("relay[0] address: got %q", p0.Relays[0].Address)
	}
	if p0.Relays[0].Port != 3001 {
		t.Errorf("relay[0] port: got %d, want 3001", p0.Relays[0].Port)
	}
	if p0.Relays[1].Address != "1.2.3.4" {
		t.Errorf("relay[1] address: got %q", p0.Relays[1].Address)
	}

	// Second pool
	p1 := snap.BigPools[1]
	if len(p1.Relays) != 1 || p1.Relays[0].Address != "5.6.7.8" {
		t.Errorf("pool[1]: unexpected relays %+v", p1.Relays)
	}
}

func TestParseSnapshotV23(t *testing.T) {
	snap, err := ledger.ParseSnapshot([]byte(snapshotV23Big))
	if err != nil {
		t.Fatalf("parse v23: %v", err)
	}
	if snap.Version != 23 {
		t.Errorf("version: got %d, want 23", snap.Version)
	}
	if len(snap.BigPools) != 1 {
		t.Errorf("bigPools: got %d, want 1", len(snap.BigPools))
	}
	if len(snap.AllPools) != 1 {
		t.Errorf("allPools: got %d, want 1", len(snap.AllPools))
	}
	if snap.BigPools[0].Relays[0].Address != "relay.v23pool.com" {
		t.Errorf("bigPool relay: got %q", snap.BigPools[0].Relays[0].Address)
	}
	if snap.AllPools[0].Relays[0].Address != "all.relay.com" {
		t.Errorf("allPool relay: got %q", snap.AllPools[0].Relays[0].Address)
	}
}

func TestParseSnapshotUnknownVersion(t *testing.T) {
	bad := `{"version": 99, "slotNo": 0, "bigLedgerPools": []}`
	_, err := ledger.ParseSnapshot([]byte(bad))
	if err == nil {
		t.Error("expected error for unsupported version")
	}
}

func TestParseSnapshotMalformed(t *testing.T) {
	_, err := ledger.ParseSnapshot([]byte(`not json`))
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestParseSnapshotSkipsEmptyRelays(t *testing.T) {
	data := `{
	  "version": 2,
	  "slotNo": 100,
	  "bigLedgerPools": [
	    { "relativeStake": 0.001, "accumulatedStake": 0.001,
	      "relays": [{ "address": "", "port": 3001 }] },
	    { "relativeStake": 0.002, "accumulatedStake": 0.003,
	      "relays": [{ "address": "1.2.3.4", "port": 3001 }] }
	  ]
	}`
	snap, err := ledger.ParseSnapshot([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	// Pool with empty address should be skipped (no relays → pool skipped)
	if len(snap.BigPools) != 1 {
		t.Errorf("expected 1 valid pool, got %d", len(snap.BigPools))
	}
}

func TestParseSnapshotStakeValues(t *testing.T) {
	snap, _ := ledger.ParseSnapshot([]byte(snapshotV2))
	if snap.BigPools[0].RelativeStake != 0.001234 {
		t.Errorf("relativeStake: got %f", snap.BigPools[0].RelativeStake)
	}
	if snap.BigPools[0].AccumulatedStake != 0.002345 {
		t.Errorf("accumulatedStake: got %f", snap.BigPools[0].AccumulatedStake)
	}
}
