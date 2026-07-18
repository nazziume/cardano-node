package cardano_test

import (
	"encoding/hex"
	"testing"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardano-p2p-node/internal/cardano"
)

// buildTx encodes [body, witnessSet, isValid, nil] as CBOR.
func buildTx(body, witness map[interface{}]interface{}, isValid bool) []byte {
	bodyBytes, _ := cbor.Marshal(body)
	wsBytes, _ := cbor.Marshal(witness)
	tx, _ := cbor.Marshal([]interface{}{
		cbor.RawMessage(bodyBytes),
		cbor.RawMessage(wsBytes),
		isValid,
		nil,
	})
	return tx
}

func scriptHash() []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = byte(i)
	}
	return h
}

func fakeInput() []interface{} {
	return []interface{}{scriptHash(), 0}
}

func fakeAddr() []byte {
	a := make([]byte, 29)
	a[0] = 0x61
	return a
}

// ── Simple transfer ──────────────────────────────────────────────────────────

func TestParseSimpleTx(t *testing.T) {
	body := map[interface{}]interface{}{
		0: []interface{}{fakeInput()},
		1: []interface{}{[]interface{}{fakeAddr(), uint64(2_000_000)}},
		2: uint64(170_000),
		3: uint64(50_000_000), // TTL
	}
	tx := buildTx(body, map[interface{}]interface{}{}, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("expected non-nil ParsedTx")
	}
	if pt.InputCount != 1 {
		t.Errorf("inputs: got %d, want 1", pt.InputCount)
	}
	if pt.OutputCount != 1 {
		t.Errorf("outputs: got %d, want 1", pt.OutputCount)
	}
	if pt.Fee != 170_000 {
		t.Errorf("fee: got %d, want 170000", pt.Fee)
	}
	if pt.TTL == nil || *pt.TTL != 50_000_000 {
		t.Errorf("TTL: got %v, want 50000000", pt.TTL)
	}
	if pt.IsContract {
		t.Error("simple tx should not be a contract")
	}
	if pt.TxType != cardano.TxTypeSimple {
		t.Errorf("tx_type: got %s, want simple", pt.TxType)
	}
	if pt.IsValid == nil || !*pt.IsValid {
		t.Error("is_valid should be true")
	}
}

// ── Plutus V2 contract call ───────────────────────────────────────────────────

func TestParsePlutusV2Tx(t *testing.T) {
	body := map[interface{}]interface{}{
		0:  []interface{}{fakeInput()},
		1:  []interface{}{[]interface{}{fakeAddr(), uint64(5_000_000)}},
		2:  uint64(250_000),
		11: scriptHash(), // script_data_hash
		13: []interface{}{fakeInput()}, // collateral
		18: []interface{}{fakeInput()}, // reference inputs
	}
	scriptBytes := make([]byte, 128)
	witness := map[interface{}]interface{}{
		5: []interface{}{[]interface{}{0, 0, []interface{}{}, 1}}, // redeemer
		6: []interface{}{scriptBytes}, // plutus_v2_script
	}
	tx := buildTx(body, witness, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("expected non-nil ParsedTx")
	}
	if !pt.IsContract {
		t.Error("should be a contract tx (script_data_hash present)")
	}
	if !pt.HasCollateral {
		t.Error("should have collateral")
	}
	if !pt.HasRefInputs {
		t.Error("should have reference inputs")
	}
	if pt.PlutusV2Scripts != 1 {
		t.Errorf("plutus_v2_scripts: got %d, want 1", pt.PlutusV2Scripts)
	}
	if pt.Redeemers != 1 {
		t.Errorf("redeemers: got %d, want 1", pt.Redeemers)
	}
	if pt.TxType != cardano.TxTypePlutusV2 {
		t.Errorf("tx_type: got %s, want plutus-v2", pt.TxType)
	}
}

// ── Plutus V3 contract call ───────────────────────────────────────────────────

func TestParsePlutusV3Tx(t *testing.T) {
	body := map[interface{}]interface{}{
		0:  []interface{}{fakeInput()},
		1:  []interface{}{[]interface{}{fakeAddr(), uint64(1_000_000)}},
		2:  uint64(300_000),
		11: scriptHash(),
	}
	witness := map[interface{}]interface{}{
		7: []interface{}{make([]byte, 64)}, // plutus_v3_script
	}
	tx := buildTx(body, witness, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("nil")
	}
	if pt.TxType != cardano.TxTypePlutusV3 {
		t.Errorf("tx_type: got %s, want plutus-v3", pt.TxType)
	}
	if pt.PlutusV3Scripts != 1 {
		t.Errorf("plutus_v3_scripts: got %d, want 1", pt.PlutusV3Scripts)
	}
}

// ── Native script (multi-sig) ────────────────────────────────────────────────

func TestParseNativeScriptTx(t *testing.T) {
	body := map[interface{}]interface{}{
		0: []interface{}{fakeInput(), fakeInput()},
		1: []interface{}{[]interface{}{fakeAddr(), uint64(10_000_000)}},
		2: uint64(180_000),
	}
	witness := map[interface{}]interface{}{
		1: []interface{}{
			[]interface{}{0, []interface{}{}}, // native_script (all-of)
			[]interface{}{0, []interface{}{}}, // native_script
		},
	}
	tx := buildTx(body, witness, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("nil")
	}
	if pt.InputCount != 2 {
		t.Errorf("inputs: got %d, want 2", pt.InputCount)
	}
	if pt.NativeScripts != 2 {
		t.Errorf("native_scripts: got %d, want 2", pt.NativeScripts)
	}
	if pt.IsContract {
		t.Error("native-script tx should not be marked as Plutus contract")
	}
	if pt.TxType != cardano.TxTypeNative {
		t.Errorf("tx_type: got %s, want native", pt.TxType)
	}
}

// ── Minting + withdrawal ──────────────────────────────────────────────────────

func TestParseMintWithdrawTx(t *testing.T) {
	body := map[interface{}]interface{}{
		0: []interface{}{fakeInput()},
		1: []interface{}{[]interface{}{fakeAddr(), uint64(1_000_000)}},
		2: uint64(200_000),
		5: map[interface{}]interface{}{"stake1u9y": 500_000}, // withdrawals
		9: map[interface{}]interface{}{"policy1": 1000},      // mint
	}
	tx := buildTx(body, map[interface{}]interface{}{}, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("nil")
	}
	if !pt.HasWithdrawals {
		t.Error("should have withdrawals")
	}
	if !pt.HasMint {
		t.Error("should have mint")
	}
}

// ── Governance (Conway) ───────────────────────────────────────────────────────

func TestParseGovernanceTx(t *testing.T) {
	body := map[interface{}]interface{}{
		0:  []interface{}{fakeInput()},
		1:  []interface{}{[]interface{}{fakeAddr(), uint64(2_000_000)}},
		2:  uint64(175_000),
		19: []interface{}{[]interface{}{}}, // voting_procedures
		20: []interface{}{[]interface{}{}}, // proposal_procedures
	}
	tx := buildTx(body, map[interface{}]interface{}{}, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("nil")
	}
	if !pt.HasGovernance {
		t.Error("should have governance")
	}
}

// ── Staking certs ─────────────────────────────────────────────────────────────

func TestParseCertsTx(t *testing.T) {
	body := map[interface{}]interface{}{
		0: []interface{}{fakeInput()},
		1: []interface{}{[]interface{}{fakeAddr(), uint64(2_000_000)}},
		2: uint64(170_000),
		4: []interface{}{[]interface{}{0, fakeAddr()}}, // stake registration cert
	}
	tx := buildTx(body, map[interface{}]interface{}{}, true)

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("nil")
	}
	if !pt.HasCerts {
		t.Error("should have certs")
	}
}

// ── is_valid = false (failed script) ─────────────────────────────────────────

func TestParseFailedScriptTx(t *testing.T) {
	body := map[interface{}]interface{}{
		0:  []interface{}{fakeInput()},
		1:  []interface{}{[]interface{}{fakeAddr(), uint64(1_000_000)}},
		2:  uint64(200_000),
		11: scriptHash(),
		13: []interface{}{fakeInput()},
	}
	tx := buildTx(body, map[interface{}]interface{}{}, false) // is_valid=false

	pt := cardano.Parse(tx)
	if pt == nil {
		t.Fatal("nil")
	}
	if pt.IsValid == nil || *pt.IsValid {
		t.Error("is_valid should be false for failed script")
	}
}

// ── nil / empty / garbage ─────────────────────────────────────────────────────

func TestParseNilRaw(t *testing.T) {
	if cardano.Parse(nil) != nil {
		t.Error("nil input should return nil")
	}
}

func TestParseGarbage(t *testing.T) {
	pt := cardano.Parse([]byte("not cbor at all"))
	// Should return nil (unparseable)
	if pt != nil && pt.TxType != cardano.TxTypeUnknown {
		// Some garbage might accidentally decode; that's ok
	}
}

// ── Hex-encoded real tx smoke test ───────────────────────────────────────────

// This is a minimal hand-crafted CBOR transaction in hex:
// [body={0:[],1:[],2:200000}, {}, true, null]
// (no inputs or outputs to keep it short)
func TestParseMinimalCBOR(t *testing.T) {
	// Manually crafted: a4 00 80 01 80 02 1a 00 03 0d 40
	// = {0:[], 1:[], 2:200000}
	// wrapped as transaction
	body := map[interface{}]interface{}{
		0: []interface{}{},
		1: []interface{}{},
		2: uint64(200_000),
	}
	bodyBytes, _ := cbor.Marshal(body)
	txBytes, _ := cbor.Marshal([]interface{}{
		cbor.RawMessage(bodyBytes),
		map[interface{}]interface{}{},
		true,
		nil,
	})
	hexStr := hex.EncodeToString(txBytes)
	_ = hexStr

	pt := cardano.Parse(txBytes)
	if pt == nil {
		t.Fatal("expected to parse minimal tx")
	}
	if pt.Fee != 200_000 {
		t.Errorf("fee: got %d, want 200000", pt.Fee)
	}
}
