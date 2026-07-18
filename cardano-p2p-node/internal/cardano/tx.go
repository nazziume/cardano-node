// Package cardano provides best-effort parsing of Cardano transaction CBOR.
//
// Conway-era transaction wire format (CDDL):
//
//	transaction = [
//	    transaction_body,          ; CBOR map with uint keys
//	    transaction_witness_set,   ; CBOR map with uint keys
//	    bool,                      ; is_valid (false = script validation failed)
//	    transaction_metadata / null
//	]
//
//	transaction_body = {
//	    0  : set<transaction_input>,          ; inputs (mandatory)
//	    1  : [* transaction_output],          ; outputs (mandatory)
//	    2  : coin,                            ; fee in lovelace (mandatory)
//	  ? 3  : uint,                            ; TTL (time-to-live slot)
//	  ? 4  : certificates,
//	  ? 5  : withdrawals,
//	  ? 7  : auxiliary_data_hash,
//	  ? 8  : uint,                            ; validity start slot
//	  ? 9  : mint,                            ; minting/burning
//	  ? 11 : script_data_hash,               ; MUST be present for Plutus scripts
//	  ? 13 : nonempty_set<transaction_input>, ; collateral inputs
//	  ? 14 : required_signers,
//	  ? 15 : network_id,
//	  ? 16 : transaction_output,              ; collateral return
//	  ? 17 : coin,                            ; total collateral
//	  ? 18 : nonempty_set<transaction_input>, ; reference inputs
//	  ? 19 : voting_procedures,              ; Conway governance
//	  ? 20 : proposal_procedures,            ; Conway governance
//	  ? 21 : coin,                            ; treasury value
//	  ? 22 : positive_coin,                   ; donation
//	}
//
//	transaction_witness_set = {
//	  ? 0 : [* vkeywitness],
//	  ? 1 : [* native_script],
//	  ? 3 : [* plutus_v1_script],
//	  ? 4 : [* plutus_data],
//	  ? 5 : [* redeemer],
//	  ? 6 : [* plutus_v2_script],
//	  ? 7 : [* plutus_v3_script],
//	}
//
// Source: https://github.com/IntersectMBO/cardano-ledger/blob/master/eras/conway/impl/cddl/data/conway.cddl
package cardano

import (
	"github.com/fxamacker/cbor/v2"
)

// TxType classifies a Cardano transaction by the kind of script logic it uses.
type TxType string

const (
	TxTypeSimple    TxType = "simple"     // pure ADA/token transfer, no scripts
	TxTypeNative    TxType = "native"     // native multi-sig or time-lock script
	TxTypePlutusV1 TxType = "plutus-v1"  // Plutus V1 smart contract
	TxTypePlutusV2 TxType = "plutus-v2"  // Plutus V2 smart contract
	TxTypePlutusV3 TxType = "plutus-v3"  // Plutus V3 smart contract (Conway+)
	TxTypeUnknown  TxType = "unknown"    // could not determine (parse error)
)

// ParsedTx holds fields extracted from a Cardano transaction body.
// All fields are best-effort: zero values mean "absent or could not parse".
type ParsedTx struct {
	// Body fields
	InputCount    int    `json:"input_count"`
	OutputCount   int    `json:"output_count"`
	Fee           uint64 `json:"fee_lovelace"`
	TTL           *uint64 `json:"ttl,omitempty"`           // time-to-live slot
	ValidityStart *uint64 `json:"validity_start,omitempty"` // validity interval start

	// Feature flags
	HasCerts      bool `json:"has_certs"`        // staking / pool / governance certs
	HasWithdrawals bool `json:"has_withdrawals"` // reward withdrawals
	HasMint       bool `json:"has_mint"`         // token minting or burning
	HasCollateral bool `json:"has_collateral"`   // Plutus collateral inputs
	HasRefInputs  bool `json:"has_ref_inputs"`   // Plutus reference inputs
	HasGovernance bool `json:"has_governance"`   // Conway voting/proposals

	// Plutus-specific
	IsContract    bool `json:"is_contract"`     // script_data_hash (key 11) present
	IsValid       *bool `json:"is_valid,omitempty"` // is_valid field (nil = not parsed)

	// Witness counts
	NativeScripts   int `json:"native_scripts"`   // multi-sig / time-lock
	PlutusV1Scripts int `json:"plutus_v1_scripts"`
	PlutusV2Scripts int `json:"plutus_v2_scripts"`
	PlutusV3Scripts int `json:"plutus_v3_scripts"`
	Redeemers       int `json:"redeemers"` // number of script redeemers

	// Derived classification
	TxType TxType `json:"tx_type"`
}

// Parse decodes a raw Cardano transaction and returns the extracted fields.
// It tries multiple structural interpretations to handle different era encodings.
// Returns nil if the data cannot be parsed at all (e.g. not a valid CBOR tx).
func Parse(raw []byte) *ParsedTx {
	if len(raw) == 0 {
		return nil
	}

	// Try: full transaction = [body, witness_set, is_valid, aux]
	var fullTx []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &fullTx); err == nil && len(fullTx) >= 3 {
		pt := &ParsedTx{}
		parseBody(fullTx[0], pt)
		if len(fullTx) >= 2 {
			parseWitness(fullTx[1], pt)
		}
		if len(fullTx) >= 3 {
			var valid bool
			if err := cbor.Unmarshal(fullTx[2], &valid); err == nil {
				pt.IsValid = &valid
			}
		}
		pt.TxType = classify(pt)
		return pt
	}

	// Fallback: try treating raw bytes directly as the transaction body map
	pt := &ParsedTx{}
	if parseBody(raw, pt) {
		pt.TxType = classify(pt)
		return pt
	}

	return nil
}

// parseBody decodes the transaction_body CBOR map.
// Returns true if the key structure looks like a valid body.
func parseBody(raw cbor.RawMessage, pt *ParsedTx) bool {
	var body map[uint64]cbor.RawMessage
	if err := cbor.Unmarshal(raw, &body); err != nil {
		return false
	}

	// Key 0: inputs — set<transaction_input>
	if v, ok := body[0]; ok {
		var inputs []cbor.RawMessage
		if err := cbor.Unmarshal(v, &inputs); err == nil {
			pt.InputCount = len(inputs)
		}
	}

	// Key 1: outputs — [* transaction_output]
	if v, ok := body[1]; ok {
		var outputs []cbor.RawMessage
		if err := cbor.Unmarshal(v, &outputs); err == nil {
			pt.OutputCount = len(outputs)
		}
	}

	// Key 2: fee — coin (uint)
	if v, ok := body[2]; ok {
		var fee uint64
		if err := cbor.Unmarshal(v, &fee); err == nil {
			pt.Fee = fee
		}
	}

	// Key 3: TTL
	if v, ok := body[3]; ok {
		var ttl uint64
		if err := cbor.Unmarshal(v, &ttl); err == nil {
			pt.TTL = &ttl
		}
	}

	// Key 4: certificates
	if _, ok := body[4]; ok {
		pt.HasCerts = true
	}

	// Key 5: withdrawals
	if _, ok := body[5]; ok {
		pt.HasWithdrawals = true
	}

	// Key 8: validity start
	if v, ok := body[8]; ok {
		var start uint64
		if err := cbor.Unmarshal(v, &start); err == nil {
			pt.ValidityStart = &start
		}
	}

	// Key 9: mint
	if _, ok := body[9]; ok {
		pt.HasMint = true
	}

	// Key 11: script_data_hash — presence means Plutus scripts are involved
	if _, ok := body[11]; ok {
		pt.IsContract = true
	}

	// Key 13: collateral inputs
	if _, ok := body[13]; ok {
		pt.HasCollateral = true
	}

	// Key 18: reference inputs
	if _, ok := body[18]; ok {
		pt.HasRefInputs = true
	}

	// Key 19 or 20: Conway governance
	if _, ok := body[19]; ok {
		pt.HasGovernance = true
	}
	if _, ok := body[20]; ok {
		pt.HasGovernance = true
	}

	// Require at least fee (key 2) to consider it a valid body
	_, hasFee := body[2]
	return hasFee
}

// parseWitness decodes the transaction_witness_set CBOR map.
func parseWitness(raw cbor.RawMessage, pt *ParsedTx) {
	var ws map[uint64]cbor.RawMessage
	if err := cbor.Unmarshal(raw, &ws); err != nil {
		return
	}

	// Key 1: native scripts
	if v, ok := ws[1]; ok {
		var scripts []cbor.RawMessage
		if err := cbor.Unmarshal(v, &scripts); err == nil {
			pt.NativeScripts = len(scripts)
		}
	}

	// Key 3: Plutus V1 scripts
	if v, ok := ws[3]; ok {
		var scripts []cbor.RawMessage
		if err := cbor.Unmarshal(v, &scripts); err == nil {
			pt.PlutusV1Scripts = len(scripts)
		}
	}

	// Key 4: Plutus data (datums) — presence hints at script usage
	if v, ok := ws[4]; ok {
		var datums []cbor.RawMessage
		if err := cbor.Unmarshal(v, &datums); err == nil && len(datums) > 0 {
			pt.IsContract = true
		}
	}

	// Key 5: redeemers
	if v, ok := ws[5]; ok {
		var redeemers []cbor.RawMessage
		if err := cbor.Unmarshal(v, &redeemers); err == nil {
			pt.Redeemers = len(redeemers)
		}
	}

	// Key 6: Plutus V2 scripts
	if v, ok := ws[6]; ok {
		var scripts []cbor.RawMessage
		if err := cbor.Unmarshal(v, &scripts); err == nil {
			pt.PlutusV2Scripts = len(scripts)
		}
	}

	// Key 7: Plutus V3 scripts
	if v, ok := ws[7]; ok {
		var scripts []cbor.RawMessage
		if err := cbor.Unmarshal(v, &scripts); err == nil {
			pt.PlutusV3Scripts = len(scripts)
		}
	}
}

// classify assigns a TxType based on parsed fields.
func classify(pt *ParsedTx) TxType {
	if pt.PlutusV3Scripts > 0 {
		return TxTypePlutusV3
	}
	if pt.PlutusV2Scripts > 0 {
		return TxTypePlutusV2
	}
	if pt.PlutusV1Scripts > 0 {
		return TxTypePlutusV1
	}
	// script_data_hash present but no inline scripts → scripts via reference inputs
	if pt.IsContract {
		return TxTypePlutusV2 // default assumption for ref-script Plutus
	}
	if pt.NativeScripts > 0 {
		return TxTypeNative
	}
	if pt.InputCount == 0 && pt.Fee == 0 {
		return TxTypeUnknown
	}
	return TxTypeSimple
}
