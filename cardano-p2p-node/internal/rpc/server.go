// Package rpc provides a lightweight HTTP query interface for the node.
//
// Endpoints:
//
//	GET  /status        — current connection list (inbound + outbound) with IP, port, direction, uptime
//	GET  /mempool       — all transactions currently in the mempool with metadata
//	GET  /health        — simple liveness probe (returns 200 OK)
//	POST /debug/inject  — inject N fake test transactions into the local mempool
//	                      query param: count=N (default 1, max 1000)
package rpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"

	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/peer"
)

// PoolWriter extends PoolSource with the ability to add transactions directly
// and query statistics. Implemented by *mempool.Mempool.
type PoolWriter interface {
	PoolSource
	Add(e *mempool.TxEntry) bool
	GetStats() mempool.Stats
}

// ConnSource is implemented by the peer manager.
type ConnSource interface {
	GetConnections() []peer.ConnInfo
	Stats() (inbound, outbound int)
}

// PoolSource is implemented by the mempool.
type PoolSource interface {
	GetAll() []*mempool.TxEntry
	Size() int
}

// ChainSource provides chain tip information.
type ChainSource interface {
	Tip() (slot uint64, blockNo uint64)
}

// tipAdapter wraps chain.Store Tip() which returns a Point.
type tipAdapter struct {
	fn func() (uint64, uint64)
}

// Server is the HTTP RPC server.
type Server struct {
	addr    string
	conns   ConnSource
	pool    PoolWriter
	getTip  func() (uint64, uint64)
	log     *zap.Logger
	httpSrv *http.Server
}

// New creates a new RPC Server.
func New(addr string, conns ConnSource, pool PoolWriter, getTip func() (uint64, uint64), log *zap.Logger) *Server {
	s := &Server{
		addr:   addr,
		conns:  conns,
		pool:   pool,
		getTip: getTip,
		log:    log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/mempool", s.handleMempool)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/debug/inject", s.handleDebugInject)

	s.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	s.log.Info("RPC server listening", zap.String("addr", s.addr))

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}

// ── /status ──────────────────────────────────────────────────────────────────

type statusResponse struct {
	ChainTipSlot  uint64        `json:"chain_tip_slot"`
	ChainTipBlock uint64        `json:"chain_tip_block"`
	Inbound       int           `json:"inbound_count"`
	Outbound      int           `json:"outbound_count"`
	Connections   []connInfoOut `json:"connections"`
	MempoolStats  mempoolStats  `json:"mempool_stats"`
}

type mempoolStats struct {
	Current          int    `json:"current_size"`
	TotalAdded       uint64 `json:"total_added"`
	RemovedConfirmed uint64 `json:"removed_confirmed"` // txs removed after on-chain confirmation
	RemovedTTL       uint64 `json:"removed_ttl"`       // txs removed due to TTL expiry
	RemovedEvicted   uint64 `json:"removed_evicted"`   // txs evicted due to capacity
}

type connInfoOut struct {
	IP          string `json:"ip"`
	Port        string `json:"port"`
	Direction   string `json:"direction"`
	ConnectedAt string `json:"connected_at"` // UTC+8 RFC3339
	UptimeSec   int64  `json:"uptime_sec"`
}

var jst = time.FixedZone("CST", 8*60*60)

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	conns := s.conns.GetConnections()
	inb, outb := s.conns.Stats()
	slot, blockNo := s.getTip()

	st := s.pool.GetStats()
	out := statusResponse{
		ChainTipSlot:  slot,
		ChainTipBlock: blockNo,
		Inbound:       inb,
		Outbound:      outb,
		Connections:   make([]connInfoOut, 0, len(conns)),
		MempoolStats: mempoolStats{
			Current:          s.pool.Size(),
			TotalAdded:       st.Added,
			RemovedConfirmed: st.RemovedConfirmed,
			RemovedTTL:       st.RemovedTTL,
			RemovedEvicted:   st.RemovedEvicted,
		},
	}

	now := time.Now()
	for _, c := range conns {
		out.Connections = append(out.Connections, connInfoOut{
			IP:          c.IP,
			Port:        c.Port,
			Direction:   string(c.Direction),
			ConnectedAt: c.ConnectedAt.In(jst).Format(time.RFC3339),
			UptimeSec:   int64(now.Sub(c.ConnectedAt).Seconds()),
		})
	}

	writeJSON(w, out)
}

// ── /mempool ─────────────────────────────────────────────────────────────────

type mempoolResponse struct {
	Count        int      `json:"count"`
	Transactions []txOut  `json:"transactions"`
}

type txOut struct {
	TxID       string `json:"txid"`
	SizeBytes  uint32 `json:"size_bytes"`
	FromPeer   string `json:"from_peer"`
	ReceivedAt string `json:"received_at"` // UTC+8 RFC3339
	RawHex     string `json:"raw_hex"`     // full CBOR as hex

	// Parsed transaction fields (nil if the tx CBOR could not be decoded)
	Parsed *parsedTxOut `json:"parsed,omitempty"`
}

// parsedTxOut is the JSON-serialisable view of cardano.ParsedTx.
type parsedTxOut struct {
	TxType        string  `json:"tx_type"`
	InputCount    int     `json:"input_count"`
	OutputCount   int     `json:"output_count"`
	FeeLovelace   uint64  `json:"fee_lovelace"`
	FeeADA        float64 `json:"fee_ada"`
	TTL           *uint64 `json:"ttl,omitempty"`
	ValidityStart *uint64 `json:"validity_start,omitempty"`
	IsContract    bool    `json:"is_contract"`
	IsValid       *bool   `json:"is_valid,omitempty"`

	HasCerts       bool `json:"has_certs"`
	HasWithdrawals bool `json:"has_withdrawals"`
	HasMint        bool `json:"has_mint"`
	HasCollateral  bool `json:"has_collateral"`
	HasRefInputs   bool `json:"has_ref_inputs"`
	HasGovernance  bool `json:"has_governance"`

	NativeScripts   int `json:"native_scripts"`
	PlutusV1Scripts int `json:"plutus_v1_scripts"`
	PlutusV2Scripts int `json:"plutus_v2_scripts"`
	PlutusV3Scripts int `json:"plutus_v3_scripts"`
	Redeemers       int `json:"redeemers"`
}

func (s *Server) handleMempool(w http.ResponseWriter, r *http.Request) {
	all := s.pool.GetAll()
	out := mempoolResponse{
		Count:        len(all),
		Transactions: make([]txOut, 0, len(all)),
	}

	for _, e := range all {
		tx := txOut{
			TxID:       e.ID.String(),
			SizeBytes:  e.Size,
			FromPeer:   e.FromPeer,
			ReceivedAt: e.ReceivedAt.In(jst).Format(time.RFC3339),
			RawHex:     hex.EncodeToString(e.Raw),
		}
		if p := e.Parsed; p != nil {
			tx.Parsed = &parsedTxOut{
				TxType:          string(p.TxType),
				InputCount:      p.InputCount,
				OutputCount:     p.OutputCount,
				FeeLovelace:     p.Fee,
				FeeADA:          float64(p.Fee) / 1_000_000,
				TTL:             p.TTL,
				ValidityStart:   p.ValidityStart,
				IsContract:      p.IsContract,
				IsValid:         p.IsValid,
				HasCerts:        p.HasCerts,
				HasWithdrawals:  p.HasWithdrawals,
				HasMint:         p.HasMint,
				HasCollateral:   p.HasCollateral,
				HasRefInputs:    p.HasRefInputs,
				HasGovernance:   p.HasGovernance,
				NativeScripts:   p.NativeScripts,
				PlutusV1Scripts: p.PlutusV1Scripts,
				PlutusV2Scripts: p.PlutusV2Scripts,
				PlutusV3Scripts: p.PlutusV3Scripts,
				Redeemers:       p.Redeemers,
			}
		}
		out.Transactions = append(out.Transactions, tx)
	}

	writeJSON(w, out)
}

// ── /health ───────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok\n"))
}

// ── helpers ───────────────────────────────────────────────────────────────────

// ── /debug/inject ─────────────────────────────────────────────────────────────

type injectResponse struct {
	Injected int      `json:"injected"`
	TxIDs    []string `json:"txids"`
}

func (s *Server) handleDebugInject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}

	countStr := r.URL.Query().Get("count")
	count := 1
	if countStr != "" {
		if n, err := strconv.Atoi(countStr); err == nil && n > 0 && n <= 1000 {
			count = n
		}
	}

	var injected []string
	for i := 0; i < count; i++ {
		txid := make([]byte, 32)
		if _, err := rand.Read(txid); err != nil {
			http.Error(w, "rand failed", http.StatusInternalServerError)
			return
		}

		// Construct a fake transaction as a proper 4-element CBOR array
		// [transaction_body, witness_set, is_valid, null]
		// so that our parser can extract meaningful fields.

		// Fake input = [tx_hash_bytes, index]
		inputHash := make([]byte, 32)
		rand.Read(inputHash)
		fakeInput := []interface{}{inputHash, 0}

		// Fake output address (29 bytes enterprise addr tag)
		fakeAddr := make([]byte, 29)
		rand.Read(fakeAddr)
		fakeAddr[0] = 0x61 // mainnet enterprise addr tag

		// Alternate between tx types to cover all cases
		fee := uint64(170000 + i*10000) // realistic fee range

		var body map[interface{}]interface{}
		switch i % 4 {
		case 0: // simple transfer
			body = map[interface{}]interface{}{
				0: []interface{}{fakeInput},
				1: []interface{}{[]interface{}{fakeAddr, 2_000_000}},
				2: fee,
				3: uint64(100_000_000 + i), // TTL
			}

		case 1: // native script (multi-sig)
			body = map[interface{}]interface{}{
				0: []interface{}{fakeInput},
				1: []interface{}{[]interface{}{fakeAddr, 5_000_000}},
				2: fee,
			}

		case 2: // Plutus contract call
			scriptHash := make([]byte, 32)
			rand.Read(scriptHash)
			body = map[interface{}]interface{}{
				0:  []interface{}{fakeInput},
				1:  []interface{}{[]interface{}{fakeAddr, 10_000_000}},
				2:  fee,
				11: scriptHash, // script_data_hash → IsContract=true
				13: []interface{}{[]interface{}{inputHash, 1}}, // collateral
			}

		case 3: // minting + governance
			body = map[interface{}]interface{}{
				0:  []interface{}{fakeInput, []interface{}{inputHash, 2}},
				1:  []interface{}{[]interface{}{fakeAddr, 3_000_000}},
				2:  fee,
				9:  map[interface{}]interface{}{"policy1": 1000}, // mint
				19: []interface{}{}, // governance (voting_procedures)
			}
		}

		bodyBytes, _ := cbor.Marshal(body)

		// Witness set
		var witnessMap map[interface{}]interface{}
		switch i % 4 {
		case 1: // native script witness
			witnessMap = map[interface{}]interface{}{
				1: []interface{}{[]interface{}{0, []interface{}{}}}, // native_script
			}
		case 2: // Plutus V2 witness
			scriptBytes := make([]byte, 64)
			rand.Read(scriptBytes)
			witnessMap = map[interface{}]interface{}{
				5: []interface{}{[]interface{}{0, 0, []interface{}{}, 1}}, // redeemer
				6: []interface{}{scriptBytes}, // plutus_v2_script
			}
		default:
			witnessMap = map[interface{}]interface{}{}
		}
		witnessBytes, _ := cbor.Marshal(witnessMap)

		// Full transaction: [body, witness_set, is_valid, null]
		fullTx, _ := cbor.Marshal([]interface{}{
			cbor.RawMessage(bodyBytes),
			cbor.RawMessage(witnessBytes),
			true,
			nil,
		})

		// Compute the canonical txid = Blake2b-256(transaction_body_cbor).
		// This matches how Cardano nodes compute txids, so real nodes won't
		// reject us for announcing a txid that doesn't match the body.
		h, _ := blake2b.New256(nil)
		h.Write(bodyBytes)
		txid = h.Sum(nil)

		entry := &mempool.TxEntry{
			ID:         mempool.TxID(txid),
			Size:       uint32(len(fullTx)),
			Raw:        cbor.RawMessage(fullTx),
			FromPeer:   fmt.Sprintf("debug/inject (type=%s)", txTypeLabel(i)),
			ReceivedAt: time.Now().UTC(),
		}
		if s.pool.Add(entry) {
			injected = append(injected, hex.EncodeToString(txid))
		}
	}

	writeJSON(w, injectResponse{
		Injected: len(injected),
		TxIDs:    injected,
	})
}

func txTypeLabel(i int) string {
	switch i % 4 {
	case 0:
		return "simple"
	case 1:
		return "native-script"
	case 2:
		return "plutus-v2"
	case 3:
		return "mint+governance"
	default:
		return "unknown"
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
