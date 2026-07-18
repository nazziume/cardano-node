// Package rpc provides a lightweight HTTP query interface for the node.
//
// Endpoints:
//
//	GET /status  — current connection list (inbound + outbound) with IP, port, direction, uptime
//	GET /mempool — all transactions currently in the mempool with metadata
//	GET /health  — simple liveness probe (returns 200 OK)
package rpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/peer"
)

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
	pool    PoolSource
	getTip  func() (uint64, uint64)
	log     *zap.Logger
	httpSrv *http.Server
}

// New creates a new RPC Server.
func New(addr string, conns ConnSource, pool PoolSource, getTip func() (uint64, uint64), log *zap.Logger) *Server {
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
}

type connInfoOut struct {
	IP          string `json:"ip"`
	Port        string `json:"port"`
	Direction   string `json:"direction"`
	ConnectedAt string `json:"connected_at"` // UTC+9 RFC3339
	UptimeSec   int64  `json:"uptime_sec"`
}

var jst = time.FixedZone("JST", 9*60*60)

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	conns := s.conns.GetConnections()
	inb, outb := s.conns.Stats()
	slot, blockNo := s.getTip()

	out := statusResponse{
		ChainTipSlot:  slot,
		ChainTipBlock: blockNo,
		Inbound:       inb,
		Outbound:      outb,
		Connections:   make([]connInfoOut, 0, len(conns)),
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
	TxID        string `json:"txid"`
	SizeBytes   uint32 `json:"size_bytes"`
	FromPeer    string `json:"from_peer"`
	ReceivedAt  string `json:"received_at"` // UTC+9 RFC3339
	RawHex      string `json:"raw_hex"`     // full CBOR as hex
}

func (s *Server) handleMempool(w http.ResponseWriter, r *http.Request) {
	all := s.pool.GetAll()
	out := mempoolResponse{
		Count:        len(all),
		Transactions: make([]txOut, 0, len(all)),
	}

	for _, e := range all {
		out.Transactions = append(out.Transactions, txOut{
			TxID:       e.ID.String(),
			SizeBytes:  e.Size,
			FromPeer:   e.FromPeer,
			ReceivedAt: e.ReceivedAt.In(jst).Format(time.RFC3339),
			RawHex:     hex.EncodeToString(e.Raw),
		})
	}

	writeJSON(w, out)
}

// ── /health ───────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok\n"))
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
