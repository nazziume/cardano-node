// Package peer manages individual P2P peer connections.
package peer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/chain"
	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/mux"
	"github.com/cardano-p2p-node/internal/protocol"
)

// Role describes which side of a connection this peer is.
type Role int

const (
	Outbound Role = iota // we initiated the connection
	Inbound              // they connected to us
)

func (r Role) String() string {
	if r == Outbound {
		return "outbound"
	}
	return "inbound"
}

// Peer represents a single P2P connection to a remote node.
type Peer struct {
	addr         string
	role         Role
	mc           *mux.Conn
	store        *chain.Store
	pool         *mempool.Mempool
	networkMagic uint32
	log          *zap.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{}

	// getPeers returns known peer addresses for sharing
	getPeers func() []protocol.PeerAddress
	// onNewPeers is called when we discover new peers via peer sharing
	onNewPeers func([]protocol.PeerAddress)
}

// New creates a new Peer.
func New(
	addr string,
	role Role,
	conn net.Conn,
	store *chain.Store,
	pool *mempool.Mempool,
	networkMagic uint32,
	getPeers func() []protocol.PeerAddress,
	onNewPeers func([]protocol.PeerAddress),
	log *zap.Logger,
) *Peer {
	isInitiator := role == Outbound
	return &Peer{
		addr:         addr,
		role:         role,
		mc:           mux.New(conn, isInitiator),
		store:        store,
		pool:         pool,
		networkMagic: networkMagic,
		log:          log.With(zap.String("peer", addr), zap.String("role", role.String())),
		done:         make(chan struct{}),
		getPeers:     getPeers,
		onNewPeers:   onNewPeers,
	}
}

// Run starts all protocols for this peer and blocks until the connection closes.
// ctx can be used to stop the peer gracefully.
func (p *Peer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	defer cancel()

	// Start the mux read loop in a goroutine
	readErrCh := make(chan error, 1)
	go func() {
		readErrCh <- p.mc.ReadLoop()
	}()

	// Run handshake (synchronously, before starting other protocols)
	var neg *protocol.NegotiatedVersion
	var err error

	if p.role == Outbound {
		neg, err = protocol.HandshakePropose(p.mc, p.networkMagic)
	} else {
		neg, err = protocol.HandshakeAccept(p.mc, p.networkMagic)
	}
	if err != nil {
		_ = p.mc.Close()
		return fmt.Errorf("handshake failed: %w", err)
	}

	p.log.Info("handshake complete",
		zap.Int("version", neg.Version),
		zap.Bool("peerSharing", neg.PeerSharing))

	// Launch all mini-protocols concurrently
	errCh := make(chan error, 10)

	if p.role == Outbound {
		// Outbound (we connected): we run initiator-side protocols
		p.wg.Add(4)

		// ChainSync CLIENT: pull headers from them → builds our chain knowledge.
		// Also triggers TTL pruning of the mempool on each new slot.
		go func() {
			defer p.wg.Done()
			if err := protocol.ChainSyncClient(p.mc, p.store, p.pool, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("chainsync client: %w", err)
			}
		}()

		// BlockFetch CLIENT: pull blocks → lets us serve fetchyness.
		// Also extracts confirmed txids to clean the mempool.
		go func() {
			defer p.wg.Done()
			if err := protocol.BlockFetchClient(p.mc, p.store, p.pool, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("blockfetch client: %w", err)
			}
		}()

		// TxSubmission OUTBOUND: provide our txs to them
		go func() {
			defer p.wg.Done()
			if err := protocol.TxSubmissionOutbound(p.mc, p.pool, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("txsubmission outbound: %w", err)
			}
		}()

		// KeepAlive CLIENT: maintain connection health and measure RTT
		go func() {
			defer p.wg.Done()
			if err := protocol.KeepAliveClient(p.mc, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("keepalive client: %w", err)
			}
		}()

		// Peer sharing client: periodically discover new peers and propagate
		// our own address. Runs every 15 minutes while the connection lives.
		// Each request triggers PeerSharingServer on the remote to include our
		// advertise address in its response, spreading us through the network.
		if neg.PeerSharing {
			go func() {
				time.Sleep(5 * time.Second) // wait for connection to stabilize
				ticker := time.NewTicker(15 * time.Minute)
				defer ticker.Stop()
				for {
					peers, err := protocol.PeerSharingClient(p.mc, 20, p.log)
					if err != nil {
						p.log.Debug("peer sharing failed", zap.Error(err))
						return
					}
					if p.onNewPeers != nil && len(peers) > 0 {
						p.onNewPeers(peers)
					}
					select {
					case <-p.done:
						return
					case <-ticker.C:
					}
				}
			}()
		}

	} else {
		// Inbound (they connected to us): we run responder-side protocols
		p.wg.Add(4)

		// ChainSync SERVER: serve our headers to them → THIS BUILDS UPSTREAMYNESS!
		// Being first to serve new headers is what scores us highly.
		go func() {
			defer p.wg.Done()
			if err := protocol.ChainSyncServer(p.mc, p.store, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("chainsync server: %w", err)
			}
		}()

		// BlockFetch SERVER: serve our blocks → THIS BUILDS FETCHYNESS!
		go func() {
			defer p.wg.Done()
			if err := protocol.BlockFetchServer(p.mc, p.store, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("blockfetch server: %w", err)
			}
		}()

		// TxSubmission INBOUND: collect their txs into our mempool
		go func() {
			defer p.wg.Done()
			if err := protocol.TxSubmissionInbound(p.mc, p.pool, p.log, p.done); err != nil {
				errCh <- fmt.Errorf("txsubmission inbound: %w", err)
			}
		}()

		// KeepAlive SERVER: respond to their pings immediately (low RTT = good signal)
		go func() {
			defer p.wg.Done()
			if err := protocol.KeepAliveServer(p.mc, p.log); err != nil {
				errCh <- fmt.Errorf("keepalive server: %w", err)
			}
		}()

		// Peer sharing server: share our known peers
		if neg.PeerSharing && p.getPeers != nil {
			go func() {
				_ = protocol.PeerSharingServer(p.mc, p.getPeers, p.log, p.done)
			}()
		}
	}

	// Wait for either context cancellation, mux read error, or protocol error
	select {
	case <-ctx.Done():
		p.log.Info("peer shutting down (context cancelled)")
	case err := <-readErrCh:
		if err != nil {
			p.log.Info("peer disconnected", zap.Error(err))
		}
	case err := <-errCh:
		p.log.Warn("protocol error", zap.Error(err))
	}

	// Signal all protocols to stop
	close(p.done)
	_ = p.mc.Close()

	// Wait for all goroutines
	doneCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		p.log.Warn("peer shutdown timeout")
	}

	return nil
}

// Addr returns the peer's address.
func (p *Peer) Addr() string {
	return p.addr
}
