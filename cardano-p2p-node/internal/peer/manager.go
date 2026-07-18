// Package peer manages the collection of P2P peers.
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
	"github.com/cardano-p2p-node/internal/protocol"
)

// Config holds manager configuration.
type Config struct {
	NetworkMagic    uint32
	ListenAddr      string
	StaticPeers     []string // host:port of peers to always connect to
	MaxInbound      int      // max inbound connections
	MaxOutbound     int      // max outbound connections
	ReconnectDelay  time.Duration
}

// Manager manages all peer connections.
// It maintains outbound connections to static peers, accepts inbound connections,
// and discovers new peers via peer sharing.
type Manager struct {
	cfg   Config
	store *chain.Store
	pool  *mempool.Mempool
	log   *zap.Logger

	mu        sync.RWMutex
	outbound  map[string]*Peer  // addr → peer (outbound)
	inbound   map[string]*Peer  // addr → peer (inbound)
	knownAddrs map[string]bool  // all known peer addresses
}

// NewManager creates a new Manager.
func NewManager(cfg Config, store *chain.Store, pool *mempool.Mempool, log *zap.Logger) *Manager {
	m := &Manager{
		cfg:        cfg,
		store:      store,
		pool:       pool,
		log:        log,
		outbound:   make(map[string]*Peer),
		inbound:    make(map[string]*Peer),
		knownAddrs: make(map[string]bool),
	}
	for _, addr := range cfg.StaticPeers {
		m.knownAddrs[addr] = true
	}
	return m
}

// Run starts the peer manager. It connects to static peers and listens for inbound connections.
func (m *Manager) Run(ctx context.Context) error {
	// Start listening for inbound connections
	go m.listenLoop(ctx)

	// Connect to static peers
	for _, addr := range m.cfg.StaticPeers {
		go m.connectLoop(ctx, addr)
	}

	<-ctx.Done()
	return nil
}

// listenLoop accepts inbound TCP connections.
func (m *Manager) listenLoop(ctx context.Context) {
	if m.cfg.ListenAddr == "" {
		return
	}

	ln, err := net.Listen("tcp", m.cfg.ListenAddr)
	if err != nil {
		m.log.Error("failed to listen", zap.String("addr", m.cfg.ListenAddr), zap.Error(err))
		return
	}
	defer ln.Close()

	m.log.Info("listening for inbound connections", zap.String("addr", m.cfg.ListenAddr))

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				m.log.Warn("accept error", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
		}

		remoteAddr := conn.RemoteAddr().String()
		m.log.Info("inbound connection", zap.String("remote", remoteAddr))

		m.mu.RLock()
		inboundCount := len(m.inbound)
		m.mu.RUnlock()

		if inboundCount >= m.cfg.MaxInbound {
			m.log.Warn("max inbound connections reached, rejecting", zap.String("remote", remoteAddr))
			conn.Close()
			continue
		}

		go m.runInbound(ctx, conn, remoteAddr)
	}
}

func (m *Manager) runInbound(ctx context.Context, conn net.Conn, addr string) {
	p := New(addr, Inbound, conn, m.store, m.pool,
		m.cfg.NetworkMagic,
		m.getShareablePeers,
		m.onNewPeers,
		m.log)

	m.mu.Lock()
	m.inbound[addr] = p
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.inbound, addr)
		m.mu.Unlock()
	}()

	if err := p.Run(ctx); err != nil {
		m.log.Debug("inbound peer ended", zap.String("addr", addr), zap.Error(err))
	}
}

// connectLoop maintains a persistent outbound connection to addr, reconnecting on failure.
func (m *Manager) connectLoop(ctx context.Context, addr string) {
	delay := m.cfg.ReconnectDelay
	if delay == 0 {
		delay = 10 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		m.mu.RLock()
		_, alreadyConnected := m.outbound[addr]
		m.mu.RUnlock()
		if alreadyConnected {
			time.Sleep(time.Second)
			continue
		}

		m.log.Info("connecting to peer", zap.String("addr", addr))
		if err := m.connectOutbound(ctx, addr); err != nil {
			m.log.Warn("outbound connection ended", zap.String("addr", addr), zap.Error(err))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (m *Manager) connectOutbound(ctx context.Context, addr string) error {
	dialer := net.Dialer{Timeout: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	p := New(addr, Outbound, conn, m.store, m.pool,
		m.cfg.NetworkMagic,
		m.getShareablePeers,
		m.onNewPeers,
		m.log)

	m.mu.Lock()
	m.outbound[addr] = p
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.outbound, addr)
		m.mu.Unlock()
	}()

	return p.Run(ctx)
}

// onNewPeers handles newly discovered peer addresses from peer sharing.
func (m *Manager) onNewPeers(peers []protocol.PeerAddress) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newCount := 0
	for _, p := range peers {
		addr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
		if !m.knownAddrs[addr] {
			m.knownAddrs[addr] = true
			newCount++
			m.log.Info("discovered new peer", zap.String("addr", addr))
		}
	}
	m.log.Debug("peer sharing: discovered peers", zap.Int("new", newCount))
}

// getShareablePeers returns peer addresses suitable for sharing.
func (m *Manager) getShareablePeers() []protocol.PeerAddress {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []protocol.PeerAddress
	for addr := range m.outbound {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		ip := net.ParseIP(host)
		if ip == nil {
			continue
		}
		var port uint16
		fmt.Sscanf(portStr, "%d", &port)
		result = append(result, protocol.PeerAddress{IP: ip, Port: port})
	}
	return result
}

// Stats returns current connection statistics.
func (m *Manager) Stats() (inbound, outbound int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.inbound), len(m.outbound)
}
