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

// Direction describes whether a connection was initiated by us or the remote.
type Direction string

const (
	DirInbound  Direction = "inbound"
	DirOutbound Direction = "outbound"
)

// ConnInfo holds metadata about one active peer connection.
type ConnInfo struct {
	ID          string    // unique ID (remote addr)
	IP          string
	Port        string
	Direction   Direction
	ConnectedAt time.Time // UTC
	Version     int       // negotiated NtN version
}

// Config holds manager configuration.
type Config struct {
	NetworkMagic   uint32
	ListenAddr     string
	StaticPeers    []string // host:port of peers to always connect to
	MaxInbound     int      // max simultaneous inbound connections (0 = unlimited)
	MaxOutbound    int      // max simultaneous outbound connections
	ReconnectDelay time.Duration
}

// Manager manages all peer connections.
type Manager struct {
	cfg   Config
	store *chain.Store
	pool  *mempool.Mempool
	log   *zap.Logger

	// ctx is stored so that dynamically discovered peers can be dialled
	// even from callbacks that don't receive a context argument.
	ctx context.Context

	mu         sync.RWMutex
	peers      map[string]*connState // addr → state
	knownAddrs map[string]bool

	// Semaphore channels for connection limits
	inboundSem  chan struct{} // capacity = MaxInbound
	outboundSem chan struct{} // capacity = MaxOutbound

	// Hooks called on connection events (used by RPC / logging layer)
	onConnect    func(ConnInfo)
	onDisconnect func(ConnInfo)
}

type connState struct {
	info ConnInfo
	peer *Peer
}

// NewManager creates a new Manager.
func NewManager(cfg Config, store *chain.Store, pool *mempool.Mempool, log *zap.Logger) *Manager {
	if cfg.MaxInbound <= 0 {
		cfg.MaxInbound = 50
	}
	if cfg.MaxOutbound <= 0 {
		cfg.MaxOutbound = 10
	}

	m := &Manager{
		cfg:         cfg,
		store:       store,
		pool:        pool,
		log:         log,
		peers:       make(map[string]*connState),
		knownAddrs:  make(map[string]bool),
		inboundSem:  make(chan struct{}, cfg.MaxInbound),
		outboundSem: make(chan struct{}, cfg.MaxOutbound),
	}
	for _, addr := range cfg.StaticPeers {
		m.knownAddrs[addr] = true
	}
	return m
}

// SetConnectHook installs a callback invoked (in a new goroutine) when a
// connection is established. Useful for logging and RPC.
func (m *Manager) SetConnectHook(on func(ConnInfo), off func(ConnInfo)) {
	m.onConnect = on
	m.onDisconnect = off
}

// Run starts the peer manager. It connects to static peers and listens for
// inbound connections. Blocks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	m.ctx = ctx // store for use by dynamic peer discovery callbacks
	go m.listenLoop(ctx)
	for _, addr := range m.cfg.StaticPeers {
		go m.connectLoop(ctx, addr)
	}
	<-ctx.Done()
	return nil
}

// AddDynamicPeers is called by the ledger peer manager (and peer sharing) to
// register newly discovered addresses. For each address that is not already
// known, a connectLoop goroutine is spawned so we connect when a slot is free.
func (m *Manager) AddDynamicPeers(ctx context.Context, addrs []string) {
	m.mu.Lock()
	var fresh []string
	for _, addr := range addrs {
		if !m.knownAddrs[addr] {
			m.knownAddrs[addr] = true
			fresh = append(fresh, addr)
		}
	}
	m.mu.Unlock()

	for _, addr := range fresh {
		addr := addr
		go m.connectLoop(ctx, addr)
	}
	if len(fresh) > 0 {
		m.log.Info("peer manager: added dynamic peers",
			zap.Int("new", len(fresh)),
			zap.Int("total_known", m.knownCount()))
	}
}

func (m *Manager) knownCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.knownAddrs)
}

// listenLoop accepts inbound TCP connections.
func (m *Manager) listenLoop(ctx context.Context) {
	if m.cfg.ListenAddr == "" {
		return
	}

	ln, err := net.Listen("tcp", m.cfg.ListenAddr)
	if err != nil {
		m.log.Error("failed to listen",
			zap.String("addr", m.cfg.ListenAddr), zap.Error(err))
		return
	}
	defer ln.Close()

	m.log.Info("listening for inbound connections",
		zap.String("addr", m.cfg.ListenAddr),
		zap.Int("maxInbound", m.cfg.MaxInbound))

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

		// Try to acquire an inbound slot (non-blocking).
		select {
		case m.inboundSem <- struct{}{}:
		default:
			m.log.Warn("max inbound connections reached, rejecting",
				zap.String("remote", conn.RemoteAddr().String()),
				zap.Int("limit", m.cfg.MaxInbound))
			conn.Close()
			continue
		}

		go m.runInbound(ctx, conn)
	}
}

func (m *Manager) runInbound(ctx context.Context, conn net.Conn) {
	defer func() { <-m.inboundSem }() // release slot on exit

	remoteAddr := conn.RemoteAddr().String()
	host, port, _ := net.SplitHostPort(remoteAddr)

	info := ConnInfo{
		ID:          remoteAddr,
		IP:          host,
		Port:        port,
		Direction:   DirInbound,
		ConnectedAt: time.Now().UTC(),
	}

	m.addPeer(remoteAddr, info)
	if m.onConnect != nil {
		go m.onConnect(info)
	}

	p := New(remoteAddr, Inbound, conn, m.store, m.pool,
		m.cfg.NetworkMagic,
		m.getShareablePeers,
		m.onNewPeers,
		m.log)

	m.mu.Lock()
	if s, ok := m.peers[remoteAddr]; ok {
		s.peer = p
	}
	m.mu.Unlock()

	if err := p.Run(ctx); err != nil {
		m.log.Debug("inbound peer ended", zap.String("addr", remoteAddr), zap.Error(err))
	}

	m.removePeer(remoteAddr)
	if m.onDisconnect != nil {
		go m.onDisconnect(info)
	}
}

// connectLoop maintains a persistent outbound connection to addr,
// reconnecting on failure with exponential back-off up to ReconnectDelay.
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

		// Acquire outbound slot (blocking — waits if we're at the limit).
		select {
		case m.outboundSem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		m.log.Info("connecting to peer", zap.String("addr", addr))
		if err := m.connectOutbound(ctx, addr); err != nil {
			m.log.Warn("outbound connection ended",
				zap.String("addr", addr), zap.Error(err))
		}

		<-m.outboundSem // release slot before sleeping

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

	host, port, _ := net.SplitHostPort(addr)
	info := ConnInfo{
		ID:          addr,
		IP:          host,
		Port:        port,
		Direction:   DirOutbound,
		ConnectedAt: time.Now().UTC(),
	}

	m.addPeer(addr, info)
	if m.onConnect != nil {
		go m.onConnect(info)
	}

	p := New(addr, Outbound, conn, m.store, m.pool,
		m.cfg.NetworkMagic,
		m.getShareablePeers,
		m.onNewPeers,
		m.log)

	m.mu.Lock()
	if s, ok := m.peers[addr]; ok {
		s.peer = p
	}
	m.mu.Unlock()

	err = p.Run(ctx)

	m.removePeer(addr)
	if m.onDisconnect != nil {
		go m.onDisconnect(info)
	}
	return err
}

// onNewPeers handles newly discovered peer addresses from peer sharing.
// It registers the addresses and immediately tries to dial any that are new.
func (m *Manager) onNewPeers(peers []protocol.PeerAddress) {
	var addrs []string
	m.mu.Lock()
	for _, p := range peers {
		addr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
		if !m.knownAddrs[addr] {
			m.knownAddrs[addr] = true
			addrs = append(addrs, addr)
		}
	}
	m.mu.Unlock()

	if len(addrs) == 0 {
		return
	}
	ctx := m.ctx
	if ctx == nil {
		return
	}
	// Dial newly discovered peers (respects outboundSem limit).
	for _, addr := range addrs {
		m.log.Info("discovered new peer via peer sharing", zap.String("addr", addr))
		go m.connectLoop(ctx, addr)
	}
}

// getShareablePeers returns peer addresses suitable for sharing with others.
func (m *Manager) getShareablePeers() []protocol.PeerAddress {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []protocol.PeerAddress
	for _, s := range m.peers {
		if s.info.Direction != DirOutbound {
			continue
		}
		ip := net.ParseIP(s.info.IP)
		if ip == nil {
			continue
		}
		var port uint16
		fmt.Sscanf(s.info.Port, "%d", &port)
		result = append(result, protocol.PeerAddress{IP: ip, Port: port})
	}
	return result
}

// GetConnections returns a snapshot of all active connection metadata.
func (m *Manager) GetConnections() []ConnInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ConnInfo, 0, len(m.peers))
	for _, s := range m.peers {
		out = append(out, s.info)
	}
	return out
}

// Stats returns (inbound, outbound) connection counts.
func (m *Manager) Stats() (inbound, outbound int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.peers {
		if s.info.Direction == DirInbound {
			inbound++
		} else {
			outbound++
		}
	}
	return
}

func (m *Manager) addPeer(addr string, info ConnInfo) {
	m.mu.Lock()
	m.peers[addr] = &connState{info: info}
	m.mu.Unlock()
}

func (m *Manager) removePeer(addr string) {
	m.mu.Lock()
	delete(m.peers, addr)
	m.mu.Unlock()
}
