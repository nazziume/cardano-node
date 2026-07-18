// cardano-p2p-node is a lightweight Cardano relay node implementation in Go.
//
// It participates in the Cardano P2P network by implementing the Ouroboros
// NodeToNode mini-protocols and is optimized to score highly as a quality peer.
//
// See README.md for details on peer scoring and architecture.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"

	"github.com/cardano-p2p-node/internal/chain"
	"github.com/cardano-p2p-node/internal/ledger"
	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/peer"
	"github.com/cardano-p2p-node/internal/protocol"
	"github.com/cardano-p2p-node/internal/rpc"
)

// jst is UTC+8 (China Standard Time), used for all log timestamps.
var jst = time.FixedZone("CST", 8*60*60)

// Config is the top-level node configuration.
type Config struct {
	Network        string   `yaml:"network"`
	ListenAddr     string   `yaml:"listenAddr"`
	StaticPeers    []string `yaml:"staticPeers"`
	MaxInbound     int      `yaml:"maxInbound"`
	MaxOutbound    int      `yaml:"maxOutbound"`
	ReconnectDelay string   `yaml:"reconnectDelay"`
	LogLevel       string   `yaml:"logLevel"`
	RPCAddr        string   `yaml:"rpcAddr"`

	// Ledger peers configuration
	LedgerPeers LedgerPeersConfig `yaml:"ledgerPeers"`
}

// LedgerPeersConfig controls ledger peer discovery.
type LedgerPeersConfig struct {
	// SnapshotFile is a path to a local peer snapshot JSON file.
	SnapshotFile string `yaml:"snapshotFile"`
	// SnapshotURL is an HTTP(S) URL to fetch the snapshot from.
	// Defaults to the official IOG mainnet snapshot URL.
	SnapshotURL string `yaml:"snapshotURL"`
	// RefreshInterval: how often to re-fetch the snapshot. Default: 1h.
	RefreshInterval string `yaml:"refreshInterval"`
	// MaxPeers: max relay addresses to use per refresh. Default: 100.
	MaxPeers int `yaml:"maxPeers"`
	// Enabled: set false to disable ledger peer discovery. Default: true.
	Enabled *bool `yaml:"enabled"`
}

func boolPtr(b bool) *bool { return &b }

func defaultConfig() Config {
	return Config{
		Network:        "mainnet",
		ListenAddr:     "0.0.0.0:3001",
		MaxInbound:     50,
		MaxOutbound:    10,
		ReconnectDelay: "10s",
		LogLevel:       "info",
		RPCAddr:        "127.0.0.1:8888",
		LedgerPeers: LedgerPeersConfig{
			SnapshotURL:     "https://book.world.dev.cardano.org/environments/mainnet/peer-snapshot.json",
			RefreshInterval: "1h",
			MaxPeers:        100,
			Enabled:         boolPtr(true),
		},
	}
}

func networkMagic(network string) uint32 {
	switch network {
	case "mainnet":
		return protocol.MainnetMagic
	case "preview":
		return protocol.PreviewMagic
	case "preprod":
		return protocol.PreprodMagic
	case "sanchonet":
		return protocol.SanchoMagic
	default:
		return protocol.MainnetMagic
	}
}

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg := defaultConfig()
	if data, err := os.ReadFile(*cfgPath); err == nil {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error parsing config: %v\n", err)
			os.Exit(1)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "error reading config: %v\n", err)
		os.Exit(1)
	}

	// Structured logger with UTC+8 timestamps
	log := buildLogger(cfg.LogLevel)
	defer log.Sync()

	reconnectDelay, err := time.ParseDuration(cfg.ReconnectDelay)
	if err != nil {
		reconnectDelay = 10 * time.Second
	}

	magic := networkMagic(cfg.Network)

	log.Info("starting cardano-p2p-node",
		zap.String("network", cfg.Network),
		zap.Uint32("networkMagic", magic),
		zap.String("listen", cfg.ListenAddr),
		zap.Int("maxInbound", cfg.MaxInbound),
		zap.Int("maxOutbound", cfg.MaxOutbound),
		zap.String("rpcAddr", cfg.RPCAddr),
		zap.Strings("staticPeers", cfg.StaticPeers))

	chainStore := chain.New()
	pool := mempool.New()

	// Install a hook that logs every new transaction with UTC+8 timestamp.
	// This fires synchronously inside Add(), so it must be fast (just logging).
	pool.SetOnAdd(func(e *mempool.TxEntry) {
		log.Info("mempool: new transaction",
			zap.String("txid", e.ID.String()),
			zap.Uint32("size_bytes", e.Size),
			zap.String("from_peer", e.FromPeer),
			zap.String("received_at_jst", e.ReceivedAt.In(jst).Format("2006-01-02 15:04:05 MST")),
			zap.Int("pool_size", 0), // avoid recursive lock; caller holds pool.mu
		)
	})

	mgr := peer.NewManager(peer.Config{
		NetworkMagic:   magic,
		ListenAddr:     cfg.ListenAddr,
		StaticPeers:    cfg.StaticPeers,
		MaxInbound:     cfg.MaxInbound,
		MaxOutbound:    cfg.MaxOutbound,
		ReconnectDelay: reconnectDelay,
	}, chainStore, pool, log)

	// Install connection event hooks for structured logging.
	mgr.SetConnectHook(
		func(info peer.ConnInfo) {
			log.Info("peer connected",
				zap.String("direction", string(info.Direction)),
				zap.String("ip", info.IP),
				zap.String("port", info.Port),
				zap.String("connected_at_jst", info.ConnectedAt.In(jst).Format("2006-01-02 15:04:05 MST")),
			)
		},
		func(info peer.ConnInfo) {
			uptime := time.Since(info.ConnectedAt).Round(time.Second)
			log.Info("peer disconnected",
				zap.String("direction", string(info.Direction)),
				zap.String("ip", info.IP),
				zap.String("port", info.Port),
				zap.Duration("uptime", uptime),
			)
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Shutdown on SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	// Start ledger peers discovery
	lpCfg := cfg.LedgerPeers
	ledgerEnabled := lpCfg.Enabled == nil || *lpCfg.Enabled
	if ledgerEnabled && (lpCfg.SnapshotFile != "" || lpCfg.SnapshotURL != "") {
		refreshInterval, _ := time.ParseDuration(lpCfg.RefreshInterval)

		// For non-mainnet, override the snapshot URL if not explicitly set
		if lpCfg.SnapshotURL == defaultConfig().LedgerPeers.SnapshotURL {
			switch cfg.Network {
			case "preprod":
				lpCfg.SnapshotURL = "https://book.world.dev.cardano.org/environments/preprod/peer-snapshot.json"
			case "preview":
				lpCfg.SnapshotURL = "https://book.world.dev.cardano.org/environments/preview/peer-snapshot.json"
			}
		}

		lm := ledger.NewManager(ledger.Config{
			SnapshotFile:    lpCfg.SnapshotFile,
			SnapshotURL:     lpCfg.SnapshotURL,
			RefreshInterval: refreshInterval,
			MaxPeers:        lpCfg.MaxPeers,
		}, log)

		lm.OnPeers = func(peers []ledger.ResolvedPeer) {
			addrs := make([]string, len(peers))
			for i, p := range peers {
				addrs[i] = p.Addr
			}
			log.Info("ledger peers: feeding addresses to peer manager",
				zap.Int("count", len(addrs)))
			mgr.AddDynamicPeers(ctx, addrs)
		}

		go lm.Run(ctx)
		log.Info("ledger peers discovery started",
			zap.String("url", lpCfg.SnapshotURL),
			zap.String("file", lpCfg.SnapshotFile),
			zap.Duration("refresh", refreshInterval),
			zap.Int("maxPeers", lpCfg.MaxPeers))
	} else {
		log.Info("ledger peers discovery disabled")
	}

	// Start RPC server
	if cfg.RPCAddr != "" {
		rpcSrv := rpc.New(
			cfg.RPCAddr,
			mgr,
			pool,
			func() (uint64, uint64) {
				tip, blockNo := chainStore.Tip()
				return tip.SlotNo, blockNo
			},
			log,
		)
		go func() {
			if err := rpcSrv.Run(ctx); err != nil {
				log.Warn("RPC server stopped", zap.Error(err))
			}
		}()
	}

	// Periodic stats log
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				inb, outb := mgr.Stats()
				tip, blockNo := chainStore.Tip()
				log.Info("node stats",
					zap.Int("inbound_peers", inb),
					zap.Int("outbound_peers", outb),
					zap.Uint64("chain_tip_slot", tip.SlotNo),
					zap.Uint64("chain_tip_block", blockNo),
					zap.Int("mempool_txs", pool.Size()),
					zap.String("stats_at_jst", time.Now().In(jst).Format("2006-01-02 15:04:05 MST")),
				)
			}
		}
	}()

	if err := mgr.Run(ctx); err != nil {
		log.Error("manager error", zap.Error(err))
	}
	log.Info("node stopped")
}

// buildLogger creates a zap logger that:
//   - uses UTC+8 (CST) for all timestamps
//   - outputs console-friendly coloured text to stdout
//   - logs errors to stderr
func buildLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	// Custom time encoder: RFC3339 in UTC+8
	jstTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.In(jst).Format("2006-01-02T15:04:05+08:00"))
	}

	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     jstTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewTee(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			zapLevel,
		),
	)

	return zap.New(core, zap.AddCaller())
}
