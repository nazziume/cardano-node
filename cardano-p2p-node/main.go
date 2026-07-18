// cardano-p2p-node is a lightweight Cardano relay node implementation in Go.
//
// It participates in the Cardano P2P network by implementing the Ouroboros
// NodeToNode mini-protocols:
//   - Handshake (version negotiation)
//   - ChainSync (block header relay)
//   - BlockFetch (block body relay)
//   - TxSubmission2 (mempool synchronization)
//   - KeepAlive (connection health)
//   - PeerSharing (peer discovery)
//
// Peer quality scoring (why this node scores high):
//
// Cardano nodes score peers using two metrics tracked over a sliding window
// of the last ~180 slots (roughly 1 hour):
//
//  1. upstreamyness: times this node was FIRST to report a new block header.
//     Tracked by ChainSync: when a downstream peer calls MsgRequestNext and
//     we send MsgRollForward, they measure which peer delivered the header first.
//
//  2. fetchynessBlocks: times this node was FIRST to deliver a full block.
//     Tracked by BlockFetch: which peer served the full block body first.
//
// During churn (~every 15 minutes), peers with the lowest
// (upstreamyness + fetchynessBlocks) score are demoted first.
//
// Our strategy to maximize these scores:
//   - Maintain fast upstream connections to multiple well-connected nodes
//   - When we receive a new header, immediately signal downstream peers
//   - Cache full blocks and serve them with minimal latency
//   - Respond to KeepAlive immediately (low RTT is a positive signal)
//   - Enable PeerSharing to appear as a well-connected relay
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
	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/peer"
	"github.com/cardano-p2p-node/internal/protocol"
)

// Config is the top-level node configuration.
type Config struct {
	Network      string        `yaml:"network"`
	ListenAddr   string        `yaml:"listenAddr"`
	StaticPeers  []string      `yaml:"staticPeers"`
	MaxInbound   int           `yaml:"maxInbound"`
	MaxOutbound  int           `yaml:"maxOutbound"`
	ReconnectDelay string      `yaml:"reconnectDelay"`
	LogLevel     string        `yaml:"logLevel"`
}

func defaultConfig() Config {
	return Config{
		Network:      "mainnet",
		ListenAddr:   "0.0.0.0:3000",
		MaxInbound:   50,
		MaxOutbound:  10,
		ReconnectDelay: "10s",
		LogLevel:     "info",
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

	// Set up structured logging
	log := buildLogger(cfg.LogLevel)
	defer log.Sync()

	log.Info("starting cardano-p2p-node",
		zap.String("network", cfg.Network),
		zap.String("listen", cfg.ListenAddr),
		zap.Int("maxInbound", cfg.MaxInbound),
		zap.Int("maxOutbound", cfg.MaxOutbound),
		zap.Strings("staticPeers", cfg.StaticPeers))

	// Parse reconnect delay
	reconnectDelay, err := time.ParseDuration(cfg.ReconnectDelay)
	if err != nil {
		reconnectDelay = 10 * time.Second
	}

	magic := networkMagic(cfg.Network)

	// Shared chain state store: all peers read/write to this
	chainStore := chain.New()

	// Shared mempool
	pool := mempool.New()

	// Peer manager
	mgr := peer.NewManager(peer.Config{
		NetworkMagic:   magic,
		ListenAddr:     cfg.ListenAddr,
		StaticPeers:    cfg.StaticPeers,
		MaxInbound:     cfg.MaxInbound,
		MaxOutbound:    cfg.MaxOutbound,
		ReconnectDelay: reconnectDelay,
	}, chainStore, pool, log)

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	// Print stats periodically
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
					zap.Int("mempool_size", pool.Size()))
			}
		}
	}()

	if err := mgr.Run(ctx); err != nil {
		log.Error("manager error", zap.Error(err))
	}
	log.Info("node stopped")
}

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

	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zapLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding: "console",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalColorLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, _ := cfg.Build()
	return logger
}
