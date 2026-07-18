# cardano-p2p-node

A Go implementation of a Cardano relay node that participates in the P2P network via the Ouroboros NodeToNode mini-protocols.

## Overview

This node is designed to achieve **maximum peer quality scores** in the Cardano P2P network by acting as a fast, reliable relay between upstream block producers/relays and downstream peers.

## Peer Quality Scoring (How Cardano Evaluates Peers)

Cardano nodes track two metrics per peer over a sliding window of ~180 slots (~1 hour on mainnet):

| Metric | What it measures | How to maximize it |
|--------|-----------------|-------------------|
| **upstreamyness** | How often a peer was **first** to deliver a block header for a given slot | Forward headers from upstream immediately |
| **fetchynessBlocks** | How often a peer was **first** to deliver a full block | Cache blocks and serve them with minimal latency |

**Demotion policy** (from `Ouroboros.Network.Diffusion.Policies`):
```haskell
-- Sort peers by ascending score; demote lowest-scoring first
let scores = Map.unionWith (+) upstreamyness fetchynessBlocks
```

During churn (every ~15 min in deadline mode, ~5 min in bulk sync), peers with the **lowest** combined score are demoted from the hot peer set. Peers with higher scores are kept.

**Our strategy:**
1. Connect to multiple well-known upstream nodes
2. When a new header arrives from *any* upstream, signal **all** downstream ChainSync servers immediately
3. Download full blocks from upstream and serve them to downstream peers on request
4. Respond to KeepAlive pings without delay (low RTT is a positive signal)
5. Enable PeerSharing so peers see us as well-connected

## Implemented Protocols

| Protocol | ID | Role | Purpose |
|----------|---|------|---------|
| **Handshake** | 0 | Both | Version negotiation (NtN v14, v15) |
| **ChainSync** | 2 | Client + Server | Header relay — critical for upstreamyness |
| **BlockFetch** | 3 | Client + Server | Block relay — critical for fetchyness |
| **TxSubmission2** | 4 | Both | Mempool synchronization |
| **KeepAlive** | 8 | Both | Connection health, RTT measurement |
| **PeerSharing** | 10 | Both | Peer discovery |

### Protocol Roles by Connection Direction

```
Outbound connection (we initiate TCP):
  → Handshake initiator (we propose versions)
  → ChainSync CLIENT   (pull headers from them)
  → BlockFetch CLIENT  (pull blocks from them)
  → TxSubmission OUTBOUND (provide our txs)
  → KeepAlive CLIENT   (send pings, measure RTT)
  → PeerSharing CLIENT (discover their peers)

Inbound connection (they initiate TCP):
  → Handshake responder (we accept their version)
  → ChainSync SERVER   (serve our headers → builds upstreamyness)
  → BlockFetch SERVER  (serve our blocks  → builds fetchyness)
  → TxSubmission INBOUND (collect their txs)
  → KeepAlive SERVER   (respond to pings immediately)
  → PeerSharing SERVER (share our known peers)
```

## Mux Wire Format

The Ouroboros multiplexer uses 8-byte segment headers (big-endian):

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Transmission Time                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|M|     Mini Protocol ID        |        Payload Length         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **M=0**: segment from the initiator (TCP connector)
- **M=1**: segment from the responder (TCP acceptor)
- Max payload: 12,288 bytes per SDU (larger messages are segmented)

## Quick Start

### Build

```bash
cd cardano-p2p-node
go build -o cardano-p2p-node .
```

### Configure

Edit `config.yaml`:

```yaml
# Network: mainnet | preview | preprod | sanchonet
network: mainnet

# Listen for inbound connections (the more inbound peers, the better our scores)
listenAddr: "0.0.0.0:3000"

# Well-known upstream peers to maintain persistent connections to
staticPeers:
  - "backbone.cardano-mainnet.iohk.io:3001"
  - "backbone.mainnet.emurgornd.com:3000"
  - "backbone.mainnet.cardanofoundation.org:3001"

# Accept up to 50 inbound connections
maxInbound: 50
maxOutbound: 10

logLevel: "info"
```

### Run

```bash
./cardano-p2p-node -config config.yaml
```

### Testnet (preprod)

```yaml
network: preprod
staticPeers:
  - "preprod-node.world.dev.cardano.org:30000"
```

## Architecture

```
                ┌─────────────────────────────────────┐
                │           cardano-p2p-node           │
                │                                     │
  Upstream      │   ┌──────────────┐                  │   Downstream
  Cardano  ───► │   │ Chain Store  │ ◄─────────────── │ ◄─── peers
  nodes         │   │ (ring buf)   │                  │      connect
                │   └──────────────┘                  │      to us
                │          │  notify on new header     │
                │          ▼                           │
                │   ┌──────────────┐                  │
                │   │  Mempool     │                  │
                │   └──────────────┘                  │
                └─────────────────────────────────────┘

  Outbound connections (to upstream):          Inbound connections (from downstream):
  ┌─────────────────────────────────┐         ┌─────────────────────────────────┐
  │ ChainSync CLIENT   ──► store   │         │ ChainSync SERVER  ◄── store    │
  │ BlockFetch CLIENT  ──► store   │         │ BlockFetch SERVER ◄── store    │
  │ TxSubmission OUT   ──► them    │         │ TxSubmission IN   ──► mempool  │
  │ KeepAlive CLIENT               │         │ KeepAlive SERVER  (instant)    │
  │ PeerSharing CLIENT             │         │ PeerSharing SERVER             │
  └─────────────────────────────────┘         └─────────────────────────────────┘
```

## Key Design Decisions

### Instant Header Relay

The chain store uses a pub/sub model with buffered Go channels. When a new header arrives from any upstream ChainSync client:

1. `store.AddHeader(h)` is called — O(1) operation
2. All downstream ChainSync servers are signaled via `chan struct{}`
3. Any server blocked on `MsgAwaitReply` wakes up and sends `MsgRollForward` immediately

This minimizes the time between "new block produced" and "downstream peer informed", maximizing our `upstreamyness` score.

### Block Caching

The BlockFetch client downloads full block bodies for every header in the rolling window (last 256 blocks). When a downstream peer requests a block:

- If in cache: served immediately (good for `fetchynessBlocks`)
- If not in cache: responds with `MsgNoBlocks`

### Version Negotiation

Supports NodeToNode v14 (Plomin HF, mandatory on mainnet since Jan 2025) and v15 (SRV support). Both versions use the same version data format:
```
[networkMagic, initiatorOnlyDiffusionMode, peerSharing, query]
```

We always advertise:
- `initiatorOnlyDiffusionMode = false` → full bidirectional mode (InitiatorAndResponder)
- `peerSharing = 1` → PeerSharing enabled

## Project Structure

```
cardano-p2p-node/
├── main.go                     # Entry point, config loading, signal handling
├── config.yaml                 # Example configuration
├── internal/
│   ├── mux/
│   │   ├── mux.go              # Ouroboros multiplexer (SDU framing)
│   │   └── mux_test.go
│   ├── chain/
│   │   ├── store.go            # Ring buffer chain store with pub/sub
│   │   └── store_test.go
│   ├── mempool/
│   │   ├── mempool.go          # In-memory transaction pool
│   │   └── mempool_test.go
│   ├── protocol/
│   │   ├── handshake.go        # NtN handshake (propose/accept)
│   │   ├── keepalive.go        # KeepAlive client and server
│   │   ├── chainsync.go        # ChainSync client and server
│   │   ├── blockfetch.go       # BlockFetch client and server
│   │   ├── txsubmission.go     # TxSubmission2 (outbound + inbound)
│   │   ├── peersharing.go      # PeerSharing client and server
│   │   └── *_test.go
│   └── peer/
│       ├── peer.go             # Single connection lifecycle manager
│       └── manager.go          # Multi-peer connection manager
```

## Scoring Churn Cycle

The Cardano P2P governor runs churn every:
- **~3300s** (55 min) in deadline/Praos mode
- **~900s** (15 min) in bulk sync / Genesis mode

During churn, the governor temporarily lowers the number of active (hot) peer targets, causing the worst-scoring peers to be demoted. After a short wait it raises targets again, accepting new peers.

To survive churn: maintain high `upstreamyness + fetchynessBlocks`. Our instant-relay architecture is designed specifically to maximize these values.
