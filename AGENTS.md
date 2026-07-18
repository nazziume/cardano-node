# AGENTS.md

## Cursor Cloud specific instructions

### Repository layout / active project

This repo is a fork of the IntersectMBO `cardano-node` Haskell/Nix monorepo, but the
active work on this branch lives entirely in `cardano-p2p-node/` — a self-contained
**Go** relay node implementing the Ouroboros NodeToNode mini-protocols. Development,
lint, test, build, and run all target that Go module. The surrounding Haskell packages
(`cardano-node/`, `cardano-submit-api/`, etc.) are built via Nix/Cabal and are **not**
part of this branch's workflow; do not attempt a full Nix build unless explicitly asked.

### Go toolchain

Go 1.22 is preinstalled (`go1.22.2`). All commands below run from `cardano-p2p-node/`.

| Task  | Command |
|-------|---------|
| Build | `go build -o cardano-p2p-node .` |
| Lint  | `go vet ./...` |
| Test  | `go test ./...` |
| Run   | `go run . -config config.yaml` (or `./cardano-p2p-node -config config.yaml`) |

The built `cardano-p2p-node` binary is git-ignored (see `cardano-p2p-node/.gitignore`).

### Running / network egress caveats (non-obvious)

- The node connects **outbound** to public Cardano relays listed under `staticPeers`
  in the config; a real end-to-end run needs internet egress. In Cursor Cloud, DNS/egress
  is restricted: the default `staticPeers` in `config.yaml`
  (`backbone.cardano-mainnet.iohk.io`, `backbone.mainnet.emurgornd.com`) do **not**
  resolve. Reachable upstreams confirmed working here:
  - mainnet: `backbone.mainnet.cardanofoundation.org:3001`
  - preprod: `preprod-node.world.dev.cardano.org:30000`
  - preview: `preview-node.play.dev.cardano.org:3001`
  To run against the live chain, point `staticPeers` at one of these (use a local dev
  config file so you don't edit the committed `config.yaml`).
- Once connected, verify liveness via the HTTP RPC server (default `127.0.0.1:8888`):
  `GET /health`, `GET /status` (shows chain tip slot/block + connections),
  `GET /mempool`. A successful run shows `chain_tip_slot`/`chain_tip_block` advancing.
- `logLevel: "debug"` is very verbose (one "new live header" line per poll). Use
  `info` for normal runs. All log timestamps are UTC+8 (CST) by design.
