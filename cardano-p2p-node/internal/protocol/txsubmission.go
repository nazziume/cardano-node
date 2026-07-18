package protocol

import (
	"fmt"
	"io"
	"time"

	"github.com/fxamacker/cbor/v2"
	"go.uber.org/zap"

	"github.com/cardano-p2p-node/internal/mempool"
	"github.com/cardano-p2p-node/internal/mux"
)

// TxSubmission2 message tags
const (
	tsTagRequestTxIds = 0
	tsTagReplyTxIds   = 1
	tsTagRequestTxs   = 2
	tsTagReplyTxs     = 3
	tsTagDone         = 4
	tsTagInit         = 6
)

// TxSubmissionOutbound is the OUTBOUND (client/initiator) side of the protocol.
// This runs when we CONNECTED OUT to a peer (we're the mux initiator).
// We provide our transactions to their server (they pull from us).
//
// Flow: we send MsgInit, then they pull our txids/txs.
//
// Tracking uses a per-peer SET of already-announced txids rather than an
// integer index. This is robust to RemoveConfirmed() compacting mempool.order.
func TxSubmissionOutbound(mc *mux.Conn, pool *mempool.Mempool, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoTxSubmission)

	// Send MsgInit = [6]
	initMsg, _ := cbor.Marshal([]interface{}{uint8(tsTagInit)})
	if err := mc.Send(mux.ProtoTxSubmission, initMsg); err != nil {
		return fmt.Errorf("txsubmission outbound init: %w", err)
	}

	// announced: txids this peer has been told about (acknowledged or pending ack).
	// We never re-announce a txid to the same peer.
	announced := make(map[string]struct{})
	// pendingAck: txids announced but not yet acknowledged, in FIFO order.
	var pendingAck []string

	for {
		select {
		case <-done:
			return nil
		default:
		}

		// Wait for MsgRequestTxIds from the remote server
		raw, err := readOneMessage(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("txsubmission outbound read: %w", err)
		}

		var msg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if len(msg) < 1 {
			continue
		}

		var tag uint8
		_ = cbor.Unmarshal(msg[0], &tag)

		switch tag {
		case tsTagRequestTxIds:
			// MsgRequestTxIds = [0, blocking, ack, req]
			if len(msg) < 4 {
				continue
			}
			var ackCount, reqCount uint16
			var blocking bool
			_ = cbor.Unmarshal(msg[1], &blocking)
			_ = cbor.Unmarshal(msg[2], &ackCount)
			_ = cbor.Unmarshal(msg[3], &reqCount)

			// Remove acknowledged txids from our pending list.
			// They can now be dropped from our tracking.
			n := int(ackCount)
			if n > len(pendingAck) {
				n = len(pendingAck)
			}
			for _, key := range pendingAck[:n] {
				delete(announced, key)
			}
			pendingAck = pendingAck[n:]

			// Find txids that are currently in the pool but not yet announced.
			all := pool.GetAll()
			var toAnnounce []*mempool.TxEntry
			for _, e := range all {
				key := e.ID.String()
				if _, seen := announced[key]; !seen {
					toAnnounce = append(toAnnounce, e)
					if len(toAnnounce) >= int(reqCount) {
						break
					}
				}
			}

			if len(toAnnounce) == 0 {
				if blocking {
					newTxCh := pool.Subscribe()
					select {
					case <-done:
						pool.Unsubscribe(newTxCh)
						return nil
					case <-newTxCh:
						pool.Unsubscribe(newTxCh)
						// Re-scan after new tx arrived
						all = pool.GetAll()
						for _, e := range all {
							key := e.ID.String()
							if _, seen := announced[key]; !seen {
								toAnnounce = append(toAnnounce, e)
								if len(toAnnounce) >= int(reqCount) {
									break
								}
							}
						}
					}
				} else {
					select {
					case <-done:
						return nil
					case <-time.After(200 * time.Millisecond):
					}
				}
			}

			// Build reply and record what we're announcing.
			txidsAndSizes := make([]interface{}, 0, len(toAnnounce))
			for _, e := range toAnnounce {
				key := e.ID.String()
				announced[key] = struct{}{}
				pendingAck = append(pendingAck, key)
				txidsAndSizes = append(txidsAndSizes, []interface{}{[]byte(e.ID), e.Size})
			}

			if len(toAnnounce) > 0 {
				log.Info("txsubmission outbound: advertising txids to peer",
					zap.Int("count", len(toAnnounce)),
					zap.String("first_txid", toAnnounce[0].ID.String()[:16]+"..."))
			}

			replyMsg, _ := cbor.Marshal([]interface{}{uint8(tsTagReplyTxIds), txidsAndSizes})
			if err := mc.Send(mux.ProtoTxSubmission, replyMsg); err != nil {
				return fmt.Errorf("txsubmission outbound reply txids: %w", err)
			}

		case tsTagRequestTxs:
			// MsgRequestTxs = [2, txidList]
			if len(msg) < 2 {
				continue
			}
			var txidList []cbor.RawMessage
			if err := cbor.Unmarshal(msg[1], &txidList); err != nil {
				continue
			}

			// Look up and return txs
			txList := make([]interface{}, 0, len(txidList))
			logIDs := make([]string, 0, len(txidList))
			for _, rawID := range txidList {
				var txid []byte
				_ = cbor.Unmarshal(rawID, &txid)
				if len(txid) >= 8 {
					logIDs = append(logIDs, mempool.TxID(txid).String()[:16]+"...")
				}
				if e, ok := pool.Get(mempool.TxID(txid)); ok {
					txList = append(txList, e.Raw)
				}
			}
			log.Info("txsubmission outbound: peer requested tx bodies",
				zap.Int("requested", len(txidList)),
				zap.Int("found_in_pool", len(txList)),
				zap.Strings("txids", logIDs))

			// MsgReplyTxs = [3, txList]
			replyMsg, _ := cbor.Marshal([]interface{}{uint8(tsTagReplyTxs), txList})
			if err := mc.Send(mux.ProtoTxSubmission, replyMsg); err != nil {
				return fmt.Errorf("txsubmission outbound reply txs: %w", err)
			}

		case tsTagDone:
			return nil
		}
	}
}

// TxSubmissionInbound is the INBOUND (server/responder) side of the protocol.
// This runs when a peer CONNECTED TO US (we're the mux responder).
// We pull their transactions from them.
//
// Flow: they send MsgInit, then we pull their txids/txs.
func TxSubmissionInbound(mc *mux.Conn, pool *mempool.Mempool, log *zap.Logger, done <-chan struct{}) error {
	r := mc.Reader(mux.ProtoTxSubmission)

	// Wait for MsgInit from the outbound side
	raw, err := readOneMessage(r)
	if err != nil {
		return fmt.Errorf("txsubmission inbound wait init: %w", err)
	}

	var initMsg []cbor.RawMessage
	if err := cbor.Unmarshal(raw, &initMsg); err != nil {
		return fmt.Errorf("txsubmission inbound decode init: %w", err)
	}
	if len(initMsg) < 1 {
		return fmt.Errorf("txsubmission inbound: empty init")
	}
	var initTag uint8
	_ = cbor.Unmarshal(initMsg[0], &initTag)
	if initTag != tsTagInit {
		return fmt.Errorf("txsubmission inbound: expected init (tag=6), got %d", initTag)
	}

	const batchSize = 10
	acked := uint16(0)

	for {
		select {
		case <-done:
			// Send MsgDone = [4]
			doneMsg, _ := cbor.Marshal([]interface{}{uint8(tsTagDone)})
			_ = mc.Send(mux.ProtoTxSubmission, doneMsg)
			return nil
		default:
		}

		// Request txids: MsgRequestTxIds = [0, blocking, ack, req]
		// blocking=false first (non-blocking), then blocking=true to wait for more
		reqMsg, _ := cbor.Marshal([]interface{}{
			uint8(tsTagRequestTxIds),
			false,       // non-blocking
			acked,       // ack count (how many we processed from last batch)
			uint16(batchSize), // request up to batchSize txids
		})
		if err := mc.Send(mux.ProtoTxSubmission, reqMsg); err != nil {
			return fmt.Errorf("txsubmission inbound request txids: %w", err)
		}
		acked = 0 // reset ack count after sending

		// Receive txids reply
		raw, err := readOneMessage(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("txsubmission inbound read txids: %w", err)
		}

		var replyMsg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &replyMsg); err != nil {
			continue
		}
		if len(replyMsg) < 2 {
			continue
		}

		var replyTag uint8
		_ = cbor.Unmarshal(replyMsg[0], &replyTag)
		if replyTag != tsTagReplyTxIds {
			continue
		}

		// Decode txids
		var txidsAndSizes []cbor.RawMessage
		if err := cbor.Unmarshal(replyMsg[1], &txidsAndSizes); err != nil {
			continue
		}

		if len(txidsAndSizes) == 0 {
			// No new txs; send a blocking request to wait for more
			blockingReqMsg, _ := cbor.Marshal([]interface{}{
				uint8(tsTagRequestTxIds),
				true,       // blocking
				acked,
				uint16(batchSize),
			})
			if err := mc.Send(mux.ProtoTxSubmission, blockingReqMsg); err != nil {
				return fmt.Errorf("txsubmission inbound blocking request: %w", err)
			}
			acked = 0

			raw, err = readOneMessage(r)
			if err != nil {
				return fmt.Errorf("txsubmission inbound read blocking reply: %w", err)
			}
			var br []cbor.RawMessage
			if err := cbor.Unmarshal(raw, &br); err != nil {
				continue
			}
			if len(br) >= 2 {
				replyMsg = br
				_ = cbor.Unmarshal(replyMsg[0], &replyTag)
				if replyTag == tsTagReplyTxIds {
					_ = cbor.Unmarshal(replyMsg[1], &txidsAndSizes)
				}
			}
			if len(txidsAndSizes) == 0 {
				continue
			}
		}

		// Extract txids we don't have yet
		var wantTxids []interface{}
		for _, rawEntry := range txidsAndSizes {
			var entry []cbor.RawMessage
			if err := cbor.Unmarshal(rawEntry, &entry); err != nil || len(entry) < 2 {
				continue
			}
			var txid []byte
			_ = cbor.Unmarshal(entry[0], &txid)
			if !pool.Has(mempool.TxID(txid)) {
				wantTxids = append(wantTxids, txid)
			}
		}

		if len(wantTxids) == 0 {
			acked = uint16(len(txidsAndSizes))
			continue
		}

		// Request the actual txs
		reqTxsMsg, _ := cbor.Marshal([]interface{}{uint8(tsTagRequestTxs), wantTxids})
		if err := mc.Send(mux.ProtoTxSubmission, reqTxsMsg); err != nil {
			return fmt.Errorf("txsubmission inbound request txs: %w", err)
		}

		// Receive txs
		raw, err = readOneMessage(r)
		if err != nil {
			return fmt.Errorf("txsubmission inbound read txs: %w", err)
		}

		var txsMsg []cbor.RawMessage
		if err := cbor.Unmarshal(raw, &txsMsg); err != nil {
			continue
		}
		if len(txsMsg) < 2 {
			continue
		}

		var txsTag uint8
		_ = cbor.Unmarshal(txsMsg[0], &txsTag)
		if txsTag != tsTagReplyTxs {
			continue
		}

		var txList []cbor.RawMessage
		if err := cbor.Unmarshal(txsMsg[1], &txList); err != nil {
			continue
		}

		newCount := 0
		for i, rawTx := range txList {
			if i >= len(wantTxids) {
				break
			}
			var txid []byte
			if wantEntry, ok := wantTxids[i].([]byte); ok {
				txid = wantEntry
			}
			entry := &mempool.TxEntry{
				ID:       mempool.TxID(txid),
				Size:     uint32(len(rawTx)),
				Raw:      rawTx,
				FromPeer: mc.Underlying().RemoteAddr().String(),
			}
			if pool.Add(entry) {
				newCount++
			}
		}

		if newCount > 0 {
			log.Debug("txsubmission inbound: added txs", zap.Int("count", newCount), zap.Int("pool_size", pool.Size()))
		}

		acked = uint16(len(txidsAndSizes))
	}
}
