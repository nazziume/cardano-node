// Package mux implements the Ouroboros network multiplexer protocol.
//
// Wire format (8-byte header, big-endian):
//
//	 0                   1                   2                   3
//	 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                      Transmission Time                        |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|M|     Mini Protocol ID        |        Payload Length         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// M=0 means the segment is from the initiator; M=1 from the responder.
// Max SDU payload for node-to-node: 12 288 bytes.
package mux

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	HeaderLen  = 8
	MaxSDUSize = 12288

	// Protocol IDs for NodeToNode
	ProtoHandshake    uint16 = 0
	ProtoChainSync    uint16 = 2
	ProtoBlockFetch   uint16 = 3
	ProtoTxSubmission uint16 = 4
	ProtoKeepAlive    uint16 = 8
	ProtoPeerSharing  uint16 = 10

	// Mode bits
	ModeInitiator uint16 = 0x0000
	ModeResponder uint16 = 0x8000
)

// Segment is a single mux SDU.
type Segment struct {
	Timestamp uint32
	ProtoID   uint16 // protocol ID, 0x7FFF masked
	IsResp    bool   // true if sent by the responder
	Payload   []byte
}

// Conn wraps a net.Conn with Ouroboros mux framing.
// isInitiator must be true for the side that opened the TCP connection.
type Conn struct {
	conn        net.Conn
	startTime   time.Time
	isInitiator bool

	writeMu sync.Mutex

	// incoming data buffers keyed by protocolID
	// Each protocol gets its own assembler buffer
	incomingMu sync.Mutex
	incoming   map[uint16]*protoBuf
}

type protoBuf struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	cond *sync.Cond
	done bool
}

func newProtoBuf() *protoBuf {
	pb := &protoBuf{}
	pb.cond = sync.NewCond(&pb.mu)
	return pb
}

func (pb *protoBuf) write(data []byte) {
	pb.mu.Lock()
	pb.buf.Write(data)
	pb.cond.Broadcast()
	pb.mu.Unlock()
}

func (pb *protoBuf) close() {
	pb.mu.Lock()
	pb.done = true
	pb.cond.Broadcast()
	pb.mu.Unlock()
}

// Read blocks until at least n bytes are available or the buffer is closed.
func (pb *protoBuf) Read(p []byte) (int, error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	for {
		if pb.buf.Len() > 0 {
			return pb.buf.Read(p)
		}
		if pb.done {
			return 0, io.EOF
		}
		pb.cond.Wait()
	}
}

// New creates a new Mux Conn.
func New(conn net.Conn, isInitiator bool) *Conn {
	return &Conn{
		conn:        conn,
		startTime:   time.Now(),
		isInitiator: isInitiator,
		incoming:    make(map[uint16]*protoBuf),
	}
}

// Underlying returns the underlying TCP connection.
func (c *Conn) Underlying() net.Conn {
	return c.conn
}

// getOrCreateBuf returns the incoming buffer for a given protocol.
func (c *Conn) getOrCreateBuf(protoID uint16) *protoBuf {
	c.incomingMu.Lock()
	defer c.incomingMu.Unlock()
	if pb, ok := c.incoming[protoID]; ok {
		return pb
	}
	pb := newProtoBuf()
	c.incoming[protoID] = pb
	return pb
}

// Reader returns an io.Reader for the incoming stream of a given protocol.
func (c *Conn) Reader(protoID uint16) io.Reader {
	return c.getOrCreateBuf(protoID)
}

// ReadLoop reads all incoming mux segments and routes them to protocol buffers.
// It runs until the connection is closed.
func (c *Conn) ReadLoop() error {
	header := make([]byte, HeaderLen)
	for {
		if _, err := io.ReadFull(c.conn, header); err != nil {
			c.closeAllBufs()
			return fmt.Errorf("mux read header: %w", err)
		}

		_ = binary.BigEndian.Uint32(header[0:4]) // timestamp (we don't use it)
		modeProto := binary.BigEndian.Uint16(header[4:6])
		length := binary.BigEndian.Uint16(header[6:8])
		protoID := modeProto & 0x7FFF
		isResp := (modeProto & 0x8000) != 0

		// Validate direction: we should only receive from the other side
		// isInitiator=true → we receive from responder (isResp=true)
		// isInitiator=false → we receive from initiator (isResp=false)
		if c.isInitiator != !isResp {
			// Direction mismatch – skip payload but don't error;
			// some versions send keepalive from both sides
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			c.closeAllBufs()
			return fmt.Errorf("mux read payload (proto=%d len=%d): %w", protoID, length, err)
		}

		_ = isResp
		buf := c.getOrCreateBuf(protoID)
		buf.write(payload)
	}
}

func (c *Conn) closeAllBufs() {
	c.incomingMu.Lock()
	defer c.incomingMu.Unlock()
	for _, pb := range c.incoming {
		pb.close()
	}
}

// Close closes the underlying connection and all protocol buffers.
func (c *Conn) Close() error {
	c.closeAllBufs()
	return c.conn.Close()
}

// transmissionTime returns the lower 32 bits of microseconds since start.
func (c *Conn) transmissionTime() uint32 {
	return uint32(time.Since(c.startTime).Microseconds())
}

// Send writes a message for a protocol, splitting into SDUs as needed.
// isResponder should match our role: true if we accepted the connection.
func (c *Conn) Send(protoID uint16, data []byte) error {
	isResponder := !c.isInitiator
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	offset := 0
	for offset < len(data) {
		end := offset + MaxSDUSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		offset = end

		ts := c.transmissionTime()
		modeProto := protoID
		if isResponder {
			modeProto |= ModeResponder
		}

		hdr := make([]byte, HeaderLen)
		binary.BigEndian.PutUint32(hdr[0:4], ts)
		binary.BigEndian.PutUint16(hdr[4:6], modeProto)
		binary.BigEndian.PutUint16(hdr[6:8], uint16(len(chunk)))

		if _, err := c.conn.Write(hdr); err != nil {
			return err
		}
		if _, err := c.conn.Write(chunk); err != nil {
			return err
		}
	}
	return nil
}
