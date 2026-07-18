package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// readOneMessageLenient reads exactly one CBOR value from r, returning its
// raw bytes.  Unlike the standard CBOR decoder used in readOneMessage, this
// function accepts non-standard additional-info values (28–30) that appear in
// Cardano's block CBOR encoding (the Haskell cborg library uses CBOR
// extensions not sanctioned by RFC 8949).
//
// The function performs structural byte-counting only—it does not validate
// semantic correctness—which is exactly what we need for opaque block relay.
func readOneMessageLenient(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	if err := readLenientValue(r, &buf, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

const (
	maxCBORDepth   = 64
	maxPayloadSize = 8 << 20 // 8 MB single-value limit (block bodies are < 1 MB)
)

func readLenientValue(r io.Reader, out *bytes.Buffer, depth int) error {
	if depth > maxCBORDepth {
		return fmt.Errorf("cbor: nesting too deep")
	}

	hdr := make([]byte, 1)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return fmt.Errorf("cbor: read header: %w", err)
	}
	out.WriteByte(hdr[0])

	majorType := hdr[0] >> 5
	addInfo := hdr[0] & 0x1f

	var arg uint64
	switch addInfo {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
		15, 16, 17, 18, 19, 20, 21, 22, 23:
		arg = uint64(addInfo)
	case 24:
		b := readN(r, out, 1)
		if b == nil {
			return fmt.Errorf("cbor: ai24 read failed")
		}
		arg = uint64(b[0])
	case 25:
		b := readN(r, out, 2)
		if b == nil {
			return fmt.Errorf("cbor: ai25 read failed")
		}
		arg = uint64(binary.BigEndian.Uint16(b))
	case 26:
		b := readN(r, out, 4)
		if b == nil {
			return fmt.Errorf("cbor: ai26 read failed")
		}
		arg = uint64(binary.BigEndian.Uint32(b))
	case 27:
		b := readN(r, out, 8)
		if b == nil {
			return fmt.Errorf("cbor: ai27 read failed")
		}
		arg = binary.BigEndian.Uint64(b)
	case 28, 29, 30:
		// Reserved/non-standard (Cardano cborg extension): treat as arg=0,
		// no payload bytes. The next byte in the stream is the next value.
		arg = 0
	case 31:
		return readLenientIndefinite(r, out, majorType, depth)
	}

	// Sanity-check arg to avoid huge allocations
	if arg > maxPayloadSize {
		return fmt.Errorf("cbor: value too large (%d bytes)", arg)
	}

	switch majorType {
	case 0, 1: // uint, negint – no payload
		return nil
	case 2, 3: // bstr, tstr – arg bytes payload
		return readRawBytes(r, out, arg)
	case 4: // array – arg items
		for i := uint64(0); i < arg; i++ {
			if err := readLenientValue(r, out, depth+1); err != nil {
				return err
			}
		}
	case 5: // map – arg key-value pairs
		for i := uint64(0); i < arg*2; i++ {
			if err := readLenientValue(r, out, depth+1); err != nil {
				return err
			}
		}
	case 6: // tag – one value follows
		return readLenientValue(r, out, depth+1)
	case 7: // float16/32/64 – extra bytes already read via readN above
	}
	return nil
}

// readLenientIndefinite handles additional-info == 31.
func readLenientIndefinite(r io.Reader, out *bytes.Buffer, majorType uint8, depth int) error {
	switch majorType {
	case 7: // 0xff = break – header already written
		return nil

	case 2, 3: // indefinite bstr/tstr – definite chunks until 0xff
		for {
			chunkHdr := make([]byte, 1)
			if _, err := io.ReadFull(r, chunkHdr); err != nil {
				return fmt.Errorf("cbor: indef bstr: chunk hdr: %w", err)
			}
			out.WriteByte(chunkHdr[0])
			if chunkHdr[0] == 0xff { // break
				return nil
			}
			chunkAddInfo := chunkHdr[0] & 0x1f
			var chunkLen uint64
			switch chunkAddInfo {
			case 24:
				b := readN(r, out, 1)
				if b == nil {
					return fmt.Errorf("cbor: indef chunk ai24")
				}
				chunkLen = uint64(b[0])
			case 25:
				b := readN(r, out, 2)
				if b == nil {
					return fmt.Errorf("cbor: indef chunk ai25")
				}
				chunkLen = uint64(binary.BigEndian.Uint16(b))
			case 26:
				b := readN(r, out, 4)
				if b == nil {
					return fmt.Errorf("cbor: indef chunk ai26")
				}
				chunkLen = uint64(binary.BigEndian.Uint32(b))
			case 27:
				b := readN(r, out, 8)
				if b == nil {
					return fmt.Errorf("cbor: indef chunk ai27")
				}
				chunkLen = binary.BigEndian.Uint64(b)
			default:
				chunkLen = uint64(chunkAddInfo)
			}
			if chunkLen > maxPayloadSize {
				return fmt.Errorf("cbor: indef chunk too large (%d)", chunkLen)
			}
			if err := readRawBytes(r, out, chunkLen); err != nil {
				return err
			}
		}

	default: // 4=array, 5=map, 6=tag – items until break
		for {
			peek := make([]byte, 1)
			if _, err := io.ReadFull(r, peek); err != nil {
				return fmt.Errorf("cbor: indef container item: %w", err)
			}
			if peek[0] == 0xff { // break
				out.WriteByte(peek[0])
				return nil
			}
			// Prepend the peeked byte back into the value stream
			combined := io.MultiReader(bytes.NewReader(peek), r)
			if err := readLenientValue(combined, out, depth+1); err != nil {
				return err
			}
		}
	}
}

// readN reads exactly n bytes, appends them to out, and returns them.
// Returns nil on error.
func readN(r io.Reader, out *bytes.Buffer, n int) []byte {
	if n == 0 {
		return []byte{}
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil
	}
	out.Write(buf)
	return buf
}

// readRawBytes reads exactly n bytes and appends to out.
func readRawBytes(r io.Reader, out *bytes.Buffer, n uint64) error {
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("cbor: read %d payload bytes: %w", n, err)
	}
	out.Write(buf)
	return nil
}

// splitLenientArray parses a CBOR array whose elements may contain
// non-standard additional-info bytes (28–30, Cardano cborg extensions).
// It returns the raw CBOR bytes of each element without validating their content.
// This is used by BlockFetch to parse [tag, block_body] messages where
// block_body contains Haskell cborg extensions that fxamacker/cbor rejects.
func splitLenientArray(data []byte) ([][]byte, error) {
	r := bytes.NewReader(data)

	hdr := make([]byte, 1)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("cbor: read array header: %w", err)
	}
	majorType := hdr[0] >> 5
	addInfo := hdr[0] & 0x1f
	if majorType != 4 {
		return nil, fmt.Errorf("cbor: expected array (major type 4), got type %d", majorType)
	}

	indefinite := addInfo == 31
	var count uint64
	if !indefinite {
		switch {
		case addInfo <= 23:
			count = uint64(addInfo)
		case addInfo == 24:
			b := make([]byte, 1)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, fmt.Errorf("cbor: array length ai24: %w", err)
			}
			count = uint64(b[0])
		case addInfo == 25:
			b := make([]byte, 2)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, fmt.Errorf("cbor: array length ai25: %w", err)
			}
			count = uint64(binary.BigEndian.Uint16(b))
		case addInfo == 26:
			b := make([]byte, 4)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, fmt.Errorf("cbor: array length ai26: %w", err)
			}
			count = uint64(binary.BigEndian.Uint32(b))
		case addInfo == 27:
			b := make([]byte, 8)
			if _, err := io.ReadFull(r, b); err != nil {
				return nil, fmt.Errorf("cbor: array length ai27: %w", err)
			}
			count = binary.BigEndian.Uint64(b)
		default:
			return nil, fmt.Errorf("cbor: unsupported array length encoding ai=%d", addInfo)
		}
	}

	var elements [][]byte
	for {
		if !indefinite && uint64(len(elements)) >= count {
			break
		}
		if r.Len() == 0 {
			break
		}
		// For indefinite-length arrays, check for break code (0xff).
		if indefinite {
			peek := make([]byte, 1)
			if _, err := io.ReadFull(r, peek); err != nil {
				break
			}
			if peek[0] == 0xff {
				break
			}
			// Not a break — prepend back and read a full value.
			remaining := make([]byte, r.Len())
			_, _ = io.ReadFull(r, remaining)
			r = bytes.NewReader(append(peek, remaining...))
		}

		var buf bytes.Buffer
		if err := readLenientValue(r, &buf, 0); err != nil {
			return nil, fmt.Errorf("cbor: array element %d: %w", len(elements), err)
		}
		elements = append(elements, buf.Bytes())
	}
	return elements, nil
}
