package cache

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/bufpool"
	"github.com/vmihailenco/msgpack/v4"
)

const compressionThreshold = 64

const (
	noCompression = 0x0
	s2Compression = 0x1
)

var epoch = time.Date(2020, time.January, 01, 00, 0, 0, 0, time.UTC).Unix()

func encodeTime(b []byte, tm time.Time) {
	secs := tm.Unix() - epoch
	binary.LittleEndian.PutUint32(b, uint32(secs))
}

func decodeTime(b []byte) time.Time {
	secs := binary.LittleEndian.Uint32(b)
	return time.Unix(int64(secs)+epoch, 0)
}

//------------------------------------------------------------------------------

var encPool = sync.Pool{
	New: func() interface{} {
		return msgpack.NewEncoder(nil)
	},
}

func marshal(buf *bufpool.Buffer, value interface{}) ([]byte, error) {
	enc := encPool.Get().(*msgpack.Encoder)

	enc.Reset(buf)
	enc.UseCompactEncoding(true)

	err := enc.Encode(value)

	encPool.Put(enc)

	if err != nil {
		return nil, err
	}

	b := buf.Bytes()

	if len(b) < compressionThreshold {
		b = append(b, noCompression)
		return b, nil
	}

	n := s2.MaxEncodedLen(len(b))

	buf2 := bufpool.Get(n + 1)
	compressed := buf2.Bytes()

	compressed = s2.Encode(compressed, b)
	compressed = append(compressed, s2Compression)

	buf.ResetBytes(compressed)

	buf2.ResetBytes(b)
	bufpool.Put(buf2)

	return compressed, nil
}

func unmarshal(b []byte, value interface{}) error {
	if len(b) == 0 {
		return nil
	}

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]

		n, err := s2.DecodedLen(b)
		if err != nil {
			return err
		}

		buf := bufpool.Get(n)
		defer bufpool.Put(buf)

		b, err = s2.Decode(buf.Bytes(), b)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("uknownn compression method: %x", c)
	}

	return msgpack.Unmarshal(b, value)
}
