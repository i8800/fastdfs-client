package pool

import (
	"bufio"
	"io"
)

const bytesAllocLimit = 1024 * 1024 // 1mb

type Reader struct {
	src *bufio.Reader
	buf []byte
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		src: bufio.NewReader(rd),
		buf: make([]byte, 4096),
	}
}

func (r *Reader) Reset(rd io.Reader) {
	r.src.Reset(rd)
}

func (r *Reader) PeekBuffered() []byte {
	if n := r.src.Buffered(); n != 0 {
		b, _ := r.src.Peek(n)
		return b
	}
	return nil
}

//读取指定的字节 to []byte
func (r *Reader) ReadN(n int) ([]byte, error) {
	b, err := readN(r.src, r.buf, n)
	if err != nil {
		return nil, err
	}

	r.buf = b
	return b, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.src.Read(p)
}

//从指定的io.Reader中读取指定的字节
func readN(r io.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 && b == nil {
		return make([]byte, 0), nil
	}

	if cap(b) >= n {
		b = b[:n]
		_, err := io.ReadFull(r, b)
		return b, err
	}
	b = b[:cap(b)]

	pos := 0
	for pos < n {
		diff := n - len(b)
		if diff > bytesAllocLimit {
			diff = bytesAllocLimit
		}
		b = append(b, make([]byte, diff)...)

		nn, err := io.ReadFull(r, b[pos:])
		if err != nil {
			return nil, err
		}
		pos += nn
	}

	return b, nil
}
