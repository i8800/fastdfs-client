package go_fastdfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/monkey92t/go_fastdfs/pool"
	"io"
	"sync"
)

type Request interface {
	marshal() []byte
}
type Response interface {
	unmarshal([]byte) error
}

type Storage struct {
	addr       string
	groupName  string
	remoteName string
	pathIndex  int
	connPool   *pool.ConnPool
	conn       *pool.Conn
	mu         sync.Mutex
	closed     bool
}

func (s *Storage) downloadFile(offset, downloadSize int64) (io.ReadCloser, error) {
	conn, err := getConn(s.connPool)
	if err != nil {
		return nil, err
	}

	defer s.connPool.Put(conn)

	//构建tracker
	th := buildTrackerHeader(STORAGE_PROTO_CMD_DOWNLOAD_FILE, int64(FDFS_PROTO_PKG_LEN_SIZE*2+FDFS_GROUP_NAME_MAX_LEN+len(s.remoteName)))
	buff := bytes.NewBuffer(th.bytes())
	request := downloadRequestMarshal(offset, downloadSize, s.groupName, s.remoteName)
	buff.Write(request)

	_, err = conn.Write(buff.Bytes())
	if err != nil {
		return nil, err
	}

	if err := th.recvHeader(conn, STORAGE_PROTO_CMD_RESP, -1); err != nil {
		return nil, err
	}

	s.conn = conn
	return s, nil
}

func (s *Storage) Close() error {
	if s.closed || s.conn == nil {
		return errors.New("storage is closed.")
	}

	s.mu.Lock()
	if s.conn == nil {
		s.closed = true
		s.mu.Unlock()
		return errors.New("storage is closed.")
	}
	s.connPool.Put(s.conn)
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *Storage) Read(p []byte) (n int, err error) {
	if s.closed {
		return 0, errors.New("storage is closed.")
	}
	return s.conn.Reader.Read(p)
}

func downloadRequestMarshal(offset, downloadSize int64, gn, rn string) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, offset)
	binary.Write(buf, binary.BigEndian, downloadSize)

	buf.Write(buildGroupName(gn))
	buf.WriteString(rn)

	return buf.Bytes()
}
