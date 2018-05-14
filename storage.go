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
	mu         sync.Mutex
	closed     bool
}

//下载的数据流，写入到w
func (s *Storage) downloadToWrite(w io.Writer, offset, downloadSize int64) (int, error) {
	conn, err := getConn(s.connPool)
	if err != nil {
		return 0, err
	}

	defer closeConn(s.connPool, conn)

	//构建tracker
	th := buildTrackerHeader(STORAGE_PROTO_CMD_DOWNLOAD_FILE, int64(FDFS_PROTO_PKG_LEN_SIZE*2+FDFS_GROUP_NAME_MAX_LEN+len(s.remoteName)))
	buff := bytes.NewBuffer(th.bytes())
	request := downloadRequestMarshal(offset, downloadSize, s.groupName, s.remoteName)
	buff.Write(request)

	_, err = conn.Write(buff.Bytes())
	if err != nil {
		return 0, err
	}

	if err := th.recvHeader(conn, STORAGE_PROTO_CMD_RESP, -1); err != nil {
		return 0, err
	}

	writesize := 0
	readsize := 0
	buf := make([]byte, 32*1024)
	var downerr error
	for {
		nr, err := conn.Reader.Read(buf)
		if nr > 0 {
			readsize += nr
			nw, ew := w.Write(buf[0:nr])
			if nw > 0 {
				writesize += nw
			}
			if ew != nil {
				downerr = ew
				break
			}
			if nr != nw {
				downerr = errors.New("short write.")
				break
			}
		}
		if err != nil {
			if err != io.EOF {
				downerr = err
			}
			break
		}
		if int64(readsize) >= th.pkgLen {
			break
		}
	}

	if int64(readsize) != th.pkgLen {
		//抹除conn
		s.connPool.Remove(conn)
		conn = nil
	}

	return writesize, downerr
}

func downloadRequestMarshal(offset, downloadSize int64, gn, rn string) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, offset)
	binary.Write(buf, binary.BigEndian, downloadSize)

	buf.Write(buildGroupName(gn))
	buf.WriteString(rn)

	return buf.Bytes()
}
