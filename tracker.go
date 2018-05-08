package go_fastdfs

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/monkey92t/go_fastdfs/pool"
	"io"
)

//fastdfs tracker

type trackerHeader struct {
	pkgLen int64
	cmd    int8
	status int8
}

func buildTrackerHeader(cmd int8, pl int64) *trackerHeader {
	return &trackerHeader{
		pkgLen: pl,
		cmd:    cmd,
		status: 0,
	}
}

func (th *trackerHeader) bytes() []byte {
	buf := new(bytes.Buffer)
	buf.Write(Int64ToBuff(th.pkgLen))
	buf.WriteByte(byte(th.cmd))
	buf.WriteByte(byte(th.status))
	return buf.Bytes()
}

//向conn中写入fastdfs tracker header (10 byte)
//逐步写入pkglen+cmd+status
func (th *trackerHeader) sendHeader(conn *pool.Conn) error {
	_, err := conn.Write(th.bytes())

	return err
}

//从conn中读取tracker header并解析
//tracker header 为 10 bytes
//前8byte为下面要接收字节的大小，9位cmd，10为status，
//status正常为0
func (th *trackerHeader) recvHeader(conn *pool.Conn, cmd int8, needLen int64) error {
	b := make([]byte, 10)
	b, err := conn.Reader.ReadN(10)
	if err != nil {
		return err
	}

	if b[PROTO_HEADER_CMD_INDEX] != byte(cmd) {
		es := fmt.Sprintf("recv cmd error:", b[FDFS_PROTO_PKG_LEN_SIZE], "is not correct.", "expect cmd:", cmd)
		return errors.New(es)
	}

	if b[PROTO_HEADER_STATUS_INDEX] != 0 {
		return errors.New("Recv tracker header error, The status code is not 0.")
	}

	pl := buffToInt64(b, 0)
	if pl < 0 {
		return errors.New(fmt.Sprint("recv body length:", pl, "< 0."))
	}

	if needLen >= 0 && pl != needLen {
		es := fmt.Sprint("recv body length:", pl, "is not correct.", "expect length:", needLen)
		return errors.New(es)
	}

	th.pkgLen = pl

	return nil
}

//向下解包
func (th *trackerHeader) recvPackage(conn *pool.Conn, cmd int8, needLen int64) ([]byte, error) {
	err := th.recvHeader(conn, cmd, needLen)
	if err != nil {
		return nil, err
	}

	buff := new(bytes.Buffer)
	buffsize := int(th.pkgLen)
	for {
		b, err := conn.Reader.ReadN(buffsize)
		if len(b) > 0 {
			buff.Write(b)
			buffsize += len(b)
		}
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}

		if buff.Len() == int(th.pkgLen) {
			break
		}
	}

	if int(th.pkgLen) != buff.Len() {
		return nil, errors.New("recv package size error.")
	}

	return buff.Bytes(), nil
}
