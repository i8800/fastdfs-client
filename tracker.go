package go_fastdfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/monkey92t/go_fastdfs/pool"
)

//fastdfs tracker

type trackerHeader struct {
	pkgLen int64
	cmd    int8
	status int8
}

//向conn中写入fastdfs tracker header (10 byte)
//逐步写入pkglen+cmd+status
func (th *trackerHeader) sendHeader(conn *pool.Conn) error {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.BigEndian, th.pkgLen)
	if err == nil {
		buf.WriteByte(byte(th.cmd))
		buf.WriteByte(byte(th.status))
		_, err = conn.Write(buf.Bytes())
	}

	return err
}

//从conn中读取tracker header并解析
//tracker header 为 10 bytes
//前8byte为下面要接收字节的大小，9位cmd，10为status，
//status正常为0
func (th *trackerHeader) recvHeader(conn *pool.Conn) error {
	b := make([]byte, 10)
	b, err := conn.Reader.ReadN(10)
	if err != nil {
		return err
	}

	buf := bytes.NewReader(b)
	err = binary.Read(buf, binary.BigEndian, &th.pkgLen)
	if err != nil {
		return err
	}

	cmd, err := buf.ReadByte()
	if err != nil {
		return err
	}

	status, err := buf.ReadByte()
	if err != nil {
		return err
	}

	if 0 != status {
		return errors.New("Recv tracker header error, The status code is not 0.")
	}

	th.cmd = int8(cmd)
	th.status = int8(status)

	return nil
}
