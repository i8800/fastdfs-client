package go_fastdfs

import (
	"bytes"
	"errors"
	"github.com/monkey92t/go_fastdfs/pool"
	"strings"
	"time"
)

type FileInfo struct {
	CreateTime time.Time
	Address    string
	FileSize   int
	Crc32      int
}

//对fastdfs操作
func (c *FastdfsClient) getStorageInfo(fileid string) (*FileInfo, error) {
	_, remoteName, err := splitFileid(fileid)
	if err != nil {
		return nil, err
	}
	end := FDFS_LOGIC_FILE_PATH_LEN + (FDFS_LOGIC_FILE_PATH_LEN + FDFS_FILENAME_BASE64_LENGTH)
	var b string
	if len(remoteName) < end {
		b = remoteName[FDFS_LOGIC_FILE_PATH_LEN:]
	} else {
		b = remoteName[FDFS_LOGIC_FILE_PATH_LEN:end]
	}

	deData, err := decodeAuto(b)
	if err != nil {
		return nil, err
	}

	fileinfo := &FileInfo{
		Address:    ipToString(int(buffToInt32(deData, 0))),
		CreateTime: time.Unix(int64(buffToInt32(deData, 4)), 0),
		Crc32:      int(buffToInt32(deData, 16)),
	}
	size := buffToInt64(deData, 8)
	if size>>63 != 0 {
		size &= 0xFFFFFFFF
	} else {
		size = -1
	}
	fileinfo.FileSize = int(size)

	return fileinfo, nil
}

func (c *FastdfsClient) trackerQueryStore(fileid string, cmd int8) ([]byte, error) {
	groupName, remoteName, err := splitFileid(fileid)
	if err != nil {
		return nil, err
	}

	if len(groupName) > FDFS_GROUP_NAME_MAX_LEN {
		return nil, errors.New("groupname is too long.")
	}

	conn, err := c.getPoolConn()
	if err != nil {
		return nil, err
	}

	th := &trackerHeader{
		pkgLen: int64(FDFS_GROUP_NAME_MAX_LEN + len(remoteName)),
		cmd:    cmd,
	}

	if err := th.sendHeader(conn); err != nil {
		return nil, err
	}

	fileBuff := bytes.NewBufferString(groupName)
	for i := fileBuff.Len(); i < FDFS_GROUP_NAME_MAX_LEN; i++ {
		fileBuff.WriteByte(byte(0))
	}
	fileBuff.WriteString(remoteName)
	if _, err := conn.Write(fileBuff.Bytes()); err != nil {
		return nil, err
	}

	if err := th.recvHeader(conn); err != nil {
		return nil, err
	}

	recvBuff, err := conn.Reader.ReadN(int(th.pkgLen))

	return recvBuff, err
}

//获取默认链接的fastdfs pool.conn
func (c *FastdfsClient) getPoolConn() (*pool.Conn, error) {
	conn, _, err := c.connPool.Get()
	return conn, err
}

//根据ip:port获取一个可用的存储*pool.Conn
//如果没有则创建一个
func (c *FastdfsClient) getStorePoolConn(addr string) (*pool.Conn, error) {
	addr = strings.TrimSpace(addr)
	if "" == addr {
		return nil, errors.New("addr is null.")
	}

	var conn *pool.Conn
	var err error

	if addr != c.opt.Addr {
		c.mu.Lock()
		p, ok := c.storePools[addr]
		if !ok {
			poolOpt := c.getPoolOpt()
			poolOpt.Dialer = defaultDialer(addr, c.opt.DialTimeout)
			p = pool.NewConnPool(poolOpt)
			c.storePools[addr] = p
		}
		c.mu.Unlock()
		conn, _, err = p.Get()
	} else {
		conn, _, err = c.connPool.Get()
	}

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func splitFileid(fid string) (string, string, error) {
	fid = strings.TrimSpace(fid)
	p := strings.SplitN(fid, "/", 2)
	if len(p) < 2 {
		return "", "", errors.New("Fileid format error.")
	}

	return p[0], p[1], nil
}
