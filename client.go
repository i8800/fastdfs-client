package go_fastdfs

import (
	"bytes"
	"errors"
	"github.com/monkey92t/go_fastdfs/pool"
	"strings"
	"time"
	"io"
)

type FileInfo struct {
	CreateTime time.Time
	Address    string
	FileSize   int64
	Crc32      int
}

//获取文件信息，必要时从存储获取
func (c *FastdfsClient) getFileInfo(fileid string) (*FileInfo, error) {
	groupName, remoteName, err := splitFileid(fileid)
	if err != nil {
		return nil, err
	}

	remoteNameLen := len(remoteName)
	if remoteNameLen < (FDFS_LOGIC_FILE_PATH_LEN + FDFS_FILENAME_BASE64_LENGTH + FDFS_FILE_EXT_NAME_MAX_LEN + 1) {
		return nil, errors.New("error: wrong fileid.")
	}

	end := FDFS_LOGIC_FILE_PATH_LEN + (FDFS_LOGIC_FILE_PATH_LEN + FDFS_FILENAME_BASE64_LENGTH)
	var b string
	if remoteNameLen < end {
		b = remoteName[FDFS_LOGIC_FILE_PATH_LEN:]
	} else {
		b = remoteName[FDFS_LOGIC_FILE_PATH_LEN:end]
	}

	deData, err := decodeAuto(b)
	if err != nil {
		return nil, err
	}

	fsize := buffToInt64(deData, 8)
	if ((remoteNameLen > TRUNK_LOGIC_FILENAME_LENGTH) ||
		((remoteNameLen > NORMAL_LOGIC_FILENAME_LENGTH) && ((fsize & TRUNK_FILE_MARK_SIZE) == 0))) ||
		((fsize & APPENDER_FILE_SIZE) != 0) {
		return c.queryFileInfo(groupName, remoteName)
	}

	if fsize>>63 != 0 {
		fsize &= 0xFFFFFFFF
	} else {
		fsize = -1
	}

	fileinfo := &FileInfo{
		Address:    ipToString(int(buffToInt32(deData, 0))),
		CreateTime: time.Unix(int64(buffToInt32(deData, 4)), 0),
		FileSize:   fsize,
		Crc32:      int(buffToInt32(deData, 16)),
	}

	return fileinfo, nil
}

//从存储服务器获取文件信息
func (c *FastdfsClient) queryFileInfo(groupName, remoteName string) (*FileInfo, error) {
	//组装trackerHeader
	conn, _, err := c.connPool.Get()
	if err != nil {
		return nil, err
	}

	groupBytes := c.buildGroupName(groupName)

	th := buildTrackerHeader(STORAGE_PROTO_CMD_QUERY_FILE_INFO, int64(len(groupBytes)+len(remoteName)))

	whole := new(bytes.Buffer)
	whole.Write(th.bytes())
	whole.Write(groupBytes)
	whole.WriteString(remoteName)

	_, err = conn.Write(whole.Bytes())
	if err != nil {
		return nil, err
	}

	buff, err := th.recvPackage(conn, STORAGE_PROTO_CMD_RESP, 3*FDFS_PROTO_PKG_LEN_SIZE+FDFS_IPADDR_SIZE)
	if err != nil {
		return nil, err
	}

	fsize := buffToInt64(buff, 0)
	ctime := buffToInt64(buff, FDFS_PROTO_PKG_LEN_SIZE)
	crc32 := int(buffToInt64(buff, 2*FDFS_PROTO_PKG_LEN_SIZE))
	start := 3 * FDFS_PROTO_PKG_LEN_SIZE
	end := start + FDFS_IPADDR_SIZE
	ipaddr := string(disAddr(buff[start:end]))

	return &FileInfo{
		Address:    ipaddr,
		CreateTime: time.Unix(ctime, 0),
		FileSize:   fsize,
		Crc32:      crc32,
	}, nil
}

func (c *FastdfsClient) download(groupName, remoteName string) (io.Reader, error) {
	conn, _, err := c.connPool.Get()
	if err != nil {
		return nil, err
	}

	groupBytes := c.buildGroupName(groupName)

	th := buildTrackerHeader(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, int64(len(groupBytes)+len(remoteName)))
	whole := new(bytes.Buffer)
	whole.Write(th.bytes())
	whole.Write(groupBytes)
	whole.WriteString(remoteName)

	_, err = conn.Write(whole.Bytes())
	if err != nil {
		return nil, err
	}

	buff, err := th.recvPackage(conn, STORAGE_PROTO_CMD_RESP, TRACKER_QUERY_STORAGE_FETCH_BODY_LEN)
	if err != nil {
		return nil, err
	}

	ipaddr := disAddr(buff[FDFS_GROUP_NAME_MAX_LEN:FDFS_GROUP_NAME_MAX_LEN+FDFS_IPADDR_SIZE-1])
	port := buffToInt64(buff, FDFS_GROUP_NAME_MAX_LEN+FDFS_IPADDR_SIZE-1)

	return nil, nil
}

//func (c *FastdfsClient) trackerQueryStore(fileid string, cmd int8) ([]byte, error) {
//	groupName, remoteName, err := splitFileid(fileid)
//	if err != nil {
//		return nil, err
//	}
//
//	if len(groupName) > FDFS_GROUP_NAME_MAX_LEN {
//		return nil, errors.New("groupname is too long.")
//	}
//
//	conn, err := c.getPoolConn()
//	if err != nil {
//		return nil, err
//	}
//
//	th := &trackerHeader{
//		pkgLen: int64(FDFS_GROUP_NAME_MAX_LEN + len(remoteName)),
//		cmd:    cmd,
//	}
//
//	if err := th.sendHeader(conn); err != nil {
//		return nil, err
//	}
//
//	fileBuff := bytes.NewBufferString(groupName)
//	for i := fileBuff.Len(); i < FDFS_GROUP_NAME_MAX_LEN; i++ {
//		fileBuff.WriteByte(byte(0))
//	}
//	fileBuff.WriteString(remoteName)
//	if _, err := conn.Write(fileBuff.Bytes()); err != nil {
//		return nil, err
//	}
//
//	if err := th.recvHeader(conn); err != nil {
//		return nil, err
//	}
//
//	recvBuff, err := conn.Reader.ReadN(int(th.pkgLen))
//
//	return recvBuff, err
//}

//组装groupname
func (c *FastdfsClient) buildGroupName(gn string) []byte {
	b := make([]byte, FDFS_GROUP_NAME_MAX_LEN)
	copy(b, []byte(gn))
	return b
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
