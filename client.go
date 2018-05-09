package go_fastdfs

import (
	"bytes"
	"errors"
	"github.com/monkey92t/go_fastdfs/pool"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type FileInfo struct {
	CreateTime time.Time
	Address    string
	FileSize   int64
	Crc32      int
}

//获取文件信息，必要时从存储获取
func (c *FastdfsClient) FileInfo(fileid string) (*FileInfo, error) {
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
	conn, err := getConn(c.connPool)
	if err != nil {
		return nil, err
	}

	defer c.connPool.Put(conn)

	groupBytes := buildGroupName(groupName)

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
	ipaddr := string(readStr(buff[start:end]))

	return &FileInfo{
		Address:    ipaddr,
		CreateTime: time.Unix(ctime, 0),
		FileSize:   fsize,
		Crc32:      crc32,
	}, nil
}

func (c *FastdfsClient) DownloadToIoReader(fileid string, offset, size int64) (io.ReadCloser, error) {
	groupName, remoteName, err := splitFileid(fileid)
	if err != nil {
		return nil, err
	}

	return c.download(groupName, remoteName, offset, size)
}

func (c *FastdfsClient) download(groupName, remoteName string, offset, size int64) (io.ReadCloser, error) {
	storage, err := c.queryStorage(groupName, remoteName, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE)
	if err != nil {
		return nil, err
	}

	return storage.downloadFile(offset, size)
}

//获取默认链接的fastdfs pool.conn
func (c *FastdfsClient) getPoolConn() (*pool.Conn, error) {
	return getConn(c.connPool)
}

//根据op:port 获取一个pool.connpool
func (c *FastdfsClient) getStoragePool(addr string) (*pool.ConnPool, error) {
	addr = strings.TrimSpace(addr)
	if "" == addr {
		return nil, errors.New("addr is null.")
	}

	if addr == c.opt.Addr {
		return c.connPool, nil
	}

	c.mu.Lock()
	p, ok := c.storePools[addr]
	if !ok {
		poolOpt := c.getPoolOpt()
		poolOpt.Dialer = defaultDialer(addr, c.opt.DialTimeout)
		p = pool.NewConnPool(poolOpt)
		c.storePools[addr] = p
	}
	c.mu.Unlock()

	return p, nil
}

//根据ip:port获取一个可用的存储*pool.Conn
//如果没有则创建一个
func (c *FastdfsClient) getStorePoolConn(addr string) (*pool.Conn, error) {
	var conn *pool.Conn
	var err error

	if addr != c.opt.Addr {
		p, err := c.getStoragePool(addr)
		if err != nil {
			return nil, err
		}
		conn, err = getConn(p)
	} else {
		conn, err = getConn(c.connPool)
	}

	if err != nil {
		return nil, err
	}

	return conn, nil
}

//查询已有文件存储信息
func (c *FastdfsClient) queryStorage(gname, rname string, cmd int8) (*Storage, error) {
	conn, err := getConn(c.connPool)
	if err != nil {
		return nil, err
	}

	defer c.connPool.Put(conn)

	groupBytes := buildGroupName(gname)

	th := buildTrackerHeader(cmd, int64(len(groupBytes)+len(rname)))
	whole := new(bytes.Buffer)
	whole.Write(th.bytes())
	whole.Write(groupBytes)
	whole.WriteString(rname)

	_, err = conn.Write(whole.Bytes())
	if err != nil {
		return nil, err
	}

	buff, err := th.recvPackage(conn, STORAGE_PROTO_CMD_RESP, -1)
	if err != nil {
		return nil, err
	}

	blen := len(buff)
	if blen < TRACKER_QUERY_STORAGE_FETCH_BODY_LEN {
		return nil, errors.New("Invalid body length: " + strconv.Itoa(blen))
	}

	if ((blen - TRACKER_QUERY_STORAGE_FETCH_BODY_LEN) % (FDFS_IPADDR_SIZE - 1)) != 0 {
		return nil, errors.New("Invalid body length: " + strconv.Itoa(blen))
	}

	group := readStr(buff[:FDFS_GROUP_NAME_MAX_LEN])
	ipaddr := readStr(buff[FDFS_GROUP_NAME_MAX_LEN : FDFS_GROUP_NAME_MAX_LEN+FDFS_IPADDR_SIZE-1])
	port := buffToInt64(buff, FDFS_GROUP_NAME_MAX_LEN+FDFS_IPADDR_SIZE-1)

	addr := net.JoinHostPort(ipaddr, strconv.FormatInt(port, 10))
	p, err := c.getStoragePool(addr)
	if err != nil {
		return nil, err
	}
	return &Storage{
		addr:       net.JoinHostPort(ipaddr, strconv.FormatInt(port, 10)),
		groupName:  group,
		remoteName: rname,
		connPool:   p,
	}, nil
}
