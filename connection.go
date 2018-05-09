package go_fastdfs

import "github.com/monkey92t/go_fastdfs/pool"

//从一个pool中获取一个连接池的链接
//并检测可用性
func getConn(p *pool.ConnPool) (*pool.Conn, error) {
	conn, isnew, err := p.Get()
	if err != nil {
		return nil, err
	}
	if isnew {
		return conn, nil
	}

	th := buildTrackerHeader(FDFS_PROTO_CMD_ACTIVE_TEST, 0)
	if err := th.sendHeader(conn); err != nil {
		return nil, err
	}
	if err := th.recvHeader(conn, TRACKER_PROTO_CMD_RESP, -1); err != nil {
		return nil, err
	}
	return conn, nil
}
