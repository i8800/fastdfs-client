package go_fastdfs

import (
	"github.com/monkey92t/go_fastdfs/pool"
)

//从一个pool中获取一个连接池的链接
//并检测可用性
//如果是连接池中不可用的conn，或者不是纯洁的conn，则会从连接池中删除它
func getConn(p *pool.ConnPool) (*pool.Conn, error) {
	for {
		conn, isnew, err := p.Get()
		if err != nil {
			return nil, err
		}
		if isnew {
			return conn, nil
		}

		if checkConnPure(conn) {
			return conn, nil
		}

		p.Remove(conn)
	}
}

//把一个连接放入指定连接池，放入之前检测是否是干净的连接
//如果是不干净的，则会关闭它
func closeConn(p *pool.ConnPool, c *pool.Conn) {
	if c == nil {
		return
	}
	if checkConnPure(c) {
		p.Put(c)
	} else {
		p.Remove(c)
	}
}

//检测一个连接的是否是可用而且干净的
//true 可用而且干净的
//false 不可用或者不干净的
func checkConnPure(conn *pool.Conn) bool {
	th := buildTrackerHeader(FDFS_PROTO_CMD_ACTIVE_TEST, 0)
	if err := th.sendHeader(conn); err != nil {
		return false
	}
	if err := th.recvHeader(conn, TRACKER_PROTO_CMD_RESP, -1); err != nil {
		return false
	}

	return true
}
