package pool

import (
	"net"
	"sync/atomic"
	"time"
)

type Conn struct {
	net.Conn
	usedAt atomic.Value
	Reader *Reader
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		Conn:   netConn,
		Reader: NewReader(netConn),
	}
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(cn.UsedAt()) > timeout
}

func (cn *Conn) UsedAt() time.Time {
	return cn.usedAt.Load().(time.Time)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	cn.usedAt.Store(tm)
}
