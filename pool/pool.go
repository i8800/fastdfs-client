package pool

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var ErrClosed = errors.New("fastdfs: client is closed")
var ErrPoolTimeout = errors.New("fastdfs: connection pool timeout")

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

type Stats struct {
	Hits     uint32 //命中连接池中连接次数
	Misses   uint32 //没有命中连接池连接次数
	Timeouts uint32 //等待连接池时超时次数

	PoolConns  uint32 //连接池中连接数量
	StaleConns uint32 //删除连接池中过期连接的次数
}

type Pooler interface {
	NewConn() (*Conn, error)
	CloseConn(*Conn) error

	Get() (*Conn, bool, error)
	Put(*Conn) error
	Remove(*Conn) error

	Len() int
	Stats() *Stats

	Close() error
}

type Options struct {
	Dialer func() (net.Conn, error)

	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type ConnPool struct {
	opt *Options

	dialErrorsNum uint32 //atomic

	lastDialError   error
	lastDialErrorMu sync.RWMutex

	queue chan struct{}

	connsMu sync.Mutex
	conns   []*Conn

	stats Stats

	_closed uint32 // atomic
}

var _ Pooler = (*ConnPool)(nil)

func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		opt: opt,

		queue: make(chan struct{}, opt.PoolSize),
		conns: make([]*Conn, 0, opt.PoolSize),
	}
	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}
	return p
}

//创建一个新的连接
func (p *ConnPool) NewConn() (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	//当前的失败数量过多
	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.opt.Dialer()
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn)
	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.connsMu.Unlock()

	return cn, nil
}

//连接丢失，心跳检测连接
func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer()
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

//获取最后一个dial错误
func (p *ConnPool) getLastDialError() error {
	p.lastDialErrorMu.RLock()
	err := p.lastDialError
	p.lastDialErrorMu.RUnlock()
	return err
}

//设置最后一个dial错误
func (p *ConnPool) setLastDialError(err error) {
	p.lastDialErrorMu.Lock()
	p.lastDialError = err
	p.lastDialErrorMu.Unlock()
}

// 获取一个连接，如果未命中连接池连接，会在连接池未满的情况下则创建一个返回
func (p *ConnPool) Get() (*Conn, bool, error) {
	if p.closed() {
		return nil, false, ErrClosed
	}

	select {
	case p.queue <- struct{}{}:
	default:
		timer := timers.Get().(*time.Timer)
		timer.Reset(p.opt.PoolTimeout)

		select {
		case p.queue <- struct{}{}:
			if !timer.Stop() {
				<-timer.C
			}
			timers.Put(timer)
		case <-timer.C:
			timers.Put(timer)
			atomic.AddUint32(&p.stats.Timeouts, 1)
			return nil, false, ErrPoolTimeout
		}
	}

	for {
		p.connsMu.Lock()
		cn := p.popDial()
		p.connsMu.Unlock()

		if cn == nil {
			break
		}

		//如果是已经过期的连接，则关闭
		if cn.IsStale(p.opt.IdleTimeout) {
			p.closeConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, false, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	newcn, err := p.NewConn()
	if err != nil {
		<-p.queue
		return nil, false, err
	}

	return newcn, true, nil
}

//弹出一个连接池的连接
//如连接池没有可用的，返回nil
func (p *ConnPool) popDial() *Conn {
	if len(p.conns) == 0 {
		return nil
	}

	idx := len(p.conns) - 1
	cn := p.conns[idx]
	p.conns = p.conns[:idx]
	return cn
}

//放入连接
func (p *ConnPool) Put(cn *Conn) error {
	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.connsMu.Unlock()
	<-p.queue
	return nil
}

//移除一个连接
func (p *ConnPool) Remove(cn *Conn) error {
	_ = p.CloseConn(cn)
	<-p.queue
	return nil
}

//返回当前长度
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	l := len(p.conns)
	p.connsMu.Unlock()
	return l
}

//获取当前的统计值
func (p *ConnPool) Stats() *Stats {
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		PoolConns:  uint32(p.Len()),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

//对指定的conn进行关闭
//并从连接池中移除，如果连接池中并无连接，则只关闭,不会产生错误
func (p *ConnPool) CloseConn(cn *Conn) error {
	p.connsMu.Lock()
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	p.connsMu.Unlock()

	return p.closeConn(cn)
}

//对指定的conn关闭
func (p *ConnPool) closeConn(cn *Conn) error {
	return cn.Close()
}

//关闭连接池
func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.connsMu.Unlock()

	return firstErr
}

//调用方mu.Lock
func (p *ConnPool) reapStaleConn() bool {
	if len(p.conns) == 0 {
		return false
	}

	cn := p.conns[0]
	if !cn.IsStale(p.opt.IdleTimeout) {
		return false
	}

	p.closeConn(cn)
	p.conns = append(p.conns[:0], p.conns[1:]...)

	return true
}

func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		p.queue <- struct{}{}
		p.connsMu.Lock()

		reaped := p.reapStaleConn()

		p.connsMu.Unlock()
		<-p.queue

		if reaped {
			n++
		} else {
			break
		}
	}
	return n, nil
}

func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for range ticker.C {
		if p.closed() {
			break
		}
		n, err := p.ReapStaleConns()
		if err != nil {
			continue
		}
		atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	}
}
