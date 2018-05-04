package go_fastdfs

import (
	"github.com/monkey92t/go_fastdfs/pool"
	"net"
	"runtime"
	"sync"
	"time"
)

type Options struct {
	Addr string //ip:port

	Dialer      func() (net.Conn, error)
	DialTimeout time.Duration

	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type FastdfsClient struct {
	storePools map[string]*pool.ConnPool
	connPool   *pool.ConnPool
	opt        *Options
	mu         sync.Mutex
}

func NewClient(opt *Options) *FastdfsClient {
	optionsInit(opt)

	c := &FastdfsClient{
		opt: opt,
	}
	poolOptions := c.getPoolOpt()
	c.connPool = pool.NewConnPool(poolOptions)
	storepool := make(map[string]*pool.ConnPool)
	c.storePools = storepool

	return c
}

func (c *FastdfsClient) getPoolOpt() *pool.Options {
	return &pool.Options{
		Dialer:             c.opt.Dialer,
		PoolSize:           c.opt.PoolSize,
		PoolTimeout:        c.opt.PoolTimeout,
		IdleTimeout:        c.opt.IdleTimeout,
		IdleCheckFrequency: c.opt.IdleCheckFrequency,
	}
}

func optionsInit(opt *Options) {
	if opt.PoolSize == 0 {
		opt.PoolSize = 10 * runtime.NumCPU()
	}

	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}

	if opt.PoolTimeout == 0 {
		opt.PoolTimeout = opt.DialTimeout
	}

	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = 5 * time.Minute
	}

	if opt.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = time.Minute
	}

	if opt.Addr == "" {
		opt.Addr = ":22122"
	}

	if opt.Dialer == nil {
		opt.Dialer = defaultDialer(opt.Addr, opt.IdleTimeout)
	}
}

func defaultDialer(addr string, dialTimeout time.Duration) func() (net.Conn, error) {
	if dialTimeout <= 0 {
		dialTimeout = 60 * time.Second
	}
	return func() (net.Conn, error) {
		return net.DialTimeout("tcp", addr, dialTimeout)
	}
}
