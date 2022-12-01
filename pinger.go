package graphqlws

import (
	"context"
	"sync"
	"time"
)

type Pinger struct {
	mx        sync.Mutex
	ctx       context.Context
	interval  time.Duration
	timeout   time.Duration
	ping      PingFn
	onTimeout OnTimeoutFn
	logger    Logger

	pong context.CancelFunc
}

type PingFn func() error

type OnTimeoutFn func()

func NewPinger(ctx context.Context, cancel context.CancelFunc, interval time.Duration, timeout time.Duration, logger Logger, ping PingFn) *Pinger {
	p := &Pinger{
		mx:        sync.Mutex{},
		ctx:       ctx,
		interval:  interval,
		timeout:   timeout,
		ping:      ping,
		onTimeout: OnTimeoutFn(cancel),
		logger:    logger,
	}

	go p.run()

	return p
}

func (p *Pinger) run() {
	for {
		if err := waitForInterval(p.ctx, p.interval); err != nil {
			return
		}

		if err := sendPingAndAwaitPong(p); err != nil {
			return
		}
	}
}

func (p *Pinger) OnPong() {
	p.mx.Lock()
	p.pong()
	p.mx.Unlock()
}

func waitForInterval(ctx context.Context, interval time.Duration) error {
	intervalCtx, intervalCancel := context.WithTimeout(context.Background(), interval)

	select {
	case <-intervalCtx.Done():
		intervalCancel()
		return nil
	case <-ctx.Done():
		intervalCancel()
		return ctx.Err()
	}
}

func sendPingAndAwaitPong(p *Pinger) error {
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), p.timeout)
	defer timeoutCancel()

	pongCtx, pongCancel := context.WithCancel(context.Background())
	defer pongCancel()

	p.mx.Lock()
	p.pong = pongCancel
	p.mx.Unlock()

	go func(p *Pinger) {
		if err := p.ping(); err != nil {
			p.logger.Error(err)
		}
	}(p)

	select {
	case <-pongCtx.Done():
		return nil
	case <-timeoutCtx.Done():
		p.onTimeout()
		return timeoutCtx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}
