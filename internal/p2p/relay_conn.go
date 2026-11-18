package p2p

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type relayAddr string

func (a relayAddr) Network() string { return "gens-relay" }
func (a relayAddr) String() string  { return "relay://" + string(a) }

var punchMagic = []byte("GENSPUNCH")

var relayPktPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

type incomingRelayPkt struct {
	poolBuf *[]byte
	length  int
	addr    net.Addr
}

// relayPacketConn is a pure-virtual net.PacketConn multiplexing opaque blind-relay packets
// from relay-server's WebSocket so direct UDP QUIC traffic can run directly on *net.UDPConn
// with full kernel GSO/ECN/buffer optimizations.
type relayPacketConn struct {
	localAddr func() net.Addr
	sendRelay func(ctx context.Context, targetID string, pkt []byte) error

	rxCh  chan incomingRelayPkt
	done  chan struct{}
	once  sync.Once
	mu    sync.Mutex
	rDead time.Time
	wDead time.Time
}

func newRelayPacketConn(localAddr func() net.Addr, sendRelay func(ctx context.Context, targetID string, pkt []byte) error) *relayPacketConn {
	return &relayPacketConn{
		localAddr: localAddr,
		sendRelay: sendRelay,
		rxCh:      make(chan incomingRelayPkt, 2048),
		done:      make(chan struct{}),
	}
}

func (c *relayPacketConn) DeliverRelayPacket(senderID string, data []byte) {
	if len(data) == 0 {
		return
	}
	if len(data) > 4096 {
		pkt := make([]byte, len(data))
		copy(pkt, data)
		select {
		case <-c.done:
		case c.rxCh <- incomingRelayPkt{poolBuf: &pkt, length: len(data), addr: relayAddr(senderID)}:
		default:
		}
		return
	}
	bufPtr := relayPktPool.Get().(*[]byte)
	copy(*bufPtr, data)
	select {
	case <-c.done:
		relayPktPool.Put(bufPtr)
	case c.rxCh <- incomingRelayPkt{poolBuf: bufPtr, length: len(data), addr: relayAddr(senderID)}:
	default:
		// Drop under extreme backpressure; QUIC handles loss & congestion control
		relayPktPool.Put(bufPtr)
	}
}

func (c *relayPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case <-c.done:
		return 0, nil, net.ErrClosed
	default:
	}

	c.mu.Lock()
	dead := c.rDead
	c.mu.Unlock()

	if !dead.IsZero() {
		d := time.Until(dead)
		if d <= 0 {
			return 0, nil, osTimeoutErr{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-c.done:
			return 0, nil, net.ErrClosed
		case <-t.C:
			return 0, nil, osTimeoutErr{}
		case pkt := <-c.rxCh:
			return c.copyOut(p, pkt)
		}
	}

	select {
	case <-c.done:
		return 0, nil, net.ErrClosed
	case pkt := <-c.rxCh:
		return c.copyOut(p, pkt)
	}
}

func (c *relayPacketConn) copyOut(p []byte, pkt incomingRelayPkt) (int, net.Addr, error) {
	n := copy(p, (*pkt.poolBuf)[:pkt.length])
	if cap(*pkt.poolBuf) == 4096 {
		relayPktPool.Put(pkt.poolBuf)
	}
	if n < pkt.length {
		return n, pkt.addr, ioShortBufferErr{}
	}
	return n, pkt.addr, nil
}

func (c *relayPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	ra, ok := addr.(relayAddr)
	if !ok {
		return 0, errors.New("relayPacketConn only routes relayAddr")
	}
	if c.sendRelay == nil {
		return 0, errors.New("relay sender unavailable")
	}

	c.mu.Lock()
	wDead := c.wDead
	c.mu.Unlock()

	ctx := context.Background()
	var cancel context.CancelFunc
	if !wDead.IsZero() {
		d := time.Until(wDead)
		if d <= 0 {
			return 0, osTimeoutErr{}
		}
		ctx, cancel = context.WithTimeout(ctx, d)
	} else {
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	}
	defer cancel()

	if err := c.sendRelay(ctx, string(ra), p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *relayPacketConn) Close() error {
	c.once.Do(func() {
		close(c.done)
	})
	return nil
}

func (c *relayPacketConn) LocalAddr() net.Addr {
	if c.localAddr != nil {
		if a := c.localAddr(); a != nil {
			return a
		}
	}
	return relayAddr("local")
}

func (c *relayPacketConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	return c.SetWriteDeadline(t)
}

func (c *relayPacketConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rDead = t
	c.mu.Unlock()
	return nil
}

func (c *relayPacketConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.wDead = t
	c.mu.Unlock()
	return nil
}

type osTimeoutErr struct{}

func (osTimeoutErr) Error() string   { return "i/o timeout" }
func (osTimeoutErr) Timeout() bool   { return true }
func (osTimeoutErr) Temporary() bool { return true }

type ioShortBufferErr struct{}

func (ioShortBufferErr) Error() string   { return "short buffer" }
func (ioShortBufferErr) Timeout() bool   { return false }
func (ioShortBufferErr) Temporary() bool { return true }
