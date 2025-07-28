// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type RTTInfo struct {
	RTT    time.Duration
	RTTVar time.Duration
}

func getRTTInfo(fd uintptr) (*RTTInfo, error) {
	info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	if err != nil {
		return nil, err
	}
	rttInfo := &RTTInfo{
		RTT:    time.Duration(info.Rtt) * time.Microsecond,
		RTTVar: time.Duration(info.Rttvar) * time.Microsecond,
	}
	return rttInfo, nil
}

type wrappedConn struct {
	net.Conn
	pm     *peerMetrics
	fd     uintptr
	ticker *time.Ticker
}

var _ net.Conn = (*wrappedConn)(nil)

func NewWrappedConn(conn net.Conn, pm *peerMetrics) net.Conn {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return conn
	}

	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return conn
	}

	var fd uintptr
	err = rawConn.Control(func(i uintptr) {
		fd = i
	})
	if err != nil {
		return conn
	}

	return &wrappedConn{
		Conn:   conn,
		pm:     pm,
		fd:     fd,
		ticker: time.NewTicker(3 * time.Second),
	}
}

func (w *wrappedConn) updateRTTInfo() {
	rttInfo, err := getRTTInfo(w.fd)
	if err != nil {
		return
	}
	w.pm.TCPRTT.Update(rttInfo.RTT.Nanoseconds())
	w.pm.TCPRTTVar.Update(rttInfo.RTTVar.Nanoseconds())
}

func (w *wrappedConn) Write(p []byte) (n int, err error) {
	// select {
	// case <-w.ticker.C:
	w.updateRTTInfo()
	// default:
	// }
	return w.Conn.Write(p)
}
