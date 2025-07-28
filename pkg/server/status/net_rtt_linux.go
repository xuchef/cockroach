//go:build linux

package status

import (
	stdnet "net"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/shirou/gopsutil/v3/net"
	"golang.org/x/sys/unix"
)

// RTTInfo holds the round-trip time information for a connection.
type RTTInfo struct {
	RTT    time.Duration
	RTTVar time.Duration
}

// getRTTInfo retrieves TCP round-trip time information for a given connection
// using a getsockopt syscall. This is only supported on Linux.
func getRTTInfo(conn net.ConnectionStat) (*RTTInfo, error) {
	// The file descriptor for the socket.
	fd := int(conn.Fd)

	// Retrieve TCP info for the socket.
	info, err := unix.GetsockoptTCPInfo(fd, unix.IPPROTO_TCP, unix.TCP_INFO)
	if err != nil {
		return nil, err
	}

	rttInfo := &RTTInfo{
		// RTT and RTTVar are in microseconds.
		RTT:    time.Duration(info.Rtt) * time.Microsecond,
		RTTVar: time.Duration(info.Rttvar) * time.Microsecond,
	}

	return rttInfo, nil
}

func getRTTInfo2(conn stdnet.Conn) (*RTTInfo, error) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil, errors.New("connection is not a syscall.Conn")
	}

	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return nil, err
	}

	var info *unix.TCPInfo
	var syscallErr error

	err = rawConn.Control(func(fd uintptr) {
		info, syscallErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	})
	if err != nil {
		return nil, err
	}
	if syscallErr != nil {
		return nil, syscallErr
	}

	rttInfo := &RTTInfo{
		RTT:    time.Duration(info.Rtt) * time.Microsecond,
		RTTVar: time.Duration(info.Rttvar) * time.Microsecond,
	}

	return rttInfo, nil
}
