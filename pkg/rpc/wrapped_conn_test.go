package rpc

import (
	"io"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
)

// createTestPeerMetrics creates a minimal peerMetrics for testing
func createTestPeerMetrics() *peerMetrics {
	tcpRTTMeta := metric.Metadata{
		Name:        "test.tcp.rtt",
		Help:        "TCP round trip time for testing",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	tcpRTTVarMeta := metric.Metadata{
		Name:        "test.tcp.rtt.var",
		Help:        "TCP round trip time variance for testing",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	tcpRTTAgg := aggmetric.NewGauge(tcpRTTMeta, "test")
	tcpRTTVarAgg := aggmetric.NewGauge(tcpRTTVarMeta, "test")

	return &peerMetrics{
		TCPRTT:    tcpRTTAgg.AddChild("test"),
		TCPRTTVar: tcpRTTVarAgg.AddChild("test"),
	}
}

// setupEchoServer creates a TCP echo server for benchmarking
func setupEchoServer(b *testing.B) (net.Listener, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}

	// Start echo server
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // echo back all data
			}(conn)
		}
	}()

	cleanup := func() {
		listener.Close()
	}

	return listener, cleanup
}

func BenchmarkConnWrapped(b *testing.B) {
	listener, cleanup := setupEchoServer(b)
	defer cleanup()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	pm := createTestPeerMetrics()
	wrappedConn := NewWrappedConn(conn, pm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := wrappedConn.Write([]byte("hello"))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnRegular(b *testing.B) {
	listener, cleanup := setupEchoServer(b)
	defer cleanup()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Write([]byte("hello"))
		if err != nil {
			b.Fatal(err)
		}
	}
}
