package minecraft

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

func TestConnBufferedSendLimits(t *testing.T) {
	conn := newBufferedTestConn(t)
	conn.SetBufferedSendLimits(2, 5)

	first := []byte{1, 2, 3}
	if n, err := conn.Write(first); err != nil || n != len(first) {
		t.Fatalf("first Write() = (%d, %v), want (%d, nil)", n, err, len(first))
	}
	first[0] = 9
	if got := conn.bufferedSend[0][0]; got != 1 {
		t.Fatalf("Write retained caller-owned memory: first byte = %d, want 1", got)
	}
	if n, err := conn.Write([]byte{4, 5}); err != nil || n != 2 {
		t.Fatalf("second Write() = (%d, %v), want (2, nil)", n, err)
	}

	if n, err := conn.Write([]byte{6}); n != 0 || !errors.Is(err, ErrBufferedSendLimit) {
		t.Fatalf("packet-limit Write() = (%d, %v), want (0, ErrBufferedSendLimit)", n, err)
	}
	if len(conn.bufferedSend) != 2 || conn.bufferedSendBytes != 5 {
		t.Fatalf("rejected write changed queue: packets=%d bytes=%d", len(conn.bufferedSend), conn.bufferedSendBytes)
	}
}

func TestConnBufferedSendByteLimitRejectsSingleOversizedWrite(t *testing.T) {
	conn := newBufferedTestConn(t)
	conn.SetBufferedSendLimits(10, 4)

	if n, err := conn.Write(make([]byte, 5)); n != 0 || !errors.Is(err, ErrBufferedSendLimit) {
		t.Fatalf("Write() = (%d, %v), want (0, ErrBufferedSendLimit)", n, err)
	}
	if len(conn.bufferedSend) != 0 || conn.bufferedSendBytes != 0 {
		t.Fatalf("rejected write changed queue: packets=%d bytes=%d", len(conn.bufferedSend), conn.bufferedSendBytes)
	}
}

func TestConnBufferedSendByteLimitBoundsPacketMarshalling(t *testing.T) {
	conn := newBufferedPacketTestConn(t)
	conn.SetBufferedSendLimits(10, 32)

	err := conn.WritePacket(&packet.Unknown{PacketID: 200, Payload: make([]byte, 1<<20)})
	if !errors.Is(err, ErrBufferedSendLimit) {
		t.Fatalf("WritePacket() error = %v, want ErrBufferedSendLimit", err)
	}
	if len(conn.bufferedSend) != 0 || conn.bufferedSendBytes != 0 {
		t.Fatalf("rejected packet changed queue: packets=%d bytes=%d", len(conn.bufferedSend), conn.bufferedSendBytes)
	}
}

func TestConnBufferedSendByteLimitBoundsSinglePacketMarshalling(t *testing.T) {
	conn := newBufferedPacketTestConn(t)
	conn.SetBufferedSendLimits(10, 32)
	pk := &packet.Unknown{PacketID: 200, Payload: make([]byte, 1<<20)}

	if err := conn.FlushSingleWithACK(pk, 1); !errors.Is(err, ErrBufferedSendLimit) {
		t.Fatalf("FlushSingleWithACK() error = %v, want ErrBufferedSendLimit", err)
	}
	if err := conn.FlushSingleWithReliability(pk, 2); !errors.Is(err, ErrBufferedSendLimit) {
		t.Fatalf("FlushSingleWithReliability() error = %v, want ErrBufferedSendLimit", err)
	}
	if conn.bufferedSendInFlight != 0 || conn.bufferedSendInFlightBytes != 0 {
		t.Fatalf("rejected single packet changed in-flight usage: packets=%d bytes=%d", conn.bufferedSendInFlight, conn.bufferedSendInFlightBytes)
	}
}

func TestConnBufferedSendLimitsIncludeInFlightBatch(t *testing.T) {
	conn := newBufferedTestConn(t)
	conn.bufferedSendInFlight = 1
	conn.bufferedSendInFlightBytes = 4
	conn.SetBufferedSendLimits(2, 5)

	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("Write at exact limits: %v", err)
	}
	if _, err := conn.Write([]byte{2}); !errors.Is(err, ErrBufferedSendLimit) {
		t.Fatalf("Write over in-flight limits = %v, want ErrBufferedSendLimit", err)
	}
}

func TestConnAutomaticFlushControl(t *testing.T) {
	conn := &Conn{}
	conn.SetAutomaticFlushEnabled(false)
	if !conn.automaticFlushDisabled.Load() {
		t.Fatal("SetAutomaticFlushEnabled(false) did not disable the routine")
	}
	conn.SetAutomaticFlushEnabled(true)
	if conn.automaticFlushDisabled.Load() {
		t.Fatal("SetAutomaticFlushEnabled(true) did not enable the routine")
	}
}

func TestConnDisabledAutomaticFlushRetainsPackets(t *testing.T) {
	local, remote := net.Pipe()
	conn := newConn(local, nil, slog.Default(), proto{}, time.Millisecond, false)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = remote.Close()
	})
	conn.SetAutomaticFlushEnabled(false)
	if _, err := conn.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	if err := remote.SetReadDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Read(make([]byte, 32)); err == nil {
		t.Fatal("disabled automatic flush released a buffered packet")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("disabled automatic flush read error = %v, want timeout", err)
	}

	conn.SetAutomaticFlushEnabled(true)
	if err := remote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := remote.Read(make([]byte, 32)); err != nil || n == 0 {
		t.Fatalf("re-enabled automatic flush read = (%d, %v), want data", n, err)
	}
}

func newBufferedTestConn(t *testing.T) *Conn {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	return &Conn{ctx: context.Background(), conn: local}
}

func newBufferedPacketTestConn(t *testing.T) *Conn {
	t.Helper()
	conn := newBufferedTestConn(t)
	conn.proto = proto{}
	conn.hdr = &packet.Header{}
	return conn
}
