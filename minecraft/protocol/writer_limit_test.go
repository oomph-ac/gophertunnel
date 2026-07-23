package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestBoundedWriterRejectsBeforeGrowingBuffer(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	writer := NewBoundedWriter(buf, 4)

	if n, err := writer.Write([]byte{1, 2, 3, 4, 5}); n != 0 || !errors.Is(err, ErrWriterLimit) {
		t.Fatalf("Write() = (%d, %v), want (0, ErrWriterLimit)", n, err)
	}
	if buf.Len() != 0 || buf.Cap() != 8 {
		t.Fatalf("rejected write changed buffer: len=%d cap=%d", buf.Len(), buf.Cap())
	}
}

func TestWriterExposesBoundedWriterError(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	bounded := NewBoundedWriter(buf, 2)
	writer := NewWriter(bounded, 0)
	value := uint32(1 << 21)

	writer.Varuint32(&value)
	if !errors.Is(writer.Err(), ErrWriterLimit) {
		t.Fatalf("Writer.Err() = %v, want ErrWriterLimit", writer.Err())
	}
	if buf.Len() != 2 {
		t.Fatalf("buffer len = %d, want 2", buf.Len())
	}
}
