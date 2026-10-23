// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package clusterhook

import (
	"bufio"
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// endless writes x without end, and counts what was read of it.
type endless struct{ n int }

func (e *endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	e.n += len(p)
	return len(p), nil
}

// A head without a newline is refused at its bound, having read little
// more than it (#107).
func TestReadExportHeadIsBounded(t *testing.T) {
	src := &endless{}
	_, err := ReadExportHead(bufio.NewReaderSize(src, 4096), 1<<20)
	require.ErrorIs(t, err, ErrExportHead)
	assert.LessOrEqual(t, src.n, 1<<20+4096)

	line, err := ReadExportHead(bufio.NewReaderSize(strings.NewReader(strings.Repeat("y", 10000)+"\nrest"), 16), 10001)
	require.NoError(t, err)
	assert.Len(t, line, 10001)
	_, err = ReadExportHead(bufio.NewReaderSize(strings.NewReader(strings.Repeat("y", 10000)+"\n"), 16), 10000)
	require.ErrorIs(t, err, ErrExportHead)
	_, err = ReadExportHead(bufio.NewReader(strings.NewReader("no newline")), 100)
	require.ErrorIs(t, err, io.EOF)
}

// CallExport refuses a socket that answers a head without end.
func TestCallExportBoundsTheHead(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s")
	l, err := net.Listen("unix", sock)
	require.NoError(t, err)
	defer l.Close()
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, io.LimitReader(&endless{}, MaxExportHead+(1<<20)))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, _, err = CallExport(ctx, sock, &ExportRequest{Op: OpExportCheckpoints})
	require.ErrorIs(t, err, ErrExportHead)
}
