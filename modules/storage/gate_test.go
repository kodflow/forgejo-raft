// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"forgejo.org/modules/setting"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A covered storage deletes through the registered gate, which decides
// when, and whether, the file goes; without a gate it deletes at once.
func TestDeleteGoesThroughTheGate(t *testing.T) {
	root := t.TempDir()
	local, err := NewLocalStorage(t.Context(), &setting.Storage{Path: root})
	require.NoError(t, err)
	s := WithDeleteGate(local)
	for _, name := range []string{"held", "free", "refused"} {
		_, err := s.Save(name, strings.NewReader(name), int64(len(name)))
		require.NoError(t, err)
	}
	exists := func(name string) bool {
		_, err := os.Stat(filepath.Join(root, name))
		return err == nil
	}

	var gated []bool
	SetDeleteGate(func(del func() error) error {
		gated = append(gated, exists("held"))
		return del()
	})
	t.Cleanup(func() { SetDeleteGate(nil) })
	require.NoError(t, s.Delete("held"))
	assert.Equal(t, []bool{true}, gated, "the gate runs before the file goes")
	assert.False(t, exists("held"))

	refusal := errors.New("the checkpoint still lists the files")
	SetDeleteGate(func(func() error) error { return refusal })
	require.ErrorIs(t, s.Delete("refused"), refusal)
	assert.True(t, exists("refused"), "a refused deletion keeps the file")

	SetDeleteGate(nil)
	require.NoError(t, s.Delete("free"))
	assert.False(t, exists("free"))
}
