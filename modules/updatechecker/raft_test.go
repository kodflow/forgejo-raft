// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package updatechecker

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/system"
	"forgejo.org/modules/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestParseRaftVersion(t *testing.T) {
	for _, c := range []struct {
		in, upstream string
		n            int
	}{
		{"17.0.1", "17.0.1", 0},
		{"v17.0.1-raft.2", "17.0.1", 2},
		{"17.0.1-raft.12+gitea-1.22.0", "17.0.1", 12},
		{"18.0.0-dev-raft.5+gitea-1.22.0", "18.0.0-dev", 5},
		{"18.0.0-dev", "18.0.0-dev", 0},
		{"0.0.0-e2e+gitea-1.22.0", "0.0.0-e2e", 0},
	} {
		v, err := ParseRaftVersion(c.in)
		require.NoError(t, err, c.in)
		assert.Equal(t, c.upstream, v.Upstream.String(), c.in)
		assert.Equal(t, c.n, v.N, c.in)
	}

	for _, bad := range []string{"", "not a version", "17.0.1-raft.0", "17.0.1-raft.99999999999999999999"} {
		_, err := ParseRaftVersion(bad)
		assert.Error(t, err, bad)
	}
}

func TestRaftVersionOrder(t *testing.T) {
	// Each is strictly greater than the one before.
	ordered := []string{
		"16.0.6-raft.3",
		"17.0.0",
		"17.0.0-raft.1",
		"17.0.0-raft.2",
		"17.0.0-raft.10",
		"17.0.1",
		"17.0.1-raft.1+gitea-1.22.0",
		"18.0.0-dev",
		"18.0.0-dev-raft.4",
		"18.0.0-dev-raft.5",
		"18.0.0",
		"18.0.0-raft.1",
	}
	for i := 1; i < len(ordered); i++ {
		a, err := ParseRaftVersion(ordered[i-1])
		require.NoError(t, err)
		b, err := ParseRaftVersion(ordered[i])
		require.NoError(t, err)
		assert.Equal(t, -1, a.Compare(b), "%s < %s", ordered[i-1], ordered[i])
		assert.Equal(t, 1, b.Compare(a), "%s > %s", ordered[i], ordered[i-1])
	}
	a, _ := ParseRaftVersion("17.0.1-raft.1")
	b, _ := ParseRaftVersion("v17.0.1-raft.1+gitea-1.22.0")
	assert.Equal(t, 0, a.Compare(b))
}

// A stable raft build is current, not out of date, when the official
// release it is built on is the latest one: the raft suffix is not read as a
// semver prerelease.
func TestGetReleaseStateRaftBuild(t *testing.T) {
	for _, c := range []struct {
		running, want string
		state         MajorReleaseState
		minor         MinorReleaseState
	}{
		{"17.0.1-raft.2+gitea-1.22.0", "", MajorReleaseSupported, MinorReleaseCurrent},
		{"17.0.0-raft.3+gitea-1.22.0", "17.0.1", MajorReleaseSupported, MinorReleaseOutOfDate},
		{"18.0.0-dev-raft.5+gitea-1.22.0", "", MajorReleasePrerelease, MinorReleaseCurrent},
	} {
		state := system.NewMockStateStore(t)
		state.On("Get", mock.Anything, mock.MatchedBy(func(item system.StateItem) bool {
			s, ok := item.(*CheckerState)
			if ok {
				s.SupportedVersions = []string{"16.0.6", "17.0.1"}
			}
			return ok
		})).Return(nil)
		restoreState := test.MockVariableValue[system.StateStore](&system.AppState, state)
		restoreVer := test.MockVariableValue(&setting.AppVer, c.running)
		got, err := GetReleaseState(t.Context())
		restoreVer()
		restoreState()
		require.NoError(t, err, c.running)
		assert.Equal(t, c.state, got.MajorReleaseState, c.running)
		assert.Equal(t, c.minor, got.MinorReleaseState, c.running)
		assert.Equal(t, c.want, got.RecommendedUpgrade.ValueOrZeroValue(), c.running)
	}
}

var testRaftReleases = []RaftRelease{
	{Version: "16.0.6-raft.1", Upstream: "16.0.6", URL: "https://example.invalid/v16.0.6-raft.1"},
	{Version: "17.0.0-raft.1", Upstream: "17.0.0", URL: "https://example.invalid/v17.0.0-raft.1"},
	{Version: "17.0.0-raft.2", Upstream: "17.0.0", URL: "https://example.invalid/v17.0.0-raft.2"},
	{Version: "18.0.0-dev-raft.5", Upstream: "18.0.0-dev", URL: "https://example.invalid/v18.0.0-dev-raft.5", Prerelease: true},
	{Version: "18.0.0-dev-raft.6", Upstream: "18.0.0-dev", URL: "https://example.invalid/v18.0.0-dev-raft.6", Prerelease: true},
}

func TestComputeRaftNotice(t *testing.T) {
	for _, c := range []struct {
		name, running, official string
		state                   RaftNoticeState
		release                 string
	}{
		{"up to date", "17.0.0-raft.2+gitea-1.22.0", "", RaftNoticeNone, ""},
		{"newer raft build of the same official release", "17.0.0-raft.1+gitea-1.22.0", "", RaftNoticeAvailable, "17.0.0-raft.2"},
		{"official out, raft build published", "16.0.5-raft.1", "16.0.6", RaftNoticeAvailable, "16.0.6-raft.1"},
		{"official out, raft build not yet", "17.0.0-raft.2", "17.0.1", RaftNoticePending, ""},
		{"plain Forgejo, raft build published", "17.0.0", "", RaftNoticeAvailable, "17.0.0-raft.2"},
		{"major unsupported, raft of the recommended LTS", "13.0.0-raft.1", "16.0.6", RaftNoticeAvailable, "16.0.6-raft.1"},
		{"development line, newer prerelease", "18.0.0-dev-raft.5+gitea-1.22.0", "", RaftNoticeAvailable, "18.0.0-dev-raft.6"},
		{"development line, latest prerelease", "18.0.0-dev-raft.6+gitea-1.22.0", "", RaftNoticeNone, ""},
		{"no raft build at all for this line", "15.0.10-raft.1", "", RaftNoticeNone, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, err := computeRaftNotice(c.running, c.official, testRaftReleases)
			require.NoError(t, err)
			assert.Equal(t, c.state, n.State)
			assert.Equal(t, c.release, n.Release.Version)
			assert.Equal(t, c.official, n.Official)
			if c.state == RaftNoticeAvailable {
				assert.NotEmpty(t, n.Release.URL)
			}
		})
	}

	_, err := computeRaftNotice("not a version", "", testRaftReleases)
	require.Error(t, err)
	_, err = computeRaftNotice("17.0.0", "garbage", testRaftReleases)
	require.Error(t, err)
}

// A stable line is never offered a prerelease, even one built on it.
func TestComputeRaftNoticeStableIgnoresPrerelease(t *testing.T) {
	n, err := computeRaftNotice("17.0.0-raft.1", "", []RaftRelease{
		{Version: "17.0.0-raft.2", Upstream: "17.0.0", Prerelease: true},
	})
	require.NoError(t, err)
	assert.Equal(t, RaftNoticeNone, n.State)
}

func TestParseRaftIndex(t *testing.T) {
	got, err := parseRaftIndex([]byte(`{"schema":1,"releases":[
		{"version":"17.0.0-raft.1","upstream":"17.0.0","url":"u","prerelease":false},
		{"version":"what","upstream":"?"}
	]}`))
	require.NoError(t, err)
	assert.Equal(t, []RaftRelease{{Version: "17.0.0-raft.1", Upstream: "17.0.0", URL: "u"}}, got)

	_, err = parseRaftIndex([]byte(`not json`))
	require.Error(t, err)
}

func TestRaftUpdateChecker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/releases.json" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"releases":[{"version":"17.0.0-raft.1","upstream":"17.0.0","url":"u"}]}`))
	}))
	defer srv.Close()

	expect := func(t *testing.T, check func(*RaftCheckerState) bool) {
		state := system.NewMockStateStore(t)
		state.On("Set", mock.Anything, mock.MatchedBy(func(item system.StateItem) bool {
			s, ok := item.(*RaftCheckerState)
			return ok && s.Name() == "update-checker-raft" && check(s)
		})).Return(nil)
		t.Cleanup(test.MockVariableValue[system.StateStore](&system.AppState, state))
	}

	t.Run("success", func(t *testing.T) {
		expect(t, func(s *RaftCheckerState) bool {
			return s.Error == "" && len(s.Releases) == 1 && s.Releases[0].Version == "17.0.0-raft.1"
		})
		require.NoError(t, RaftUpdateChecker(t.Context(), srv.URL+"/releases.json"))
	})

	t.Run("HTTP error is stored, not returned", func(t *testing.T) {
		expect(t, func(s *RaftCheckerState) bool {
			return s.Error == "forgejo-raft release index: HTTP 404" && s.Releases == nil
		})
		require.NoError(t, RaftUpdateChecker(t.Context(), srv.URL+"/missing.json"))
	})

	t.Run("disabled", func(t *testing.T) {
		expect(t, func(s *RaftCheckerState) bool { return s.Error == "" && s.Releases == nil })
		require.NoError(t, RaftUpdateChecker(t.Context(), ""))
	})
}

func TestGetRaftNotice(t *testing.T) {
	get := func(t *testing.T, fill func(*RaftCheckerState)) {
		state := system.NewMockStateStore(t)
		state.On("Get", mock.Anything, mock.MatchedBy(func(item system.StateItem) bool {
			s, ok := item.(*RaftCheckerState)
			if ok {
				fill(s)
			}
			return ok
		})).Return(nil)
		t.Cleanup(test.MockVariableValue[system.StateStore](&system.AppState, state))
		t.Cleanup(test.MockVariableValue(&setting.AppVer, "17.0.0-raft.1+gitea-1.22.0"))
	}

	t.Run("never checked", func(t *testing.T) {
		get(t, func(*RaftCheckerState) {})
		n, err := GetRaftNotice(t.Context(), "17.0.1")
		require.NoError(t, err)
		assert.Equal(t, RaftNoticeNone, n.State)
	})

	t.Run("stored error", func(t *testing.T) {
		get(t, func(s *RaftCheckerState) { s.Error = "HTTP 500" })
		_, err := GetRaftNotice(t.Context(), "")
		require.EqualError(t, err, "HTTP 500")
	})

	t.Run("pending", func(t *testing.T) {
		get(t, func(s *RaftCheckerState) { s.Releases = testRaftReleases })
		n, err := GetRaftNotice(t.Context(), "17.0.1")
		require.NoError(t, err)
		assert.True(t, n.Pending())
		assert.False(t, n.Available())
	})

	t.Run("available", func(t *testing.T) {
		get(t, func(s *RaftCheckerState) { s.Releases = testRaftReleases })
		n, err := GetRaftNotice(t.Context(), "")
		require.NoError(t, err)
		assert.True(t, n.Available())
		assert.Equal(t, "17.0.0-raft.2", n.Release.Version)
	})
}
