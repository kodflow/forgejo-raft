// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package updatechecker

// forgejo-raft builds are versioned <upstream>-raft.<N>, e.g. 17.0.1-raft.2
// (stable upstream) or 18.0.0-dev-raft.5 (development upstream), plus the
// +gitea-<compat> metadata of every Forgejo build. Read as semver, a raft
// suffix is a prerelease and would sort 17.0.1-raft.1 *before* 17.0.1; the
// upstream checker would then call every raft build out of date. So the
// upstream part is compared as Forgejo compares it, and N only breaks ties:
// 17.0.1-raft.2 > 17.0.1-raft.1 > 17.0.1.
//
// The raft releases are listed by a static JSON file the release workflow of
// kodflow/forgejo-raft publishes (no GitHub API call, so no rate limit), at
// the URL set by [cron.update_checker] RAFT_ENDPOINT.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"forgejo.org/modules/json"
	"forgejo.org/modules/proxy"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/system"

	"github.com/hashicorp/go-version"
)

const (
	// RaftReleasesEndpoint is the default index of forgejo-raft releases.
	RaftReleasesEndpoint = "https://raw.githubusercontent.com/kodflow/forgejo-raft/release-index/releases.json"
	// RaftReleasesPage lists every forgejo-raft release.
	RaftReleasesPage = "https://github.com/kodflow/forgejo-raft/releases"
)

var raftSuffix = regexp.MustCompile(`-raft\.([0-9]+)$`)

// RaftVersion is a version split into the upstream Forgejo version it is
// built on and its raft build number (0 for a plain Forgejo version).
type RaftVersion struct {
	Upstream *version.Version
	N        int
}

// ParseRaftVersion reads "17.0.1-raft.2+gitea-1.22.0", "v18.0.0-dev-raft.5"
// or a plain "17.0.1".
func ParseRaftVersion(s string) (*RaftVersion, error) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	if i := strings.IndexByte(s, '+'); i >= 0 {
		s = s[:i]
	}
	n := 0
	if m := raftSuffix.FindStringSubmatchIndex(s); m != nil {
		var err error
		if n, err = strconv.Atoi(s[m[2]:m[3]]); err != nil || n < 1 {
			return nil, fmt.Errorf("malformed raft build number in %q", s)
		}
		s = s[:m[0]]
	}
	up, err := version.NewVersion(s)
	if err != nil {
		return nil, err
	}
	return &RaftVersion{Upstream: up, N: n}, nil
}

// Compare returns -1, 0 or 1: the upstream versions first, then N.
func (v *RaftVersion) Compare(o *RaftVersion) int {
	if c := v.Upstream.Compare(o.Upstream); c != 0 {
		return c
	}
	return cmp.Compare(v.N, o.N)
}

// currentUpstreamVersion is the upstream Forgejo version this build is based
// on, which is what the official releases are compared with.
func currentUpstreamVersion(appVer string) (*version.Version, error) {
	v, err := ParseRaftVersion(appVer)
	if err != nil {
		return nil, err
	}
	return v.Upstream, nil
}

// RaftRelease is one entry of the release index.
type RaftRelease struct {
	Version    string `json:"version"`  // 17.0.1-raft.1
	Upstream   string `json:"upstream"` // 17.0.1
	URL        string `json:"url"`
	Prerelease bool   `json:"prerelease"`
}

// RaftCheckerState stores what the last check of the index found.
type RaftCheckerState struct {
	Releases []RaftRelease
	Error    string
}

// Name returns the name of the state item
func (r *RaftCheckerState) Name() string {
	return "update-checker-raft"
}

var raftHTTPClient = func() *http.Client {
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{Proxy: proxy.Proxy()},
	}
}

// RaftUpdateChecker fetches the release index and stores it. A fetch or
// parse error is stored too, for the dashboard; only a failure to store is
// returned. An empty endpoint disables the check.
func RaftUpdateChecker(ctx context.Context, endpoint string) error {
	var state RaftCheckerState
	if endpoint != "" {
		releases, err := getRaftReleases(ctx, endpoint)
		if err != nil {
			state.Error = err.Error()
		} else {
			state.Releases = releases
		}
	}
	return system.AppState.Set(ctx, &state)
}

func getRaftReleases(ctx context.Context, endpoint string) ([]RaftRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := raftHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("forgejo-raft release index: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	return parseRaftIndex(body)
}

func parseRaftIndex(body []byte) ([]RaftRelease, error) {
	var index struct {
		Releases []RaftRelease `json:"releases"`
	}
	if err := json.Unmarshal(body, &index); err != nil {
		return nil, fmt.Errorf("forgejo-raft release index: %w", err)
	}
	releases := make([]RaftRelease, 0, len(index.Releases))
	for _, r := range index.Releases {
		// An entry this build cannot read is skipped, not fatal: the index
		// may one day list versions in a form only newer builds know.
		if _, err := ParseRaftVersion(r.Version); err != nil {
			continue
		}
		releases = append(releases, r)
	}
	return releases, nil
}

// RaftNoticeState says what the dashboard tells about forgejo-raft.
type RaftNoticeState int

const (
	// Nothing to say: no newer official release, no newer raft build.
	RaftNoticeNone RaftNoticeState = iota
	// A raft build newer than the running one exists for the target version.
	RaftNoticeAvailable
	// An official release is out and its raft build is not published yet.
	RaftNoticePending
)

// RaftNotice is what the dashboard shows next to the upstream notice.
type RaftNotice struct {
	State    RaftNoticeState
	Official string // official version recommended upstream, "" if none
	Release  RaftRelease
	Page     string
}

func (n *RaftNotice) Available() bool { return n.State == RaftNoticeAvailable }
func (n *RaftNotice) Pending() bool   { return n.State == RaftNoticePending }

// GetRaftNotice combines the official version the upstream checker
// recommends ("" when there is none) with the stored raft release index.
func GetRaftNotice(ctx context.Context, official string) (*RaftNotice, error) {
	item := new(RaftCheckerState)
	if err := system.AppState.Get(ctx, item); err != nil {
		return nil, fmt.Errorf("system appstate unable to retrieve raft update checker output: %w", err)
	}
	if item.Error != "" {
		return nil, errors.New(item.Error)
	}
	if item.Releases == nil {
		// Never checked, or disabled: nothing is known.
		return &RaftNotice{Page: RaftReleasesPage}, nil
	}
	return computeRaftNotice(setting.AppVer, official, item.Releases)
}

func computeRaftNotice(running, official string, releases []RaftRelease) (*RaftNotice, error) {
	notice := &RaftNotice{Official: official, Page: RaftReleasesPage}
	current, err := ParseRaftVersion(running)
	if err != nil {
		return nil, err
	}
	target := current.Upstream
	if official != "" {
		if target, err = version.NewVersion(official); err != nil {
			return nil, fmt.Errorf("failure to parse official version %q: %w", official, err)
		}
	}
	// A development line (18.0.0-dev) is only ever offered prereleases of
	// its own line; a stable one only stable releases.
	devLine := target.Prerelease() != ""

	var best *RaftVersion
	for _, r := range releases {
		v, err := ParseRaftVersion(r.Version)
		if err != nil || v.N == 0 || !v.Upstream.Equal(target) || r.Prerelease != devLine {
			continue
		}
		if best == nil || v.Compare(best) > 0 {
			best = v
			notice.Release = r
		}
	}

	switch {
	case official != "" && best != nil:
		notice.State = RaftNoticeAvailable
	case official != "":
		notice.State = RaftNoticePending
	case best != nil && best.Compare(current) > 0:
		notice.State = RaftNoticeAvailable
	default:
		notice.Release = RaftRelease{}
	}
	if notice.Release.URL == "" && notice.State == RaftNoticeAvailable {
		notice.Release.URL = RaftReleasesPage
	}
	return notice, nil
}
