// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	"forgejo.org/modules/json"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"

	"github.com/hashicorp/go-version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

const (
	testClusterHookEnv = "FORGEJO_TEST_CLUSTER_HOOK"
	testClusterRootEnv = "FORGEJO_TEST_CLUSTER_ROOT"
	testClusterSockEnv = "FORGEJO_TEST_CLUSTER_SOCK"
)

// When git runs this test binary as the cluster's hook, through the
// wrappers InitDelegateHooks generates (`<AppPath> hook --config=...
// reference-transaction <state>`), it runs the production hook and exits.
func init() {
	if os.Getenv(testClusterHookEnv) != "1" {
		return
	}
	setting.InstallLock = true
	setting.RepoRootPath = os.Getenv(testClusterRootEnv)
	setting.Cluster.Enabled = true
	setting.Cluster.HookSocket = os.Getenv(testClusterSockEnv)
	setting.Cluster.HookTimeout = 20 * time.Second
	args := []string{"hook"}
	for i, a := range os.Args {
		if a == "reference-transaction" {
			args = append(args, os.Args[i:]...)
			break
		}
	}
	app := &cli.Command{Name: "hook", Commands: []*cli.Command{subcmdHookReferenceTransaction()}, ExitErrHandler: func(context.Context, *cli.Command, error) {}}
	if err := app.Run(context.Background(), args); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

// testReplicator stands in for the replicator on its socket: it records
// each request and refuses every "prepared", as a node does when the
// cluster refuses. A push that succeeds was written outside the cluster.
type testReplicator struct {
	mu   sync.Mutex
	reqs []clusterhook.Request
}

func (h *testReplicator) prepared() []clusterhook.Request {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []clusterhook.Request
	for _, r := range h.reqs {
		if r.Op == clusterhook.OpPrepared {
			out = append(out, r)
		}
	}
	return out
}

func serveTestReplicator(t *testing.T) (string, *testReplicator) {
	sockDir, err := os.MkdirTemp("", "fjch") //nolint:usetesting // a unix socket path must stay short: 104 bytes on macOS
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "s")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	rep := &testReplicator{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			var req clusterhook.Request
			if json.NewDecoder(c).Decode(&req) != nil {
				c.Close()
				continue
			}
			resp := clusterhook.Response{OK: true}
			if req.Op == clusterhook.OpPrepared {
				resp = clusterhook.Response{OK: false, Message: "refused by the test replicator"}
			}
			rep.mu.Lock()
			rep.reqs = append(rep.reqs, req)
			rep.mu.Unlock()
			_ = json.NewEncoder(c).Encode(resp)
			c.Close()
		}
	}()
	return sock, rep
}

// clusterHookEnv is a node's git with the hooks InitDelegateHooks
// generates, whose hook is the production one run by this test binary.
type clusterHookEnv struct {
	base, root, home, bare, work string
	env                          []string
	rep                          *testReplicator
}

func newClusterHookEnv(t *testing.T) *clusterHookEnv {
	base, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	e := &clusterHookEnv{base: base, root: filepath.Join(base, "root"), home: filepath.Join(base, "home")}
	require.NoError(t, os.MkdirAll(filepath.Join(e.root, "alice"), 0o755))
	sock, rep := serveTestReplicator(t)
	e.rep = rep

	bin, err := os.Executable()
	require.NoError(t, err)
	defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
	defer test.MockVariableValue(&setting.AppPath, bin)()
	defer test.MockVariableValue(&setting.CustomConf, filepath.Join(base, "app.ini"))()
	defer test.MockVariableValue(&setting.ScriptType, "bash")()
	require.NoError(t, git.InitDelegateHooks(e.home))

	gcfg := filepath.Join(base, "gitconfig")
	require.NoError(t, os.WriteFile(gcfg, []byte("[user]\n\tname = t\n\temail = t@t\n[commit]\n\tgpgSign = false\n[init]\n\tdefaultBranch = main\n"), 0o644))
	// The hook's temporary directory is the test's own: t.TempDir() is
	// under the system's.
	require.NoError(t, os.MkdirAll(filepath.Join(base, "tmp"), 0o755))
	e.env = append(os.Environ(), "GIT_CONFIG_GLOBAL="+gcfg, "GIT_CONFIG_NOSYSTEM=1", "TMPDIR="+filepath.Join(base, "tmp"),
		testClusterHookEnv+"=1", testClusterRootEnv+"="+e.root, testClusterSockEnv+"="+sock)
	e.bare = filepath.Join(e.root, "alice", "r.git")
	e.work = filepath.Join(base, "wc")
	e.git(t, base, "init", "-q", "--bare", e.bare)
	e.git(t, e.bare, "config", "core.hooksPath", filepath.Join(e.home, "hooks"))
	e.git(t, base, "init", "-q", e.work)
	e.git(t, e.work, "commit", "-q", "--allow-empty", "-m", "one")
	return e
}

func (e *clusterHookEnv) git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := e.run(dir, args...)
	require.NoError(t, err, "git %s: %s", strings.Join(args, " "), out)
	return out
}

func (e *clusterHookEnv) run(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(append([]string{}, e.env...), clusterhook.EnvTxn+"="+clusterhook.NewNonce())
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (e *clusterHookEnv) push(into, ref string) (string, error) {
	return e.run(e.work, "push", "--porcelain", into, "HEAD:"+ref)
}

func (e *clusterHookEnv) refExists(dir, ref string) bool {
	_, err := e.run(dir, "rev-parse", "--verify", "-q", ref)
	return err == nil
}

// The wrapper fails closed in "prepared" when the cluster's hook is missing,
// empty or not executable: git publishes nothing the cluster did not see
// (#97). Before, it ran no hook and exited 0.
func TestClusterHookWrapperFailsClosed(t *testing.T) {
	e := newClusterHookEnv(t)
	gitea := filepath.Join(e.home, "hooks", "reference-transaction.d", "gitea")
	content, err := os.ReadFile(gitea)
	require.NoError(t, err)

	out, err := e.push(e.bare, "refs/heads/control")
	require.Error(t, err, "control: the cluster refuses, git must not write: %s", out)
	require.False(t, e.refExists(e.bare, "refs/heads/control"))
	require.Len(t, e.rep.prepared(), 1, "control: the replicator saw the prepared phase")

	for _, s := range []struct {
		name string
		set  func() error
	}{
		{"gitea-absent", func() error { return os.Remove(gitea) }},
		{"gitea-empty", func() error { return os.WriteFile(gitea, nil, 0o755) }},
		{"gitea-not-executable", func() error { return os.Chmod(gitea, 0o644) }},
	} {
		t.Run(s.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(gitea, content, 0o755))
			require.NoError(t, s.set())
			out, err := e.push(e.bare, "refs/heads/"+s.name)
			require.Error(t, err, "the push was acknowledged without the cluster: %s", out)
			assert.False(t, e.refExists(e.bare, "refs/heads/"+s.name), "git wrote the ref outside the cluster")
		})
	}
}

// Hooks rewritten while pushes run (forgejo doctor, a start) never leave a
// moment without the gate: InitDelegateHooks replaces each file by a rename
// (#97). Before, it removed then wrote it, and about one push in fifteen
// was acknowledged without any "prepared".
func TestClusterHookRewriteKeepsTheGate(t *testing.T) {
	e := newClusterHookEnv(t)
	bin, err := os.Executable()
	require.NoError(t, err)

	stop := make(chan struct{})
	var rewrites atomic.Int64
	done := make(chan error, 1)
	go func() {
		var err error
		defer func() { done <- err }()
		// Two configurations, so every rewrite changes the content and
		// takes the replacing path.
		confs := []string{filepath.Join(e.base, "app.ini"), filepath.Join(e.base, "app2.ini")}
		defer test.MockVariableValue(&setting.Cluster.Enabled, true)()
		defer test.MockVariableValue(&setting.AppPath, bin)()
		defer test.MockVariableValue(&setting.ScriptType, "bash")()
		defer test.MockVariableValue(&setting.CustomConf, confs[0])()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			setting.CustomConf = confs[i%2]
			if err = git.InitDelegateHooks(e.home); err != nil {
				return
			}
			rewrites.Add(1)
		}
	}()

	var written []string
	for i := range 12 {
		ref := fmt.Sprintf("refs/heads/race-%d", i)
		if _, err := e.push(e.bare, ref); err == nil || e.refExists(e.bare, ref) {
			written = append(written, ref)
		}
	}
	close(stop)
	require.NoError(t, <-done)
	t.Logf("12 pushes, %d hook rewrites", rewrites.Load())
	assert.Empty(t, written, "pushes acknowledged although the cluster refuses every prepared")
}

// The hook names git's process, not its wrapper's, which exits as soon as
// the hook returns while git still holds its locks (#108, ADR 0152 R6-06).
// Each phase runs a wrapper of its own under the same git process: the
// "prepared" and the "aborted" that follows name the same PID.
func TestClusterHookNamesTheGitProcess(t *testing.T) {
	e := newClusterHookEnv(t)
	_, err := e.push(e.bare, "refs/heads/pid")
	require.Error(t, err)
	e.rep.mu.Lock()
	defer e.rep.mu.Unlock()
	pids := map[string]int{}
	for _, r := range e.rep.reqs {
		pids[r.Op] = r.PID
	}
	t.Logf("PIDs named per phase: %v", pids)
	require.Contains(t, pids, clusterhook.OpPrepared)
	require.Contains(t, pids, clusterhook.OpAborted)
	assert.Positive(t, pids[clusterhook.OpPrepared])
	assert.Equal(t, pids[clusterhook.OpPrepared], pids[clusterhook.OpAborted], "the PID is the hook wrapper's, one per phase")
}

// A repository reached through a symbolic link under the root (an owner
// moved to another disk) resolves outside it: the hook asks the replicator
// anyway, which refuses, instead of letting git write outside the cluster
// (#40).
func TestClusterHookSymlinkedOwnerFailsClosed(t *testing.T) {
	e := newClusterHookEnv(t)
	disk2 := filepath.Join(e.base, "disk2", "bigorg")
	require.NoError(t, os.MkdirAll(disk2, 0o755))
	require.NoError(t, os.Symlink(disk2, filepath.Join(e.root, "bigorg")))
	linked := filepath.Join(e.root, "bigorg", "r.git")
	e.git(t, e.base, "init", "-q", "--bare", linked)
	e.git(t, linked, "config", "core.hooksPath", filepath.Join(e.home, "hooks"))

	out, err := e.push(linked, "refs/heads/main")
	t.Logf("push into %s: err=%v\n%s", linked, err, out)
	require.Error(t, err, "the push into the symlinked owner was acknowledged")
	assert.False(t, e.refExists(linked, "refs/heads/main"), "git wrote the ref outside the cluster")
	assert.Len(t, e.rep.prepared(), 1, "the push never reached the replicator")

	// A temporary clone is still let through.
	tmp := filepath.Join(e.base, "tmp", "clone.git")
	e.git(t, e.base, "init", "-q", "--bare", tmp)
	e.git(t, tmp, "config", "core.hooksPath", filepath.Join(e.home, "hooks"))
	out, err = e.push(tmp, "refs/heads/main")
	require.NoError(t, err, "a temporary clone: %s", out)
	assert.Len(t, e.rep.prepared(), 1, "a temporary clone went to the replicator")
}

// [cluster] ENABLED=false written while the replicator still runs: the
// replicator answering on its socket is what counts (#40).
func TestClusterHookAsksAReplicatorThatRuns(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	root := filepath.Join(base, "root")
	repo := filepath.Join(root, "alice", "r.git")
	require.NoError(t, os.MkdirAll(repo, 0o755))
	sock, rep := serveTestReplicator(t)
	defer test.MockVariableValue(&setting.InstallLock, true)()
	defer test.MockVariableValue(&setting.RepoRootPath, root)()
	defer test.MockVariableValue(&setting.Cluster.Enabled, false)()
	defer test.MockVariableValue(&setting.Cluster.HookSocket, sock)()
	t.Chdir(repo)
	t.Setenv("GIT_DIR", ".")
	t.Setenv(clusterhook.EnvTxn, clusterhook.NewNonce())

	runPrepared := func() error {
		r, w, err := os.Pipe()
		require.NoError(t, err)
		_, _ = w.WriteString(strings.Repeat("0", 40) + " " + strings.Repeat("a", 40) + " refs/heads/main\n")
		w.Close()
		defer test.MockVariableValue(&os.Stdin, r)()
		app := &cli.Command{Name: "hook", Commands: []*cli.Command{subcmdHookReferenceTransaction()}, ExitErrHandler: func(context.Context, *cli.Command, error) {}}
		return app.Run(context.Background(), []string{"hook", "reference-transaction", "prepared"})
	}
	require.Error(t, runPrepared(), "prepared answered success without asking the replicator that runs")
	assert.Len(t, rep.prepared(), 1)

	// No replicator on the socket: the cluster is off.
	setting.Cluster.HookSocket = filepath.Join(base, "none.sock")
	require.NoError(t, runPrepared())
}

// A node of a cluster refuses a git older than 2.48 (#83).
func TestClusterGitVersionError(t *testing.T) {
	for v, ok := range map[string]bool{"2.39.5": false, "2.45.4": false, "2.47.3": false, "2.48.0": true, "2.48.2": true, "2.54.0": true, "2.56.0": true} {
		err := git.ClusterGitVersionError(version.Must(version.NewVersion(v)))
		if ok {
			assert.NoError(t, err, v)
		} else if assert.Error(t, err, v) {
			assert.Contains(t, err.Error(), v)
			assert.Contains(t, err.Error(), git.ClusterRequiredVersion)
		}
	}
	assert.Error(t, git.ClusterGitVersionError(nil))
}
