// Copyright 2022 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unittest"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/storage"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster"
	"forgejo.org/services/cluster/consensus/kitsunium"
	repo_service "forgejo.org/services/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestMain(m *testing.M) {
	unittest.MainTest(m, runtimeDirOptions())
}

// runtimeDirOptions gives the engine a boot-scoped marker directory of this
// binary's own, which a runner cannot create at its Linux default,
// /run/kitsunium-consensus. SetUp runs only in the suite, never in the git
// hooks that run this binary, and TearDown removes the directory.
func runtimeDirOptions() *unittest.TestOptions {
	return &unittest.TestOptions{
		SetUp: func() error {
			dir, err := os.MkdirTemp("", "kitsunium-consensus-")
			kitsunium.RuntimeDir = dir
			return err
		},
		TearDown: func() error { return os.RemoveAll(kitsunium.RuntimeDir) },
	}
}

func makePathOutput(workPath, customPath, customConf string) string {
	return fmt.Sprintf("WorkPath=%s\nCustomPath=%s\nCustomConf=%s", workPath, customPath, customConf)
}

func newTestApp(testCmdAction func(_ context.Context, ctx *cli.Command) error) *cli.Command {
	app := NewMainApp("version", "version-extra")
	testCmd := &cli.Command{Name: "test-cmd", Action: testCmdAction}
	prepareSubcommandWithConfig(testCmd, appGlobalFlags)
	app.Commands = append(app.Commands, testCmd)
	app.DefaultCommand = testCmd.Name
	return app
}

type runResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func runTestApp(app *cli.Command, args ...string) (runResult, error) {
	outBuf := new(strings.Builder)
	errBuf := new(strings.Builder)
	app.Writer = outBuf
	app.ErrWriter = errBuf
	exitCode := -1
	defer test.MockVariableValue(&cli.ErrWriter, app.ErrWriter)()
	defer test.MockVariableValue(&cli.OsExiter, func(code int) {
		if exitCode == -1 {
			exitCode = code // save the exit code once and then reset the writer (to simulate the exit)
			app.Writer, app.ErrWriter, cli.ErrWriter = io.Discard, io.Discard, io.Discard
		}
	})()
	err := RunMainApp(app, args...)
	return runResult{outBuf.String(), errBuf.String(), exitCode}, err
}

func TestCliCmd(t *testing.T) {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}

	defaultWorkPath := filepath.Dir(path)
	defaultCustomPath := filepath.Join(defaultWorkPath, "custom")
	defaultCustomConf := filepath.Join(defaultCustomPath, "conf/app.ini")

	cli.CommandHelpTemplate = "(command help template)"
	cli.SubcommandHelpTemplate = "(subcommand help template)"

	cases := []struct {
		env map[string]string
		cmd string
		exp string
	}{
		// main command help
		{
			cmd: "./gitea help",
			exp: "DEFAULT CONFIGURATION:",
		},

		// parse paths
		{
			cmd: "./gitea test-cmd",
			exp: makePathOutput(defaultWorkPath, defaultCustomPath, defaultCustomConf),
		},
		{
			cmd: "./gitea -c /tmp/app.ini test-cmd",
			exp: makePathOutput(defaultWorkPath, defaultCustomPath, "/tmp/app.ini"),
		},
		{
			cmd: "./gitea test-cmd -c /tmp/app.ini",
			exp: makePathOutput(defaultWorkPath, defaultCustomPath, "/tmp/app.ini"),
		},
		{
			env: map[string]string{"GITEA_WORK_DIR": "/tmp"},
			cmd: "./gitea test-cmd",
			exp: makePathOutput("/tmp", "/tmp/custom", "/tmp/custom/conf/app.ini"),
		},
		{
			env: map[string]string{"GITEA_WORK_DIR": "/tmp"},
			cmd: "./gitea test-cmd --work-path /tmp/other",
			exp: makePathOutput("/tmp/other", "/tmp/other/custom", "/tmp/other/custom/conf/app.ini"),
		},
		{
			env: map[string]string{"GITEA_WORK_DIR": "/tmp"},
			cmd: "./gitea test-cmd --config /tmp/app-other.ini",
			exp: makePathOutput("/tmp", "/tmp/custom", "/tmp/app-other.ini"),
		},
		{
			env: map[string]string{"GITEA_WORK_DIR": "/tmp"},
			cmd: "./gitea forgejo-cli --help",
			exp: "(subcommand help template)",
		},
	}

	for _, c := range cases {
		t.Run(c.cmd, func(t *testing.T) {
			defer test.MockProtect(&setting.AppWorkPath)()
			defer test.MockProtect(&setting.CustomPath)()
			defer test.MockProtect(&setting.CustomConf)()

			app := newTestApp(func(_ context.Context, ctx *cli.Command) error {
				_, _ = fmt.Fprint(ctx.Root().Writer, makePathOutput(setting.AppWorkPath, setting.CustomPath, setting.CustomConf))
				return nil
			})

			for k, v := range c.env {
				t.Setenv(k, v)
			}
			args := strings.Split(c.cmd, " ") // for test only, "split" is good enough
			r, err := runTestApp(app, args...)
			require.NoError(t, err, c.cmd)
			assert.NotEmpty(t, c.exp, c.cmd)
			assert.Contains(t, r.Stdout, c.exp, c.cmd+"\n"+r.Stdout)
		})
	}
}

func TestCliCmdError(t *testing.T) {
	app := newTestApp(func(_ context.Context, ctx *cli.Command) error { return errors.New("normal error") })
	r, err := runTestApp(app, "./gitea", "test-cmd")
	require.Error(t, err)
	assert.Equal(t, 1, r.ExitCode)
	assert.Empty(t, r.Stdout)
	assert.Equal(t, "Command error: normal error\n", r.Stderr)

	app = newTestApp(func(_ context.Context, ctx *cli.Command) error { return cli.Exit("exit error", 2) })
	r, err = runTestApp(app, "./gitea", "test-cmd")
	require.Error(t, err)
	assert.Equal(t, 2, r.ExitCode)
	assert.Empty(t, r.Stdout)
	assert.Equal(t, "exit error\n", r.Stderr)

	app = newTestApp(func(_ context.Context, ctx *cli.Command) error { return nil })
	r, err = runTestApp(app, "./gitea", "test-cmd", "--no-such")
	require.Error(t, err)
	assert.Equal(t, 1, r.ExitCode)
	assert.Equal(t, "Incorrect Usage: flag provided but not defined: -no-such\n\n", r.Stderr)
	assert.Empty(t, r.Stdout)

	app = newTestApp(func(_ context.Context, ctx *cli.Command) error { return nil })
	r, err = runTestApp(app, "./gitea", "test-cmd")
	require.NoError(t, err)
	assert.Equal(t, -1, r.ExitCode) // the cli.OsExiter is not called
	assert.Empty(t, r.Stdout)
	assert.Empty(t, r.Stderr)
}

func TestCliCmdVersion(t *testing.T) {
	for _, flag := range []string{"--version", "-v"} {
		t.Run(flag, func(t *testing.T) {
			defaultActionCalled := false
			app := newTestApp(func(_ context.Context, ctx *cli.Command) error {
				defaultActionCalled = true
				return nil
			})
			r, err := runTestApp(app, "./gitea", flag)
			require.NoError(t, err)
			assert.False(t, defaultActionCalled, "%s must not run the default command", flag)
			assert.Equal(t, -1, r.ExitCode)
			assert.Equal(t, "forgejo version versionversion-extra\n", r.Stdout)
			assert.Empty(t, r.Stderr)
		})
	}
}

// clusterNodeApp returns the CLI with cmd among its commands, in place of
// the one of the same name, prepared as every command with a configuration
// is, and the arguments that run cmd's path on a node of a cluster:
// [cluster] enabled, no replicator in the process. The configuration, the
// work path and every path they derive live under t.TempDir(); the
// settings the configuration loads are restored when the test ends.
func clusterNodeApp(t *testing.T, cmd *cli.Command, path ...string) (*cli.Command, []string) {
	t.Helper()
	for _, p := range []func(){
		test.MockProtect(&setting.AppWorkPath), test.MockProtect(&setting.CustomPath),
		test.MockProtect(&setting.CustomConf), test.MockProtect(&setting.AppDataPath),
		test.MockProtect(&setting.RepoRootPath), test.MockProtect(&setting.Cluster),
	} {
		t.Cleanup(p)
	}
	root := t.TempDir()
	conf := filepath.Join(root, "app.ini")
	require.NoError(t, os.WriteFile(conf, []byte(fmt.Sprintf(`[server]
APP_DATA_PATH = %s
[repository]
ROOT = %s
[cluster]
ENABLED = true
`, filepath.Join(root, "data"), filepath.Join(root, "repos"))), 0o600))

	app := NewMainApp("version", "version-extra")
	prepareSubcommandWithConfig(cmd, appGlobalFlags)
	replaced := false
	for i, c := range app.Commands {
		if c.Name == cmd.Name {
			app.Commands[i], replaced = cmd, true
		}
	}
	if !replaced {
		app.Commands = append(app.Commands, cmd)
	}
	return app, append([]string{"./forgejo", "--work-path", root, "--config", conf}, path...)
}

// gatedStorage returns a storage under t.TempDir() that deletes through the
// registered gate, as storage.Attachments, storage.LFS and
// storage.Packages do, with names saved in it; the gate goes when the test
// ends.
func gatedStorage(t *testing.T, names ...string) (st storage.ObjectStorage, exists func(name string) bool) {
	t.Helper()
	t.Cleanup(func() { storage.SetDeleteGate(nil) })
	cfg := &setting.Storage{Type: setting.LocalStorageType, Path: t.TempDir()}
	local, err := storage.NewLocalStorage(t.Context(), cfg)
	require.NoError(t, err)
	st = storage.WithDeleteGate(local)
	for _, name := range names {
		_, err := st.Save(name, strings.NewReader("1234567"), 7)
		require.NoError(t, err)
	}
	return st, func(name string) bool {
		_, err := os.Stat(filepath.Join(cfg.Path, name))
		return err == nil
	}
}

// deleteBefore deletes name from st, and fails the test when the deletion
// still waits after d.
func deleteBefore(t *testing.T, st storage.ObjectStorage, name string, d time.Duration, why string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- st.Delete(name) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(d):
		t.Fatalf("the deletion of %s still waits after %v: %s", name, d, why)
	}
}

// On a node of a cluster, a stored file a command of the CLI deletes
// waits, as a running node's does, for a checkpoint listing the stored
// files (#90): otherwise the checkpoint misses a file the SQL at its L
// names, and `forgejo cluster recover --from-observer` refuses it at its
// sixth step. #358 registered the deletion gate in `forgejo doctor` only,
// and `forgejo admin user delete --purge` deleted the attachments, the LFS
// objects and the packages of what it purged at once (#363). Every command
// but `forgejo web` registers it now, from the wrapper every command with
// a configuration runs in (prepareWorkPathAndCustomConf).
//
// The command is nested as `forgejo admin user delete` is, and does what
// the purge does to each repository: the real DeleteRepositoryDirectly, on
// user2/repo1, while a checkpoint holds the cut. Once the purge's SQL
// committed, after the checkpoint's L, the checkpoint's walk still finds
// every attachment the SQL at L names; the files go once it released the
// cut. Every file the deletion touches lives under t.TempDir().
func TestCLIDeletesStoredFilesUnderTheCut(t *testing.T) {
	require.NoError(t, unittest.PrepareTestDatabase())
	hold, undo := cluster.SetCutWithoutReplicatorForTesting()
	defer undo()
	ran := false
	purge := func(ctx context.Context, _ *cli.Command) error {
		ran = true
		root := t.TempDir()
		local := func(dir string) storage.ObjectStorage {
			st, err := storage.NewLocalStorage(ctx, &setting.Storage{Type: setting.LocalStorageType, Path: filepath.Join(root, dir)})
			require.NoError(t, err)
			return st
		}
		attachments := filepath.Join(root, "attachments")
		t.Cleanup(test.MockVariableValue(&setting.RepoRootPath, filepath.Join(root, "repos")))
		t.Cleanup(test.MockVariableValue(&storage.Attachments, storage.WithDeleteGate(local("attachments"))))
		t.Cleanup(test.MockVariableValue(&storage.LFS, storage.WithDeleteGate(local("lfs"))))
		t.Cleanup(test.MockVariableValue(&storage.Packages, storage.WithDeleteGate(local("packages"))))
		t.Cleanup(test.MockVariableValue(&storage.Avatars, local("avatars")))
		t.Cleanup(test.MockVariableValue(&storage.RepoAvatars, local("repo-avatars")))
		t.Cleanup(test.MockVariableValue(&storage.RepoArchives, local("repo-archive")))
		t.Cleanup(test.MockVariableValue(&storage.Actions, local("actions_log")))
		t.Cleanup(test.MockVariableValue(&storage.ActionsArtifacts, local("actions_artifacts")))
		t.Cleanup(func() { storage.SetDeleteGate(nil) })
		require.True(t, setting.Cluster.Enabled, "the command runs on a node of a cluster")

		var atts []*repo_model.Attachment
		require.NoError(t, db.GetEngine(ctx).Where("repo_id = ?", 1).Find(&atts))
		require.NotEmpty(t, atts)
		for _, a := range atts {
			_, err := storage.Attachments.Save(a.RelativePath(), strings.NewReader("1234567"), 7)
			require.NoError(t, err)
		}

		// A checkpoint takes the cut, then reads L.
		release := hold()
		purged := make(chan error, 1)
		go func() {
			purged <- repo_service.DeleteRepositoryDirectly(ctx, 1, repo_service.DeleteRepositoryOpts{})
		}()
		// Its walk takes its time on a large storage; the purge's SQL
		// commits meanwhile, after L.
		var early error
		returned := false
		select {
		case early = <-purged:
			returned = true
		case <-time.After(2 * time.Second):
		}
		// The purge's SQL committed after L.
		unittest.AssertNotExistsBean(t, &repo_model.Repository{ID: 1})
		var listed []string
		for _, a := range atts {
			if _, err := os.Stat(filepath.Join(attachments, a.RelativePath())); err == nil {
				listed = append(listed, a.RelativePath())
			}
		}
		release()
		if !returned {
			select {
			case early = <-purged:
			case <-time.After(30 * time.Second):
				t.Fatal("the purge still waits after the checkpoint released the cut")
			}
		}
		require.NoError(t, early)
		assert.False(t, returned, "the purge returned while the checkpoint held the cut: it did not wait for the walk")
		for _, a := range atts {
			assert.Contains(t, listed, a.RelativePath(),
				"the checkpoint's walk misses an attachment the SQL at L names: recover --from-observer refuses this checkpoint at step 6")
			_, err := os.Stat(filepath.Join(attachments, a.RelativePath()))
			assert.ErrorIs(t, err, os.ErrNotExist, "the attachment goes once the checkpoint released the cut")
		}
		return nil
	}
	admin := &cli.Command{Name: "admin", Commands: []*cli.Command{{
		Name:     "user",
		Commands: []*cli.Command{{Name: "delete", Action: purge}},
	}}}
	app, args := clusterNodeApp(t, admin, "admin", "user", "delete")
	r, err := runTestApp(app, args...)
	require.NoError(t, err, r.Stderr)
	require.True(t, ran)
}

// The command's gate goes with the command: it puts back the gate
// registered before it, and only while it is still the registered one,
// so that a gate registered after it stays.
func TestCLIDeletionGateGoesWithTheCommand(t *testing.T) {
	hold, undo := cluster.SetCutWithoutReplicatorForTesting()
	defer undo()
	st, exists := gatedStorage(t, "after", "later")

	app, args := clusterNodeApp(t, &cli.Command{Name: "test-cmd", Action: func(context.Context, *cli.Command) error { return nil }}, "test-cmd")
	_, err := runTestApp(app, args...)
	require.NoError(t, err)
	release := hold()
	deleteBefore(t, st, "after", 5*time.Second, "the command's gate outlived the command")
	release()
	assert.False(t, exists("after"))

	later := 0
	app, args = clusterNodeApp(t, &cli.Command{Name: "test-cmd", Action: func(context.Context, *cli.Command) error {
		storage.SetDeleteGate(func(del func() error) error { later++; return del() })
		return nil
	}}, "test-cmd")
	_, err = runTestApp(app, args...)
	require.NoError(t, err)
	require.NoError(t, st.Delete("later"))
	assert.Equal(t, 1, later, "the gate registered after the command's stays once the command ended")
	assert.False(t, exists("later"))
}

// `forgejo web` registers no gate of the CLI's: its replicator registers its
// own once it started (cluster.Init), and that one stays once the command
// ended. clusterhook is not enabled yet when the command starts, so the
// command is told apart by its name.
func TestCLIWebKeepsTheReplicatorsDeletionGate(t *testing.T) {
	hold, undo := cluster.SetCutWithoutReplicatorForTesting()
	defer undo()
	st, exists := gatedStorage(t, "before", "after")

	replicator := 0
	web := &cli.Command{Name: "web", Action: func(context.Context, *cli.Command) error {
		// Before the replicator started, while a checkpoint holds the
		// cut: no gate of the CLI's holds the deletion.
		release := hold()
		deleteBefore(t, st, "before", 5*time.Second, "forgejo web registered the gate of a process without a replicator")
		release()
		storage.SetDeleteGate(func(del func() error) error { replicator++; return del() })
		return nil
	}}
	app, args := clusterNodeApp(t, web, "web")
	_, err := runTestApp(app, args...)
	require.NoError(t, err)
	assert.False(t, exists("before"))
	require.NoError(t, st.Delete("after"))
	assert.Equal(t, 1, replicator, "the replicator's gate stays")
	assert.False(t, exists("after"))
}
