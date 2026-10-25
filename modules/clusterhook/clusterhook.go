// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

// Package clusterhook is the small shared part of Forgejo's ref replicator
// that the git launch wrapper, the hook command, `forgejo serv` and the
// replicator all need: the transaction nonce, and the protocol of the
// replicator's private socket (kitsunium ADR 0152 D19, ADR 0148). It holds
// no state of the replicator and imports nothing of it.
package clusterhook

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"forgejo.org/modules/json"
)

// EnvTxn carries the transaction nonce in git's environment. It is set LAST
// by Forgejo's git launch wrapper on every git invocation, so nothing a user
// controls overrides it; it is a correlation id, not an authenticator (R7-37).
const EnvTxn = "FORGEJO_CLUSTER_TXN"

// EnvGitPID names, to the reference-transaction hook, the git process that
// runs the transaction: the generated wrapper exports its own parent's PID
// before it runs the hook, whose parent is that wrapper.
const EnvGitPID = "FORGEJO_GIT_PID"

// EnvCreateOnly tells the reference-transaction hook that the git command
// creates its refs only if they are absent: `git tag` without -f, `git
// branch` without -f, update-ref's create. git gives such a line the same
// null old value as a forced update of a ref this node does not hold, and
// checks the absence against this node's git only (#189).
const EnvCreateOnly = "FORGEJO_CLUSTER_CREATE_ONLY"

// CreateOnlyEnv returns the environment of a git command that only creates
// its refs, EnvCreateOnly added to this process's, or nil, git's default,
// outside a cluster.
func CreateOnlyEnv() []string {
	if !Enabled() {
		return nil
	}
	return append(os.Environ(), EnvCreateOnly+"=1")
}

// CreateOnlyFromEnv reads EnvCreateOnly, as the hook relays it.
func CreateOnlyFromEnv() bool {
	return os.Getenv(EnvCreateOnly) == "1"
}

// StaleCreateMark is in the replicator's refusal of a creation of a ref
// the table already holds, which git on this node had not shown yet: the
// message reaches Forgejo only as git's error output.
const StaleCreateMark = "already exists in the cluster"

// IsStaleCreate reports whether err is a git command's failure on that
// refusal.
func IsStaleCreate(err error) bool {
	return err != nil && strings.Contains(err.Error(), StaleCreateMark)
}

// NewNonce draws a 128-bit nonce from crypto/rand.
func NewNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// Line is one line of the reference-transaction hook's input.
type Line struct {
	Old string `json:"old"`
	New string `json:"new"`
	Ref string `json:"ref"`
}

// ParseLines reads the reference-transaction hook's input: one
// "<old> SP <new> SP <ref> LF" per ref, split on the first two ASCII spaces
// exactly as git writes them. The ref name is the rest of the line, byte for
// byte: git accepts Unicode spaces in a ref name (U+00A0, U+3000), which a
// split on any white space would cut or drop. A line that does not split in
// three, or a name that is not valid UTF-8 (JSON would carry another name
// than git's), is an error: the caller fails closed instead of skipping a
// ref git is about to write.
func ParseLines(r io.Reader) ([]Line, error) {
	var lines []Line
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		text := sc.Text()
		if text == "" {
			continue
		}
		parts := strings.SplitN(text, " ", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("malformed reference-transaction line %q", text)
		}
		for _, p := range parts {
			if !utf8.ValidString(p) {
				return nil, fmt.Errorf("the ref name %q is not valid UTF-8, which the cluster cannot carry", text)
			}
		}
		lines = append(lines, Line{Old: parts[0], New: parts[1], Ref: parts[2]})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading the reference-transaction lines: %w", err)
	}
	return lines, nil
}

// TxnDigest names one git ref transaction among those of a git process: its
// lines, sorted, hashed. An older git (before 2.54) runs one transaction per
// ref of a non-atomic push under the same process nonce; the replicated
// table must tell them apart (TxnNonce).
func TxnDigest(lines []Line) string {
	keys := make([]string, 0, len(lines))
	for _, l := range lines {
		keys = append(keys, l.Old+" "+l.New+" "+l.Ref)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TxnNonce is the nonce a ref transaction carries in the replicated table:
// the process nonce, which correlates the request's read-after-write cookie,
// narrowed to the transaction by its digest.
func TxnNonce(nonce string, lines []Line) string {
	if nonce == "" {
		return ""
	}
	return nonce + "." + TxnDigest(lines)
}

// Ops of the socket protocol.
const (
	OpPrepared      = "prepared"
	OpCommitted     = "committed"
	OpAborted       = "aborted"
	OpRegister      = "register"
	OpStatus        = "status"
	OpSnapshot      = "snapshot-save"
	OpReproject     = "reproject"
	OpObjectsVerify = "objects-verify"
	OpReconcile     = "reconcile"
	OpImport        = "import"
	OpObjectsFill   = "objects-backfill"
	OpParkedList    = "parked-list"
	OpParkedResolve = "parked-resolve"
	// OpFresh waits until this node serves a repository at the commit
	// index the cluster confirms now, before `forgejo serv` runs git.
	OpFresh = "fresh"
	// OpReadable refuses a read of a repository whose directory on this
	// node is not the repository the table holds under its key, before
	// `forgejo serv` runs git.
	OpReadable = "readable"
)

// Request is one call on the socket: one JSON object per connection.
type Request struct {
	Op          string            `json:"op"`
	Nonce       string            `json:"nonce,omitempty"`
	GitDir      string            `json:"git_dir,omitempty"`
	Lines       []Line            `json:"lines,omitempty"`
	PusherID    int64             `json:"pusher_id,omitempty"`
	PusherName  string            `json:"pusher_name,omitempty"`
	RepoID      int64             `json:"repo_id,omitempty"`
	IsWiki      bool              `json:"is_wiki,omitempty"`
	PRID        int64             `json:"pr_id,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`
	PushOptions map[string]string `json:"push_options,omitempty"`
	// CreateOnly says the git command creates its refs only if they are
	// absent (EnvCreateOnly).
	CreateOnly bool `json:"create_only,omitempty"`
	// PID is the git process owning the transaction, as the hook's wrapper
	// names it (EnvGitPID), else the hook's parent.
	PID int `json:"pid,omitempty"`
	// Path and Repo are arguments of the operational calls.
	Path   string `json:"path,omitempty"`
	Repo   string `json:"repo,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
	// Lineage, Index and Reason name a parked event and why an operator
	// resolves it.
	Lineage string `json:"lineage,omitempty"`
	Index   int64  `json:"index,omitempty"`
	Reason  string `json:"reason,omitempty"`
	// Operator is set by the offline command from the local account; the
	// socket's peer credentials are what authenticates the caller.
	Operator string `json:"operator,omitempty"`
	// Export carries the arguments of an export op (export.go).
	Export *ExportRequest `json:"export,omitempty"`
}

// Response answers a Request.
type Response struct {
	OK      bool    `json:"ok"`
	Message string  `json:"message,omitempty"`
	Data    RawJSON `json:"data,omitempty"`
}

// RawJSON is a JSON value carried as is.
type RawJSON []byte

// MarshalJSON returns the value itself.
func (r RawJSON) MarshalJSON() ([]byte, error) {
	if len(r) == 0 {
		return []byte("null"), nil
	}
	return r, nil
}

// UnmarshalJSON keeps a copy of the value.
func (r *RawJSON) UnmarshalJSON(b []byte) error {
	*r = append((*r)[:0], b...)
	return nil
}

// Call sends one request to the replicator's socket and reads its answer.
func Call(ctx context.Context, socket string, req *Request) (*Response, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("cluster replicator unreachable on %s: %w", socket, err)
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, err
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("cluster replicator answer: %w", err)
	}
	return &resp, nil
}

// ---- the git launch wrapper ----

var (
	enabledMu sync.RWMutex
	enabled   bool
)

// SetEnabled turns the nonce on for every git invocation of this process.
func SetEnabled(on bool) {
	enabledMu.Lock()
	enabled = on
	enabledMu.Unlock()
}

// Enabled reports whether the nonce is set on git invocations.
func Enabled() bool {
	enabledMu.RLock()
	defer enabledMu.RUnlock()
	return enabled
}

type collectorKey struct{}

// Collector gathers the nonces of the git invocations a request made, so the
// response can carry a read-after-write cookie for the writes among them.
type Collector struct {
	mu     sync.Mutex
	nonces []string
}

// WithCollector returns a context whose git invocations are collected.
func WithCollector(ctx context.Context) (context.Context, *Collector) {
	c := &Collector{}
	return context.WithValue(ctx, collectorKey{}, c), c
}

// Nonces returns what was collected so far.
func (c *Collector) Nonces() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.nonces...)
}

// TxnEnv returns the variables the git launch wrapper appends last to every
// git invocation: a fresh nonce per invocation.
func TxnEnv(ctx context.Context) []string {
	if !Enabled() {
		return nil
	}
	n := NewNonce()
	if ctx != nil {
		if c, ok := ctx.Value(collectorKey{}).(*Collector); ok {
			c.mu.Lock()
			if len(c.nonces) < 256 {
				c.nonces = append(c.nonces, n)
			}
			c.mu.Unlock()
		}
	}
	return []string{EnvTxn + "=" + n}
}

// ErrRefused is a transaction the replicator refused.
var ErrRefused = errors.New("refused by the cluster")

// DefaultTimeout is the default of [cluster] HOOK_TIMEOUT, which bounds one
// hook call: a proposal, its objects and its commit.
const DefaultTimeout = 60 * time.Second

// hookMargin is how much less than the hook the replicator allows itself,
// so its answer reaches the hook before the hook gives up. A hook that gives
// up anyway cannot tell whether the write committed.
const hookMargin = 10 * time.Second

var hookTimeout atomic.Int64

// SetHookTimeout sets the hook's budget, [cluster] HOOK_TIMEOUT, for this
// process; the replicator calls it at start, with the value the hook reads.
func SetHookTimeout(d time.Duration) {
	hookTimeout.Store(int64(d))
}

// HookTimeout is the hook's budget: DefaultTimeout unless set.
func HookTimeout() time.Duration {
	if d := time.Duration(hookTimeout.Load()); d > hookMargin {
		return d
	}
	return DefaultTimeout
}

// ServerBudget is what the replicator allows itself for one hook call.
func ServerBudget() time.Duration {
	return HookTimeout() - hookMargin
}

// AdminTimeout bounds an operator's command on the socket (import,
// reconcile, objects backfill, snapshot, reproject): it moves whole
// repositories, which a hook's budget cannot.
const AdminTimeout = 30 * time.Minute

// ExportTimeout bounds an observer's export call, a stream included: a
// base backup of a large database runs for hours, as long as the
// observer's pass, whose default it is (#111).
const ExportTimeout = 6 * time.Hour

// Budget is what the replicator allows itself for one request: the hook's
// budget for the push path, the export's for an observer's call, the
// operator's for the rest.
func Budget(op string) time.Duration {
	switch {
	case slices.Contains([]string{OpPrepared, OpCommitted, OpAborted, OpRegister, OpStatus, OpFresh, OpReadable}, op):
		return ServerBudget()
	case IsExportOp(op):
		return ExportTimeout - 10*time.Second
	default:
		return AdminTimeout - 10*time.Second
	}
}

// ---- repository lifecycle ----

// A repository deleted or renamed by Forgejo changes the table too, so every
// member's projector follows (kitsunium ADR 0152 D19). The replicator
// registers these at start; without a cluster they do nothing.
var (
	lifecycleMu sync.RWMutex
	onDeleted   func(ctx context.Context, key string, repoID int64) error
	onRenamed   func(ctx context.Context, moves [][2]string) error
)

// SetRepoLifecycle registers the replicator's handlers.
func SetRepoLifecycle(deleted func(ctx context.Context, key string, repoID int64) error, renamed func(ctx context.Context, moves [][2]string) error) {
	lifecycleMu.Lock()
	onDeleted, onRenamed = deleted, renamed
	lifecycleMu.Unlock()
}

var checkKey func(key string) error

// SetKeyCheck registers the replicator's check of a repository key: nil
// when the cluster can take a repository under it now.
func SetKeyCheck(f func(key string) error) {
	lifecycleMu.Lock()
	checkKey = f
	lifecycleMu.Unlock()
}

// CheckRepoKey reports whether the cluster can take a repository named
// name under owner now, its wiki included: a name it refuses would fail in
// git init, and leave a directory no tool of the cluster removes (#54).
// It is always nil without a cluster.
func CheckRepoKey(owner, name string) error {
	lifecycleMu.RLock()
	f := checkKey
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	for _, wiki := range []bool{false, true} {
		if err := f(RepoKey(owner, name, wiki)); err != nil {
			return err
		}
	}
	return nil
}

// RepoKey is the table's key of a repository: its path under the
// repository root, lowercase, as Forgejo writes it.
func RepoKey(owner, name string, wiki bool) string {
	n := strings.ToLower(name)
	if wiki {
		n += ".wiki"
	}
	return strings.ToLower(owner) + "/" + n + ".git"
}

// RepoDeleted records the deletion of a repository and its wiki, after
// Forgejo removed them locally. They are two entries, the wiki's first,
// and the repository's only once the wiki's committed: a wiki left alive
// beside a deleted repository was offered by no reconcile, and refused,
// under its old repository ID, every repository created again under the
// name (#163). The repository left alive instead is an orphan reconcile
// reports, which `reconcile --delete-orphan` deletes with its wiki.
func RepoDeleted(ctx context.Context, owner, name string, repoID int64) error {
	lifecycleMu.RLock()
	f := onDeleted
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	if err := f(ctx, RepoKey(owner, name, true), repoID); err != nil {
		return err
	}
	return f(ctx, RepoKey(owner, name, false), repoID)
}

// WikiDeleted records the deletion of a repository's wiki alone, before
// Forgejo removes it locally: files removed on one member only would come
// back from the others, and its key would stay live in the table (#53).
func WikiDeleted(ctx context.Context, owner, name string, repoID int64) error {
	lifecycleMu.RLock()
	f := onDeleted
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	return f(ctx, RepoKey(owner, name, true), repoID)
}

// ErrOutcomeUnresolved is a lifecycle command the cluster may or may not
// have recorded: the proposal's outcome was unknown, and no commit
// boundary could tell within the budget. The caller keeps what it did
// locally, never undoes it: if the command committed, every member,
// itself included, follows the table, and an undo would leave the
// database naming what the table moved away (#46).
var ErrOutcomeUnresolved = errors.New("the cluster may or may not have recorded it")

// IsOutcomeUnresolved reports whether err is ErrOutcomeUnresolved.
func IsOutcomeUnresolved(err error) bool { return errors.Is(err, ErrOutcomeUnresolved) }

// RepoRenamed records a repository and its wiki moving, after Forgejo moved
// them locally. Both moves are one replicated command, applied together or
// not at all. An error means the other members did not follow: the caller
// moves them back, unless it is ErrOutcomeUnresolved. A repository whose
// key left the table since Forgejo loaded it is refused, as by
// RepoRenameRefusal (#220).
func RepoRenamed(ctx context.Context, oldOwner, oldName, newOwner, newName string) error {
	lifecycleMu.RLock()
	f := onRenamed
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	return f(ctx, [][2]string{
		{RepoKey(oldOwner, oldName, false), RepoKey(newOwner, newName, false)},
		{RepoKey(oldOwner, oldName, true), RepoKey(newOwner, newName, true)},
	})
}

// UndoRepoRenamed moves a repository and its wiki back in the replicated
// table after RepoRenamed succeeded and Forgejo's SQL then failed: every
// projector, the origin's included, follows the table, so without it the
// directories would stay under names the database does not know. It runs on
// a context detached from the failed request.
func UndoRepoRenamed(ctx context.Context, oldOwner, oldName, newOwner, newName string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultTimeout)
	defer cancel()
	return RepoRenamed(ctx, newOwner, newName, oldOwner, oldName)
}

// UndoOwnerRenamed is UndoRepoRenamed for every repository of an owner.
func UndoOwnerRenamed(ctx context.Context, oldOwner, newOwner string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultTimeout)
	defer cancel()
	return OwnerRenamed(ctx, newOwner, oldOwner)
}

var onOwnerRenamed func(ctx context.Context, oldOwner, newOwner string) error

// SetOwnerLifecycle registers the replicator's handler of a renamed owner.
func SetOwnerLifecycle(renamed func(ctx context.Context, oldOwner, newOwner string) error) {
	lifecycleMu.Lock()
	onOwnerRenamed = renamed
	lifecycleMu.Unlock()
}

// OwnerRenamed moves every repository of an owner, after Forgejo moved the
// owner's directory locally. An error means the other members did not
// follow: the caller moves it back.
func OwnerRenamed(ctx context.Context, oldOwner, newOwner string) error {
	lifecycleMu.RLock()
	f := onOwnerRenamed
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	return f(ctx, strings.ToLower(oldOwner), strings.ToLower(newOwner))
}

// ErrTooLarge is a lifecycle command larger than one replicated entry may
// be: refused before it is proposed, so nothing was recorded and the
// caller undoes what it did locally. Proposed again, it is refused again:
// until the cluster writes payload version 8, an owner's rename lists
// every repository of the owner in one entry (#182).
var ErrTooLarge = errors.New("too large for one replicated entry")

var checkOwnerRename func(ctx context.Context, oldOwner, newOwner string) error

// SetOwnerRenameCheck registers the replicator's check of an owner's
// rename, nil when the cluster can record it.
func SetOwnerRenameCheck(f func(ctx context.Context, oldOwner, newOwner string) error) {
	lifecycleMu.Lock()
	checkOwnerRename = f
	lifecycleMu.Unlock()
}

// OwnerRenameRefusal refuses an owner's rename the cluster cannot record,
// an error wrapping ErrTooLarge: Forgejo asks it before it writes the
// rename to SQL or moves the owner's directory, rather than undo both
// once the proposal is refused (#182). It reads this node's table once a
// commit boundary a quorum confirmed is applied, and refuses the rename
// when the node cannot confirm one within ctx; OwnerRenamed checks again
// what it proposes. It is always nil without a cluster.
func OwnerRenameRefusal(ctx context.Context, oldOwner, newOwner string) error {
	lifecycleMu.RLock()
	f := checkOwnerRename
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	return f(ctx, strings.ToLower(oldOwner), strings.ToLower(newOwner))
}

var checkRepoRename func(ctx context.Context, key string) error

// SetRepoRenameCheck registers the replicator's check of a repository's
// rename or transfer, nil when the cluster can record it.
func SetRepoRenameCheck(f func(ctx context.Context, key string) error) {
	lifecycleMu.Lock()
	checkRepoRename = f
	lifecycleMu.Unlock()
}

// RepoRenameRefusal refuses the rename or the transfer of the repository
// owner/name when its key left the replicated table, deleted or renamed
// away: an owner's rename through another node committed after Forgejo
// loaded the repository, and this node's disk, behind it, still holds the
// directory Forgejo would move. The cluster would record a move of
// nothing, and the repository would stay under a key the database no
// longer knows (#220). Forgejo asks it before it writes the move to SQL
// or moves the directories, rather than undo both once the proposal is
// refused. It reads this node's table once a commit boundary a quorum
// confirmed is applied, and refuses the move when the node cannot confirm
// one within ctx; RepoRenamed checks again what it proposes, for an
// owner's rename committed meanwhile. It is always nil without a cluster.
func RepoRenameRefusal(ctx context.Context, owner, name string) error {
	lifecycleMu.RLock()
	f := checkRepoRename
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	return f(ctx, RepoKey(owner, name, false))
}

// ---- the checkpoint's cut ----

// A checkpoint pairs the table at its barrier B with PostgreSQL at its WAL
// position L. A deletion, a rename or a transfer writes its SQL in the
// request, beside its replicated entry: the two must fall on the same side
// of (B, L), or a restore finds refs the SQL does not name (#88). The
// replicator registers a hold that a checkpoint waits for, on any member.
var (
	cutMu   sync.RWMutex
	holdCut func(ctx context.Context) (func(), error)
)

// SetCutHold registers the replicator's hold of the checkpoint's cut.
func SetCutHold(f func(ctx context.Context) (func(), error)) {
	cutMu.Lock()
	holdCut = f
	cutMu.Unlock()
}

// HoldCut keeps every checkpoint of the cluster out until release is
// called: what the caller writes to SQL and to the table in between lands
// wholly before a checkpoint's cut or wholly after it. It waits while a
// checkpoint holds the cut, until its stored files are listed, and is
// called before the SQL transaction opens. Holds nest: a checkpoint takes the cut only
// when none is held. Without a cluster it does nothing.
func HoldCut(ctx context.Context) (func(), error) {
	cutMu.RLock()
	f := holdCut
	cutMu.RUnlock()
	if f == nil {
		return func() {}, nil
	}
	release, err := f(ctx)
	if err != nil {
		return nil, fmt.Errorf("waiting for the cluster's checkpoint: %w", err)
	}
	var once sync.Once
	return func() { once.Do(release) }, nil
}

var mayGC func(key string) bool

// SetGarbageCollectionGate registers the projector's answer to "may git gc
// run on this repository now".
func SetGarbageCollectionGate(f func(key string) bool) {
	lifecycleMu.Lock()
	mayGC = f
	lifecycleMu.Unlock()
}

// MayCollectGarbage reports whether git gc may run on a repository; it is
// always true without a cluster.
func MayCollectGarbage(key string) bool {
	lifecycleMu.RLock()
	f := mayGC
	lifecycleMu.RUnlock()
	return f == nil || f(key)
}

var waitWrites func(ctx context.Context, nonces []string) bool

// SetWriteWaiter registers the replicator's wait for a request's writes.
func SetWriteWaiter(f func(ctx context.Context, nonces []string) bool) {
	lifecycleMu.Lock()
	waitWrites = f
	lifecycleMu.Unlock()
}

// WaitWrites waits, within the read-after-write bound, until this node is
// projected and the consumer has derived SQL for the writes a collector saw.
// It reports false on a timeout; without a cluster it returns at once.
func WaitWrites(ctx context.Context, c *Collector) bool {
	lifecycleMu.RLock()
	f := waitWrites
	lifecycleMu.RUnlock()
	if f == nil || c == nil {
		return true
	}
	return f(ctx, c.Nonces())
}

var (
	waitFresh  func(ctx context.Context, key string) bool
	readRefuse func(key string, repoID int64) error
)

// SetReadGuard registers the replicator's check of a repository's
// directory before it is read.
func SetReadGuard(f func(key string, repoID int64) error) {
	lifecycleMu.Lock()
	readRefuse = f
	lifecycleMu.Unlock()
}

// ReadRefusal says why this node must not serve the repository's directory
// now, or nil: the directory still holds another repository than the one
// the table holds under the key (a rename or a recreation of the name not
// projected here yet, or a directory stamped with another repository ID
// than repoID, the database's), which the database's permissions do not
// describe. repoID 0 compares no ID. Without a cluster it returns nil.
func ReadRefusal(key string, repoID int64) error {
	lifecycleMu.RLock()
	f := readRefuse
	lifecycleMu.RUnlock()
	if f == nil {
		return nil
	}
	return f(key, repoID)
}

// SetFreshWaiter registers the replicator's wait for a repository to be
// served at the cluster's current commit index.
func SetFreshWaiter(f func(ctx context.Context, key string) bool) {
	lifecycleMu.Lock()
	waitFresh = f
	lifecycleMu.Unlock()
}

// WaitFresh waits, within the read-after-write bound, until this node
// serves the repository at least at the commit index the cluster confirms
// at the call: a write any node acknowledged before is then visible here.
// git clients carry no read-after-write cookie, so git's reads ask this
// before git runs. It reports false on a timeout or without a leader, when
// the node may be behind; without a cluster it returns true at once.
func WaitFresh(ctx context.Context, key string) bool {
	lifecycleMu.RLock()
	f := waitFresh
	lifecycleMu.RUnlock()
	if f == nil {
		return true
	}
	return f(ctx, key)
}

// ErrBehind refuses a write this node would decide from its own git while
// it may be behind the cluster's last writes to the repository: it would
// overwrite, or take for absent, what another node acknowledged.
var ErrBehind = errors.New("this node of the cluster may be behind the cluster's last writes")

// RequireFresh is WaitFresh as an error: nil once this node serves the
// repository at the commit index the cluster confirms at the call, else
// ErrBehind naming the repository. Without a cluster it returns nil.
func RequireFresh(ctx context.Context, key string) error {
	if WaitFresh(ctx, key) {
		return nil
	}
	return fmt.Errorf("%w to %s; nothing was written, retry shortly or through another node", ErrBehind, key)
}

var refLive func(key, ref string) bool

// SetRefLookup registers the replicator's read of a ref in the table this
// node applied.
func SetRefLookup(f func(key, ref string) bool) {
	lifecycleMu.Lock()
	refLive = f
	lifecycleMu.Unlock()
}

// TableHoldsRef reports whether the table this node applied holds ref of
// the repository key live, which this node's git may not show yet. Without
// a cluster it reports false.
func TableHoldsRef(key, ref string) bool {
	lifecycleMu.RLock()
	f := refLive
	lifecycleMu.RUnlock()
	return f != nil && f(key, ref)
}

type afterCommitKey struct{}

// AfterCommit collects side effects that must happen once the work of a
// replicated command committed, and never on a rolled-back attempt: the log
// consumer replays a batch until its SQL transaction commits, and a
// goroutine or a queue started during a failed attempt would act again on
// the replay (kitsunium ADR 0152 D10, R8-25).
type AfterCommit struct {
	mu  sync.Mutex
	fns []func()
}

// WithAfterCommit returns a context whose Defer calls collect into the
// returned AfterCommit instead of running at once.
func WithAfterCommit(ctx context.Context) (context.Context, *AfterCommit) {
	a := &AfterCommit{}
	return context.WithValue(ctx, afterCommitKey{}, a), a
}

// Defer runs fn after the replicated command's work commits when ctx is
// collecting, and at once otherwise (no cluster: unchanged behaviour).
func Defer(ctx context.Context, fn func()) {
	if a, ok := ctx.Value(afterCommitKey{}).(*AfterCommit); ok {
		a.mu.Lock()
		a.fns = append(a.fns, fn)
		a.mu.Unlock()
		return
	}
	fn()
}

// Collecting reports whether ctx defers side effects to a commit.
func Collecting(ctx context.Context) bool {
	_, ok := ctx.Value(afterCommitKey{}).(*AfterCommit)
	return ok
}

// Run runs what was collected, once, in order.
func (a *AfterCommit) Run() {
	a.mu.Lock()
	fns := a.fns
	a.fns = nil
	a.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

type commandKey struct{}

// WithCommand marks a context as doing the work of one replicated command,
// named by its lineage-scoped identity. Side effects created under it — a
// webhook task — derive their identity from it, so a replay after a
// failover creates no second one (kitsunium ADR 0152 D10, R8-25).
func WithCommand(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, commandKey{}, identity)
}

// WithCommandPart narrows a command's identity to one part of it, such as
// one ref of a multi-ref transaction.
func WithCommandPart(ctx context.Context, part string) context.Context {
	id, ok := CommandIdentity(ctx)
	if !ok {
		return ctx
	}
	return WithCommand(ctx, id+"|"+part)
}

// CommandIdentity returns the identity WithCommand set.
func CommandIdentity(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(commandKey{}).(string)
	return id, ok && id != ""
}
