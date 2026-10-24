// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	cluster_model "forgejo.org/models/cluster"
	"forgejo.org/models/db"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/modules/setting"
	"forgejo.org/modules/test"
	"forgejo.org/services/cluster/reftable"

	"code.forgejo.org/xorm/xorm"
	"code.forgejo.org/xorm/xorm/names"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver Forgejo uses for PostgreSQL
)

// pgRepositories opens, on the PostgreSQL FORGEJO_CLUSTER_TEST_PGSQL names,
// a database of its own holding the repository table alone, makes it the
// default engine for the test, and returns it.
func pgRepositories(t *testing.T) *xorm.Engine {
	t.Helper()
	dsn := os.Getenv("FORGEJO_CLUSTER_TEST_PGSQL")
	if dsn == "" {
		t.Skip("FORGEJO_CLUSTER_TEST_PGSQL is not set")
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })
	name := fmt.Sprintf("forgejo_cluster_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE " + name)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)") })
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.Path = "/" + name
	eng, err := xorm.NewEngine("pgx", u.String())
	require.NoError(t, err)
	eng.SetMapper(names.GonicMapper{})
	t.Cleanup(func() { _ = eng.Close() })
	require.NoError(t, eng.Sync(new(repo_model.Repository)))

	prevEngine, prevContext := db.GetEngine(db.DefaultContext), db.DefaultContext
	db.SetDefaultEngine(context.Background(), eng)
	t.Cleanup(func() {
		db.SetDefaultEngine(context.Background(), prevEngine)
		db.DefaultContext = prevContext
	})
	t.Cleanup(test.MockVariableValue(&setting.Database.Type, setting.DatabaseType("postgres")))
	return eng
}

// A request's long transaction held a repository's row, as a fork counting
// itself on its base held it during its whole copy (#177): the push of that
// repository waited on the row in the single dispatcher of the log, and
// the SQL of every repository behind it. On PostgreSQL a batch's statement
// waits batchLockTimeout at most: the held repository's event is tried
// again, then parked, and another repository's event is derived while the
// request's transaction still runs. At batchLockTimeout, maxAttempts tries
// take some 25 seconds.
func TestADispatchDoesNotWaitForARowARequestHolds(t *testing.T) {
	eng := pgRepositories(t)
	for id, name := range map[int64]string{1: "base", 2: "other"} {
		_, err := eng.Insert(&repo_model.Repository{ID: id, OwnerID: 1, OwnerName: "o", Name: name, LowerName: name})
		require.NoError(t, err)
	}

	// The request's transaction: the fork counts itself on its base, then
	// copies for minutes.
	locked, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unlock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unlock)
	go func() {
		held <- db.WithTx(context.Background(), func(ctx context.Context) error {
			if err := repo_model.IncrementRepoForkNum(ctx, 1); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case err := <-held:
		require.NoError(t, err)
	}

	store, lg, parked := newSQLStore(), &memLog{}, &memParked{}
	c := unitConsumer(store, lg, parked, func(ctx context.Context, evs []reftable.Event) error {
		return repo_model.UpdateRepoSize(ctx, evs[0].RepoID, 10, 0)
	})
	c.tx, c.inTx = batchTx, db.InTransaction
	batch := []reftable.Event{
		{Token: testToken(1), Kind: reftable.EventRef, Repo: "o/base.git", RepoID: 1, Ref: "refs/heads/main", PusherID: 1},
		{Token: testToken(2), Kind: reftable.EventRef, Repo: "o/other.git", RepoID: 2, Ref: "refs/heads/main", PusherID: 1},
	}
	lineage := testToken(1).Identity().String()
	dispatched := make(chan error, 1)
	go func() {
		cur := &cluster_model.Cursor{}
		var err error
		for range maxAttempts + 1 {
			if err = c.dispatch(context.Background(), lineage, cur, batch); err == nil {
				break
			}
		}
		dispatched <- err
	}()
	select {
	case err := <-dispatched:
		require.NoError(t, err, "the batch goes on once the held repository's event is parked")
	case <-time.After(90 * time.Second):
		unlock()
		<-dispatched
		t.Fatal("the dispatcher waited for the request's transaction to commit")
	}

	var other repo_model.Repository
	_, err := eng.ID(int64(2)).Get(&other)
	require.NoError(t, err)
	assert.EqualValues(t, 10, other.Size, "the other repository's event is derived while the row is held")
	require.Len(t, parked.rows, 1)
	assert.Equal(t, "o/base.git", parked.rows[0].Repo, "the held repository's event is parked, retried on its own")

	unlock()
	require.NoError(t, <-held)
	var base repo_model.Repository
	_, err = eng.ID(int64(1)).Get(&base)
	require.NoError(t, err)
	assert.Equal(t, 1, base.NumForks)
	assert.Zero(t, base.Size)
}
