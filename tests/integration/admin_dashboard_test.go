// Copyright 2025 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package integration

import (
	"fmt"
	"net/http"
	"testing"

	"forgejo.org/modules/setting"
	"forgejo.org/modules/system"
	"forgejo.org/modules/test"
	"forgejo.org/modules/translation"
	"forgejo.org/modules/updatechecker"
	"forgejo.org/tests"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var commonEntries = []string{
	"delete_inactive_accounts",
	"delete_repo_archives",
	"delete_missing_repos",
	"git_gc_repos",
	"reinit_missing_repos",
	"sync_external_users",
	"repo_health_check",
	"delete_generated_repository_avatars",
	"sync_repo_branches",
	"sync_repo_tags",
}

var sshEntries = []string{
	"resync_all_sshkeys",
	"resync_all_sshprincipals",
}

// Check cron options on /admin, including those that are available conditionally
func testAssertAdminDashboardOptions(t *testing.T, page *HTMLDoc, locale translation.Locale, expectSSH bool) {
	for _, entry := range commonEntries {
		page.AssertSelection(t, page.FindByText("table tr td", locale.TrString(fmt.Sprintf("admin.dashboard.%s", entry))), true)
		page.AssertSelection(t, page.Find(fmt.Sprintf("table tr td button[value='%s']", entry)), true)
	}
	for _, entry := range sshEntries {
		page.AssertSelection(t, page.FindByText("table tr td", locale.TrString(fmt.Sprintf("admin.dashboard.%s", entry))), expectSSH)
		page.AssertSelection(t, page.Find(fmt.Sprintf("table tr td button[value='%s']", entry)), expectSSH)
	}
}

func TestAdminDashboard(t *testing.T) {
	defer tests.PrepareTestEnv(t)()

	session := loginUser(t, "user1")
	locale := translation.NewLocale("en-US")
	url := "/admin"

	t.Run("SSH disabled", func(t *testing.T) {
		defer tests.PrintCurrentTest(t)()
		defer test.MockVariableValue(&setting.SSH.Disabled, true)()

		page := NewHTMLParser(t, session.MakeRequest(t, NewRequest(t, "GET", url), http.StatusOK).Body)
		testAssertAdminDashboardOptions(t, page, locale, false)
	})

	t.Run("SSH enabled, but built-in", func(t *testing.T) {
		defer tests.PrintCurrentTest(t)()
		defer test.MockVariableValue(&setting.SSH.Disabled, false)()
		defer test.MockVariableValue(&setting.SSH.StartBuiltinServer, true)()

		page := NewHTMLParser(t, session.MakeRequest(t, NewRequest(t, "GET", url), http.StatusOK).Body)
		testAssertAdminDashboardOptions(t, page, locale, false)
	})

	t.Run("SSH enabled and external", func(t *testing.T) {
		defer tests.PrintCurrentTest(t)()
		defer test.MockVariableValue(&setting.SSH.Disabled, false)()
		defer test.MockVariableValue(&setting.SSH.StartBuiltinServer, false)()

		page := NewHTMLParser(t, session.MakeRequest(t, NewRequest(t, "GET", url), http.StatusOK).Body)
		testAssertAdminDashboardOptions(t, page, locale, true)
	})

	t.Run("System status", func(t *testing.T) {
		defer tests.PrintCurrentTest(t)()

		// Check data units translations in the System status table
		selector := ".table[hx-get='/admin/system_status'] > dl > dd"
		// ...in English
		page := NewHTMLParser(t, session.MakeRequest(t, NewRequest(t, "GET", url), http.StatusOK).Body)
		assert.Contains(t, page.Find(selector).Text(), "MiB")

		// ...in another language
		lang := session.GetCookie("lang")
		lang.Value = "ru-RU"
		session.SetCookie(lang)

		page = NewHTMLParser(t, session.MakeRequest(t, NewRequest(t, "GET", url), http.StatusOK).Body)
		assert.Contains(t, page.Find(selector).Text(), "МиБ")
	})

	t.Run("forgejo-raft notice", func(t *testing.T) {
		defer tests.PrintCurrentTest(t)()
		defer test.MockVariableValue(&setting.AppVer, "17.0.0-raft.1+gitea-1.22.0")()
		ctx := t.Context()
		set := func(supported []string, releases []updatechecker.RaftRelease) {
			require.NoError(t, system.AppState.Set(ctx, &updatechecker.CheckerState{SupportedVersions: supported}))
			require.NoError(t, system.AppState.Set(ctx, &updatechecker.RaftCheckerState{Releases: releases}))
		}
		defer set(nil, nil)
		notice := func() string {
			page := NewHTMLParser(t, session.MakeRequest(t, NewRequest(t, "GET", url), http.StatusOK).Body)
			return page.Find("#raft-release-notice").Text()
		}
		raft1 := updatechecker.RaftRelease{Version: "17.0.0-raft.1", Upstream: "17.0.0", URL: "https://example.invalid/r1"}
		raft2 := updatechecker.RaftRelease{Version: "17.0.0-raft.2", Upstream: "17.0.0", URL: "https://example.invalid/r2"}
		official := updatechecker.RaftRelease{Version: "17.0.1-raft.1", Upstream: "17.0.1", URL: "https://example.invalid/o1"}

		set([]string{"17.0.0"}, []updatechecker.RaftRelease{raft1})
		assert.Empty(t, notice(), "up to date")

		set([]string{"17.0.0"}, []updatechecker.RaftRelease{raft1, raft2})
		assert.Contains(t, notice(), "forgejo-raft 17.0.0-raft.2 is available")

		set([]string{"17.0.1"}, []updatechecker.RaftRelease{raft1})
		assert.Contains(t, notice(), "The forgejo-raft build of Forgejo 17.0.1 is not available yet")

		set([]string{"17.0.1"}, []updatechecker.RaftRelease{raft1, official})
		assert.Contains(t, notice(), "forgejo-raft 17.0.1-raft.1 is available")
	})
}
