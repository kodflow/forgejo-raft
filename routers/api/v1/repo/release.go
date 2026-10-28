// Copyright 2016 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package repo

import (
	"errors"
	"fmt"
	"net/http"

	"forgejo.org/models"
	"forgejo.org/models/db"
	"forgejo.org/models/perm"
	repo_model "forgejo.org/models/repo"
	"forgejo.org/models/unit"
	"forgejo.org/modules/clusterhook"
	"forgejo.org/modules/git"
	api "forgejo.org/modules/structs"
	"forgejo.org/modules/web"
	"forgejo.org/routers/api/v1/utils"
	"forgejo.org/services/context"
	"forgejo.org/services/convert"
	release_service "forgejo.org/services/release"
)

// GetRelease get a single release of a repository
func GetRelease(ctx *context.APIContext) {
	// swagger:operation GET /repos/{owner}/{repo}/releases/{id} repository repoGetRelease
	// ---
	// summary: Get a release
	// produces:
	// - application/json
	// parameters:
	// - name: owner
	//   in: path
	//   description: owner of the repo
	//   type: string
	//   required: true
	// - name: repo
	//   in: path
	//   description: name of the repo
	//   type: string
	//   required: true
	// - name: id
	//   in: path
	//   description: id of the release to get
	//   type: integer
	//   format: int64
	//   required: true
	// responses:
	//   "200":
	//     "$ref": "#/responses/Release"
	//   "404":
	//     "$ref": "#/responses/notFound"

	id := ctx.ParamsInt64(":id")
	release, err := repo_model.GetReleaseForRepoByID(ctx, ctx.Repo().Repository.ID, id)
	if err != nil && !repo_model.IsErrReleaseNotExist(err) {
		ctx.Error(http.StatusInternalServerError, "GetReleaseForRepoByID", err)
		return
	}
	if err != nil && repo_model.IsErrReleaseNotExist(err) || release.IsTag {
		ctx.NotFound()
		return
	}
	if release.IsDraft && !ctx.Repo().CanWrite(unit.TypeReleases) {
		ctx.NotFound()
		return
	}
	if err := release.LoadAttributes(ctx); err != nil {
		ctx.Error(http.StatusInternalServerError, "LoadAttributes", err)
		return
	}
	ctx.JSON(http.StatusOK, convert.ToAPIRelease(ctx, ctx.Repo().Repository, release, ctx.AcceptsGithubResponse()))
}

// GetLatestRelease gets the most recent non-prerelease, non-draft release of a repository, sorted by created_at
func GetLatestRelease(ctx *context.APIContext) {
	// swagger:operation GET /repos/{owner}/{repo}/releases/latest repository repoGetLatestRelease
	// ---
	// summary: Gets the most recent non-prerelease, non-draft release of a repository, sorted by created_at
	// produces:
	// - application/json
	// parameters:
	// - name: owner
	//   in: path
	//   description: owner of the repo
	//   type: string
	//   required: true
	// - name: repo
	//   in: path
	//   description: name of the repo
	//   type: string
	//   required: true
	// responses:
	//   "200":
	//     "$ref": "#/responses/Release"
	//   "404":
	//     "$ref": "#/responses/notFound"
	release, err := repo_model.GetLatestReleaseByRepoID(ctx, ctx.Repo().Repository.ID)
	if err != nil && !repo_model.IsErrReleaseNotExist(err) {
		ctx.Error(http.StatusInternalServerError, "GetLatestRelease", err)
		return
	}
	if err != nil && repo_model.IsErrReleaseNotExist(err) ||
		release.IsTag || release.RepoID != ctx.Repo().Repository.ID {
		ctx.NotFound()
		return
	}
	if err := release.LoadAttributes(ctx); err != nil {
		ctx.Error(http.StatusInternalServerError, "LoadAttributes", err)
		return
	}
	ctx.JSON(http.StatusOK, convert.ToAPIRelease(ctx, ctx.Repo().Repository, release, ctx.AcceptsGithubResponse()))
}

// ListReleases list a repository's releases
func ListReleases(ctx *context.APIContext) {
	// swagger:operation GET /repos/{owner}/{repo}/releases repository repoListReleases
	// ---
	// summary: List a repo's releases
	// produces:
	// - application/json
	// parameters:
	// - name: owner
	//   in: path
	//   description: owner of the repo
	//   type: string
	//   required: true
	// - name: repo
	//   in: path
	//   description: name of the repo
	//   type: string
	//   required: true
	// - name: draft
	//   in: query
	//   description: filter (exclude / include) drafts, if you dont have repo write access none will show
	//   type: boolean
	// - name: pre-release
	//   in: query
	//   description: filter (exclude / include) pre-releases
	//   type: boolean
	// - name: q
	//   in: query
	//   description: Search string
	//   type: string
	// - name: page
	//   in: query
	//   description: page number of results to return (1-based)
	//   type: integer
	// - name: limit
	//   in: query
	//   description: page size of results
	//   type: integer
	// responses:
	//   "200":
	//     "$ref": "#/responses/ReleaseList"
	//   "404":
	//     "$ref": "#/responses/notFound"
	listOptions := utils.GetListOptions(ctx)

	opts := repo_model.FindReleasesOptions{
		ListOptions:   listOptions,
		IncludeDrafts: ctx.Repo().AccessMode >= perm.AccessModeWrite || ctx.Repo().UnitAccessMode(unit.TypeReleases) >= perm.AccessModeWrite,
		IncludeTags:   false,
		IsDraft:       ctx.FormOptionalBool("draft"),
		IsPreRelease:  ctx.FormOptionalBool("pre-release"),
		RepoID:        ctx.Repo().Repository.ID,
		Keyword:       ctx.FormTrim("q"),
	}

	releases, err := db.Find[repo_model.Release](ctx, opts)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "GetReleasesByRepoID", err)
		return
	}
	rels := make([]*api.Release, len(releases))
	for i, release := range releases {
		if err := release.LoadAttributes(ctx); err != nil {
			ctx.Error(http.StatusInternalServerError, "LoadAttributes", err)
			return
		}
		rels[i] = convert.ToAPIRelease(ctx, ctx.Repo().Repository, release, ctx.AcceptsGithubResponse())
	}

	filteredCount, err := db.Count[repo_model.Release](ctx, opts)
	if err != nil {
		ctx.InternalServerError(err)
		return
	}

	ctx.SetLinkHeader(int(filteredCount), listOptions.PageSize)
	ctx.SetTotalCountHeader(filteredCount)
	ctx.JSON(http.StatusOK, rels)
}

// releaseRowsBehind answers 503 with the reason when, in a cluster, this
// node does not serve the repository yet at the cluster's commit index, or
// the cluster's log consumer has not derived the SQL of its last writes
// (an event of the repository parked included), and reports whether it
// did. A push's derivation rewrites the release rows of its tags: a
// release or a tag written from a row read before it, and before the
// tag's state it derives, would be overwritten by it, or written on a row
// it removed, and the push's own work would not write it again (#298).
// It is asked before the row is read, which the write then builds on;
// nothing is written while it refuses.
func releaseRowsBehind(ctx *context.APIContext) bool {
	repo := ctx.Repo().Repository
	return clusterBehind(ctx, clusterhook.RequireDerived(ctx, clusterhook.RepoKey(repo.OwnerName, repo.Name, false)))
}

// CreateRelease create a release
func CreateRelease(ctx *context.APIContext) {
	// swagger:operation POST /repos/{owner}/{repo}/releases repository repoCreateRelease
	// ---
	// summary: Create a release
	// consumes:
	// - application/json
	// produces:
	// - application/json
	// parameters:
	// - name: owner
	//   in: path
	//   description: owner of the repo
	//   type: string
	//   required: true
	// - name: repo
	//   in: path
	//   description: name of the repo
	//   type: string
	//   required: true
	// - name: body
	//   in: body
	//   schema:
	//     "$ref": "#/definitions/CreateReleaseOption"
	// responses:
	//   "201":
	//     "$ref": "#/responses/Release"
	//   "404":
	//     "$ref": "#/responses/notFound"
	//   "409":
	//     "$ref": "#/responses/error"
	//   "422":
	//     "$ref": "#/responses/validationError"

	form := web.GetForm(ctx).(*api.CreateReleaseOption)
	// Before the tag's row is read: the release is built on it.
	if releaseRowsBehind(ctx) {
		return
	}
	if ctx.Repo().Repository.IsEmpty {
		ctx.Error(http.StatusUnprocessableEntity, "RepoIsEmpty", errors.New("repo is empty"))
		return
	}
	title := form.Title
	if len(title) == 0 {
		title = form.TagName
	}
	rel, err := repo_model.GetRelease(ctx, ctx.Repo().Repository.ID, form.TagName)
	if err != nil {
		if !repo_model.IsErrReleaseNotExist(err) {
			ctx.Error(http.StatusInternalServerError, "GetRelease", err)
			return
		}
		// If target is not provided use default branch
		if len(form.Target) == 0 {
			form.Target = ctx.Repo().Repository.DefaultBranch
		}
		rel = &repo_model.Release{
			RepoID:           ctx.Repo().Repository.ID,
			PublisherID:      ctx.Doer().ID,
			Publisher:        ctx.Doer(),
			TagName:          form.TagName,
			Target:           form.Target,
			Title:            title,
			Note:             form.Note,
			IsDraft:          form.IsDraft,
			IsPrerelease:     form.IsPrerelease,
			HideArchiveLinks: form.HideArchiveLinks,
			IsTag:            false,
			Repo:             ctx.Repo().Repository,
		}
		if err := release_service.CreateRelease(ctx.Repo().GitRepo, rel, "", nil); err != nil {
			if clusterBehind(ctx, err) {
				return
			}
			if repo_model.IsErrReleaseAlreadyExist(err) || models.IsErrTagAlreadyExists(err) {
				ctx.Error(http.StatusConflict, "ReleaseAlreadyExist", err)
			} else if models.IsErrProtectedTagName(err) {
				ctx.Error(http.StatusUnprocessableEntity, "ProtectedTagName", err)
			} else if git.IsErrNotExist(err) {
				ctx.Error(http.StatusNotFound, "ErrNotExist", fmt.Errorf("target \"%v\" not found: %w", rel.Target, err))
			} else {
				ctx.Error(http.StatusInternalServerError, "CreateRelease", err)
			}
			return
		}
	} else {
		if !rel.IsTag {
			ctx.Error(http.StatusConflict, "GetRelease", "Release has no Tag")
			return
		}

		rel.Title = title
		rel.Note = form.Note
		rel.IsDraft = form.IsDraft
		rel.IsPrerelease = form.IsPrerelease
		rel.HideArchiveLinks = form.HideArchiveLinks
		rel.PublisherID = ctx.Doer().ID
		rel.IsTag = false
		rel.Repo = ctx.Repo().Repository
		rel.Publisher = ctx.Doer()
		rel.Target = form.Target

		if err = release_service.UpdateRelease(ctx, ctx.Doer(), ctx.Repo().GitRepo, rel, true, nil); err != nil {
			if clusterBehind(ctx, err) {
				return
			}
			if models.IsErrTagAlreadyExists(err) {
				ctx.Error(http.StatusConflict, "TagAlreadyExists", err)
				return
			}
			ctx.Error(http.StatusInternalServerError, "UpdateRelease", err)
			return
		}
	}
	ctx.JSON(http.StatusCreated, convert.ToAPIRelease(ctx, ctx.Repo().Repository, rel, ctx.AcceptsGithubResponse()))
}

// EditRelease edit a release
func EditRelease(ctx *context.APIContext) {
	// swagger:operation PATCH /repos/{owner}/{repo}/releases/{id} repository repoEditRelease
	// ---
	// summary: Update a release
	// consumes:
	// - application/json
	// produces:
	// - application/json
	// parameters:
	// - name: owner
	//   in: path
	//   description: owner of the repo
	//   type: string
	//   required: true
	// - name: repo
	//   in: path
	//   description: name of the repo
	//   type: string
	//   required: true
	// - name: id
	//   in: path
	//   description: id of the release to edit
	//   type: integer
	//   format: int64
	//   required: true
	// - name: body
	//   in: body
	//   schema:
	//     "$ref": "#/definitions/EditReleaseOption"
	// responses:
	//   "200":
	//     "$ref": "#/responses/Release"
	//   "404":
	//     "$ref": "#/responses/notFound"

	form := web.GetForm(ctx).(*api.EditReleaseOption)
	id := ctx.ParamsInt64(":id")
	// Before the release is read: it is written back whole.
	if releaseRowsBehind(ctx) {
		return
	}
	rel, err := repo_model.GetReleaseForRepoByID(ctx, ctx.Repo().Repository.ID, id)
	if err != nil && !repo_model.IsErrReleaseNotExist(err) {
		ctx.Error(http.StatusInternalServerError, "GetReleaseForRepoByID", err)
		return
	}
	if err != nil && repo_model.IsErrReleaseNotExist(err) || rel.IsTag {
		ctx.NotFound()
		return
	}

	if len(form.TagName) > 0 {
		rel.TagName = form.TagName
	}
	if len(form.Target) > 0 {
		rel.Target = form.Target
	}
	if len(form.Title) > 0 {
		rel.Title = form.Title
	}
	if len(form.Note) > 0 {
		rel.Note = form.Note
	}
	if form.IsDraft != nil {
		rel.IsDraft = *form.IsDraft
	}
	if form.IsPrerelease != nil {
		rel.IsPrerelease = *form.IsPrerelease
	}
	if form.HideArchiveLinks != nil {
		rel.HideArchiveLinks = *form.HideArchiveLinks
	}
	if err := release_service.UpdateRelease(ctx, ctx.Doer(), ctx.Repo().GitRepo, rel, false, nil); err != nil {
		if clusterBehind(ctx, err) {
			return
		}
		if models.IsErrTagAlreadyExists(err) {
			ctx.Error(http.StatusConflict, "TagAlreadyExists", err)
			return
		}
		ctx.Error(http.StatusInternalServerError, "UpdateRelease", err)
		return
	}

	// reload data from database
	rel, err = repo_model.GetReleaseByID(ctx, id)
	if err != nil {
		ctx.Error(http.StatusInternalServerError, "GetReleaseByID", err)
		return
	}
	if err := rel.LoadAttributes(ctx); err != nil {
		ctx.Error(http.StatusInternalServerError, "LoadAttributes", err)
		return
	}
	ctx.JSON(http.StatusOK, convert.ToAPIRelease(ctx, ctx.Repo().Repository, rel, ctx.AcceptsGithubResponse()))
}

// DeleteRelease delete a release from a repository
func DeleteRelease(ctx *context.APIContext) {
	// swagger:operation DELETE /repos/{owner}/{repo}/releases/{id} repository repoDeleteRelease
	// ---
	// summary: Delete a release
	// parameters:
	// - name: owner
	//   in: path
	//   description: owner of the repo
	//   type: string
	//   required: true
	// - name: repo
	//   in: path
	//   description: name of the repo
	//   type: string
	//   required: true
	// - name: id
	//   in: path
	//   description: id of the release to delete
	//   type: integer
	//   format: int64
	//   required: true
	// responses:
	//   "204":
	//     "$ref": "#/responses/empty"
	//   "404":
	//     "$ref": "#/responses/notFound"
	//   "422":
	//     "$ref": "#/responses/validationError"

	id := ctx.ParamsInt64(":id")
	rel, err := repo_model.GetReleaseForRepoByID(ctx, ctx.Repo().Repository.ID, id)
	if err != nil && !repo_model.IsErrReleaseNotExist(err) {
		ctx.Error(http.StatusInternalServerError, "GetReleaseForRepoByID", err)
		return
	}
	if err != nil && repo_model.IsErrReleaseNotExist(err) || rel.IsTag {
		ctx.NotFound()
		return
	}
	if err := release_service.DeleteReleaseByID(ctx, ctx.Repo().Repository, rel, ctx.Doer(), false); err != nil {
		if models.IsErrProtectedTagName(err) {
			ctx.Error(http.StatusUnprocessableEntity, "delTag", "user not allowed to delete protected tag")
			return
		}
		ctx.Error(http.StatusInternalServerError, "DeleteReleaseByID", err)
		return
	}
	ctx.Status(http.StatusNoContent)
}
