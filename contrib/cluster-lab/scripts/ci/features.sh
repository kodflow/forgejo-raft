#!/usr/bin/env bash
# CI suite C, on the lab under compose.ci.yaml: Forgejo's features through
# the HTTP API, git over HTTP and SSH, with every write read back from
# another node. Prints one `CHECK <name>: PASS|FAIL` per checkpoint and
# exits non-zero when any failed.
set -uo pipefail
source "$(dirname "$0")/lib.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
SUFFIX="$(date +%s)"
USER_NAME="e2e-user-$SUFFIX"
ORG="e2e-org-$SUFFIX"
REPO_NAME=features
REPO="$ORG/$REPO_NAME"
RECEIVER_FILE=/deliveries/deliveries.jsonl

# as_user <command...>: the API calls inside run with the user's token.
as_user() { local TOKEN="$USER_TOKEN"; "$@"; }
# json <node> <path> <jq filter>: a field of a GET, as the admin.
json() { api "$1" GET "$2" | jq -r "$3"; }
deliveries() { dc exec -T receiver cat "$RECEIVER_FILE" 2>/dev/null; }
git_q() { git -c user.name=e2e -c user.email=e2e@lab.invalid "$@"; }
# branch_row <node> <branch> <sha>: the node's API shows the branch at sha.
# Branch rows derive from a push asynchronously, written by the cluster's
# consumer on the leader.
branch_row() { [ "$(json "$1" "/repos/$REPO/branches/$2" .commit.id 2>/dev/null)" = "$3" ]; }
# Git's global configuration for this run only (git lfs install writes it).
export GIT_CONFIG_GLOBAL="$WORK/gitconfig"
: >"$GIT_CONFIG_GLOBAL"

say "C0. Every node ready, every node a voter"
for n in 1 2 3; do poll 180 healthy "$n" || flunk C.node-ready "forgejo-$n"; done
# /api/healthz answers 200 on a learner too: the writes below start once
# every node votes, as suite A waits for it (#199).
poll 120 three_voters || flunk C.three-voters "$(voters_seen)"

say "C1. The web UI"
for n in 1 2 3; do
  code="$(curl -s -o "$WORK/login.html" -w '%{http_code}' -m 15 "$(node_url "$n")/user/login")"
  if [ "$code" = 200 ] && grep -q 'name="user_name"' "$WORK/login.html"; then
    pass "C.web-login-page-node-$n" "HTTP 200"
  else
    flunk "C.web-login-page-node-$n" "HTTP $code"
  fi
done

say "C2. Admin, user, organisation, repository"
if ensure_admin && [ "$(json 2 /user .login)" = "$ADMIN" ]; then
  pass C.admin-created "$ADMIN through the CLI on node 1, its token read on node 2"
else
  flunk C.admin-created
fi
body="$(jq -nc --arg u "$USER_NAME" --arg p "$E2E_USER_PASSWORD" '{username:$u, email:($u+"@lab.invalid"), password:$p, must_change_password:false}')"
if api 1 POST /admin/users "$body" >/dev/null && [ "$(json 2 "/users/$USER_NAME" .login)" = "$USER_NAME" ]; then
  pass C.user-created "$USER_NAME on node 1, read on node 2"
else
  flunk C.user-created
fi
USER_TOKEN="$(fj 1 admin user generate-access-token --username "$USER_NAME" --token-name "e2e-$SUFFIX" --scopes all --raw 2>/dev/null | tail -n 1)"
if [ -n "$USER_TOKEN" ] && [ "$(as_user json 3 /user .login)" = "$USER_NAME" ]; then
  pass C.user-token "made on node 1, accepted on node 3"
else
  flunk C.user-token
fi
if api 2 POST /orgs "{\"username\":\"$ORG\"}" >/dev/null && [ "$(json 3 "/orgs/$ORG" .username)" = "$ORG" ]; then
  pass C.org-created "$ORG on node 2, read on node 3"
else
  flunk C.org-created
fi
if api 3 POST "/orgs/$ORG/repos" "{\"name\":\"$REPO_NAME\",\"default_branch\":\"main\"}" >/dev/null &&
  [ "$(json 1 "/repos/$REPO" .full_name)" = "$REPO" ]; then
  pass C.repo-created "$REPO on node 3, read on node 1"
else
  flunk C.repo-created
fi
if api 1 PUT "/repos/$REPO/collaborators/$USER_NAME" '{"permission":"write"}' >/dev/null &&
  [ "$(api_code 2 GET "/repos/$REPO/collaborators/$USER_NAME")" = 204 ]; then
  pass C.collaborator-added "$USER_NAME writes to $REPO, read on node 2"
else
  flunk C.collaborator-added
fi

say "C3. Push to node 1, clone from nodes 2 and 3"
git init -q -b main "$WORK/src"
printf '# features\n' >"$WORK/src/README.md"
mkdir -p "$WORK/src/dir"
for i in 1 2 3; do head -c 4096 /dev/urandom | base64 >"$WORK/src/dir/file-$i.txt"; done
git -C "$WORK/src" add -A && git_q -C "$WORK/src" commit -qm "first commit"
MAIN="$(git -C "$WORK/src" rev-parse HEAD)"
if git -C "$WORK/src" push -q "$(remote 1 "$REPO")" main 2>"$WORK/err"; then
  pass C.push-node-1 "main at ${MAIN:0:12}"
else
  flunk C.push-node-1 "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"
fi
for n in 2 3; do
  if git clone -q "$(remote "$n" "$REPO")" "$WORK/clone-$n" 2>"$WORK/err" &&
    [ "$(git -C "$WORK/clone-$n" rev-parse HEAD)" = "$MAIN" ] &&
    [ "$(git -C "$WORK/clone-$n" rev-parse 'HEAD^{tree}')" = "$(git -C "$WORK/src" rev-parse 'HEAD^{tree}')" ] &&
    git -C "$WORK/clone-$n" fsck --strict --no-progress >/dev/null 2>&1; then
    pass "C.clone-node-$n-identical" "commit and tree as pushed, fsck clean"
  else
    flunk "C.clone-node-$n-identical" "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"
  fi
done

if poll 30 branch_row 2 main "$MAIN"; then
  pass C.branch-row-after-push "node 2's API shows main ${POLL_TOOK}s after the push to node 1"
else
  flunk C.branch-row-after-push "node 2's API: $(json 2 "/repos/$REPO/branches/main" .commit.id 2>&1 | tail -n 1)"
fi

say "C4. Branch, tag, branch deletion"
if api 2 POST "/repos/$REPO/branches" '{"new_branch_name":"api-branch","old_branch_name":"main"}' >/dev/null &&
  poll 10 serves 3 "$REPO" refs/heads/api-branch "$MAIN" &&
  poll 30 branch_row 1 api-branch "$MAIN"; then
  pass C.branch-created "through the API on node 2, served by node 3 over git and node 1 over the API"
else
  flunk C.branch-created
fi
if api 3 POST "/repos/$REPO/tags" "{\"tag_name\":\"v1.0.0\",\"target\":\"main\",\"message\":\"release 1\"}" >/dev/null &&
  poll 10 bash -c "[ -n \"\$(git ls-remote '$(remote 1 "$REPO")' refs/tags/v1.0.0)\" ]" &&
  [ "$(json 2 "/repos/$REPO/tags/v1.0.0" .commit.sha)" = "$MAIN" ]; then
  pass C.tag-created "annotated v1.0.0 on node 3, served by node 1, read on node 2"
else
  flunk C.tag-created
fi
if [ "$(api_code 1 DELETE "/repos/$REPO/branches/api-branch")" = 204 ] &&
  poll 10 bash -c "[ -z \"\$(git ls-remote '$(remote 2 "$REPO")' refs/heads/api-branch)\" ]" &&
  poll 30 bash -c "[ \"\$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: token $TOKEN' '$(node_url 3)/api/v1/repos/$REPO/branches/api-branch')\" = 404 ]"; then
  pass C.branch-deleted "on node 1, gone from node 2 over git and node 3 over the API"
else
  flunk C.branch-deleted
fi

say "C5. Issue, comment, label"
ISSUE="$(api 2 POST "/repos/$REPO/issues" '{"title":"e2e issue","body":"opened on node 2"}' | jq -r .number)"
if [ -n "$ISSUE" ] && [ "$ISSUE" != null ] && [ "$(json 3 "/repos/$REPO/issues/$ISSUE" .title)" = "e2e issue" ]; then
  pass C.issue-created "#$ISSUE on node 2, read on node 3"
else
  flunk C.issue-created
fi
if api 3 POST "/repos/$REPO/issues/$ISSUE/comments" '{"body":"comment from node 3"}' >/dev/null &&
  [ "$(json 1 "/repos/$REPO/issues/$ISSUE/comments" '.[0].body')" = "comment from node 3" ]; then
  pass C.comment-created "on node 3, read on node 1"
else
  flunk C.comment-created
fi
LABEL="$(api 1 POST "/repos/$REPO/labels" '{"name":"e2e","color":"#00aabb"}' | jq -r .id)"
if [ -n "$LABEL" ] && [ "$LABEL" != null ] &&
  api 2 POST "/repos/$REPO/issues/$ISSUE/labels" "{\"labels\":[$LABEL]}" >/dev/null &&
  [ "$(json 3 "/repos/$REPO/issues/$ISSUE" '[.labels[].name] | join(",")')" = e2e ]; then
  pass C.label-applied "made on node 1, applied on node 2, read on node 3"
else
  flunk C.label-applied
fi

say "C6. Webhook, exactly once"
hook="$(jq -nc '{type:"forgejo", active:true, events:["push","issues"], config:{url:"http://10.89.0.85:8080/hook", content_type:"json"}}')"
HOOK_ID="$(api 1 POST "/repos/$REPO/hooks" "$hook" | jq -r .id)"
if [ -n "$HOOK_ID" ] && [ "$HOOK_ID" != null ] && [ "$(json 2 "/repos/$REPO/hooks/$HOOK_ID" .active)" = true ]; then
  pass C.webhook-created "on node 1, read on node 2"
else
  flunk C.webhook-created
fi
git -C "$WORK/src" checkout -q -b hook-push
echo hook >"$WORK/src/hook.txt"
git -C "$WORK/src" add hook.txt && git_q -C "$WORK/src" commit -qm "hook push"
HOOK_SHA="$(git -C "$WORK/src" rev-parse HEAD)"
git -C "$WORK/src" push -q "$(remote 2 "$REPO")" hook-push 2>"$WORK/err" || flunk C.webhook-push "push to node 2 failed"
HOOK_ISSUE="$(api 3 POST "/repos/$REPO/issues" '{"title":"hook issue"}' | jq -r .number)"
push_delivered() { [ "$(deliveries | jq -c --arg s "$HOOK_SHA" 'select(.event=="push" and .ref=="refs/heads/hook-push" and .after==$s)' | grep -c .)" -ge 1 ]; }
issue_delivered() { [ "$(deliveries | jq -c --argjson n "$HOOK_ISSUE" 'select(.event=="issues" and .action=="opened" and .number==$n)' | grep -c .)" -ge 1 ]; }
poll 60 push_delivered
poll 60 issue_delivered
# Long enough for a second node or a retry to deliver again.
sleep 20
n_push="$(deliveries | jq -c --arg s "$HOOK_SHA" 'select(.event=="push" and .ref=="refs/heads/hook-push" and .after==$s)' | grep -c .)"
n_issue="$(deliveries | jq -c --argjson n "$HOOK_ISSUE" 'select(.event=="issues" and .action=="opened" and .number==$n)' | grep -c .)"
n_dup="$(deliveries | jq -r .delivery | sort | uniq -d | grep -c .)"
if [ "$n_push" = 1 ] && [ "$n_dup" = 0 ]; then
  pass C.webhook-push-exactly-once "push to node 2 delivered once"
else
  flunk C.webhook-push-exactly-once "$n_push deliveries, $n_dup duplicate ids"
fi
if [ "$n_issue" = 1 ]; then
  pass C.webhook-issue-exactly-once "issue #$HOOK_ISSUE opened on node 3, delivered once"
else
  flunk C.webhook-issue-exactly-once "$n_issue deliveries"
fi

say "C7. Pull request, review, merge"
git -C "$WORK/src" checkout -q main
git -C "$WORK/src" checkout -q -b feature
echo feature >"$WORK/src/feature.txt"
git -C "$WORK/src" add feature.txt && git_q -C "$WORK/src" commit -qm "a feature"
FEATURE="$(git -C "$WORK/src" rev-parse HEAD)"
if git -C "$WORK/src" push -q "http://$USER_NAME:$USER_TOKEN@127.0.0.1:3001/$REPO.git" feature 2>"$WORK/err" &&
  poll 30 branch_row 2 feature "$FEATURE"; then
  pass C.pr-branch-pushed "by $USER_NAME to node 1, its branch row on node 2 after ${POLL_TOOK}s"
else
  flunk C.pr-branch-pushed "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"
fi
PR="$(as_user api 2 POST "/repos/$REPO/pulls" '{"head":"feature","base":"main","title":"e2e pull request"}' | jq -r .number)"
if [ -n "$PR" ] && [ "$PR" != null ] && poll 30 bash -c "[ \"\$(curl -sf -H 'Authorization: token $TOKEN' '$(node_url 3)/api/v1/repos/$REPO/pulls/$PR' | jq -r .mergeable)\" = true ]"; then
  pass C.pr-opened "#$PR by $USER_NAME on node 2, mergeable on node 3"
else
  flunk C.pr-opened
fi
if api 3 POST "/repos/$REPO/pulls/$PR/reviews" '{"event":"APPROVED","body":"approved from node 3"}' >/dev/null &&
  [ "$(as_user json 1 "/repos/$REPO/pulls/$PR/reviews" '[.[] | select(.state=="APPROVED")] | length')" = 1 ]; then
  pass C.pr-approved "by $ADMIN on node 3, read on node 1"
else
  flunk C.pr-approved
fi
if as_user api 1 POST "/repos/$REPO/pulls/$PR/merge" '{"Do":"merge"}' >/dev/null; then
  MERGE="$(json 1 "/repos/$REPO/pulls/$PR" .merge_commit_sha)"
  ok_all=1
  for n in 1 2 3; do
    [ "$(json "$n" "/repos/$REPO/pulls/$PR" .merged)" = true ] || ok_all=0
    poll 15 serves "$n" "$REPO" refs/heads/main "$MERGE" || ok_all=0
  done
  git -C "$WORK/clone-2" pull -q --ff-only 2>/dev/null
  if [ "$ok_all" = 1 ] && [ -f "$WORK/clone-2/feature.txt" ]; then
    pass C.pr-merged-everywhere "merged on node 1 at ${MERGE:0:12}, merged and main at it on nodes 1, 2 and 3"
  else
    flunk C.pr-merged-everywhere "merge commit ${MERGE:0:12}: $(for n in 1 2 3; do printf 'node %s main %s merged %s; ' "$n" "$(ref_sha "$n" "$REPO" refs/heads/main | cut -c1-12)" "$(json "$n" "/repos/$REPO/pulls/$PR" .merged)"; done)"
  fi
else
  flunk C.pr-merged-everywhere "the merge on node 1 failed"
fi

say "C8. LFS"
if command -v git-lfs >/dev/null 2>&1; then
  git lfs install --skip-repo >/dev/null
  git -C "$WORK/src" checkout -q main && git -C "$WORK/src" pull -q "$(remote 1 "$REPO")" main 2>/dev/null
  git -C "$WORK/src" lfs install --local >/dev/null
  git -C "$WORK/src" lfs track '*.bin' >/dev/null
  head -c 1048576 /dev/urandom >"$WORK/src/blob.bin"
  LFS_SUM="$(sha256sum "$WORK/src/blob.bin" | cut -d' ' -f1)"
  git -C "$WORK/src" add .gitattributes blob.bin && git_q -C "$WORK/src" commit -qm "an LFS object"
  if git -C "$WORK/src" push -q "$(remote 1 "$REPO")" main 2>"$WORK/err"; then
    pass C.lfs-push-node-1 "1 MiB object $LFS_SUM"
  else
    flunk C.lfs-push-node-1 "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"
  fi
  for n in 2 3; do
    if git clone -q "$(remote "$n" "$REPO")" "$WORK/lfs-$n" 2>"$WORK/err" &&
      [ "$(sha256sum "$WORK/lfs-$n/blob.bin" 2>/dev/null | cut -d' ' -f1)" = "$LFS_SUM" ]; then
      pass "C.lfs-fetch-node-$n" "the object's content, not its pointer"
    else
      flunk "C.lfs-fetch-node-$n" "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"
    fi
  done
else
  flunk C.lfs "git-lfs is not installed on the runner"
fi

say "C9. Repository archive"
HEAD_NOW="$(ref_sha 1 "$REPO" refs/heads/main)"
reference=""
for n in 1 2 3; do
  code="$(curl -s -o "$WORK/archive-$n.tar.gz" -w '%{http_code}' -m 60 -H "Authorization: token $TOKEN" \
    "$(node_url "$n")/api/v1/repos/$REPO/archive/main.tar.gz")"
  # Names and content, not the bytes: each node runs its own git archive.
  got="$({ tar -tzf "$WORK/archive-$n.tar.gz" | sort; tar -xzOf "$WORK/archive-$n.tar.gz"; } 2>/dev/null | sha256sum | cut -d' ' -f1)"
  [ -n "$reference" ] || reference="$got"
  if [ "$code" = 200 ] && tar -tzf "$WORK/archive-$n.tar.gz" 2>/dev/null | grep -q '/README.md$' && [ "$got" = "$reference" ]; then
    pass "C.archive-node-$n" "main.tar.gz of ${HEAD_NOW:0:12}, the same files and content as node 1's"
  else
    flunk "C.archive-node-$n" "HTTP $code, digest $got, node 1's $reference"
  fi
done

say "C10. git over SSH"
ssh-keygen -q -t ed25519 -N '' -C e2e -f "$WORK/id_e2e"
KEY_ID="$(api 1 POST /user/keys "$(jq -nc --arg k "$(cat "$WORK/id_e2e.pub")" '{title:"e2e", key:$k}')" | jq -r .id)"
export GIT_SSH_COMMAND="ssh -i $WORK/id_e2e -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
if [ -n "$KEY_ID" ] && [ "$KEY_ID" != null ] && git clone -q "ssh://git@127.0.0.1:2222/$REPO.git" "$WORK/ssh-2" 2>"$WORK/err" &&
  [ "$(git -C "$WORK/ssh-2" rev-parse HEAD)" = "$HEAD_NOW" ]; then
  pass C.ssh-clone-node-2 "key added on node 1, clone over SSH from node 2 at ${HEAD_NOW:0:12}"
else
  flunk C.ssh-clone-node-2 "key $KEY_ID, HEAD $(git -C "$WORK/ssh-2" rev-parse --abbrev-ref HEAD 2>/dev/null) at $(git -C "$WORK/ssh-2" rev-parse HEAD 2>/dev/null | cut -c1-12), main on node 1 at ${HEAD_NOW:0:12}: $(tail -n 2 "$WORK/err" | tr '\n' ' ')"
fi
if [ -d "$WORK/ssh-2" ]; then
  git -C "$WORK/ssh-2" checkout -q -b ssh-push
  echo ssh >"$WORK/ssh-2/ssh.txt"
  git -C "$WORK/ssh-2" add ssh.txt && git_q -C "$WORK/ssh-2" commit -qm "over ssh"
  SSH_SHA="$(git -C "$WORK/ssh-2" rev-parse HEAD)"
  if git -C "$WORK/ssh-2" push -q "ssh://git@127.0.0.1:2223/$REPO.git" ssh-push 2>"$WORK/err" &&
    poll 10 serves 1 "$REPO" refs/heads/ssh-push "$SSH_SHA"; then
    pass C.ssh-push-node-3 "pushed over SSH to node 3, served over HTTP by node 1"
  else
    flunk C.ssh-push-node-3 "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"
  fi
fi
unset GIT_SSH_COMMAND

say "C11. The same refs everywhere"
same_refs() { [ "$(all_refs 1 "$REPO")" = "$(all_refs 2 "$REPO")" ] && [ "$(all_refs 2 "$REPO")" = "$(all_refs 3 "$REPO")" ]; }
if poll 30 same_refs; then
  pass C.same-refs-on-every-node "$(all_refs 1 "$REPO" | grep -c .) refs"
else
  flunk C.same-refs-on-every-node
fi

# Behind a load balancer the next request of a client may reach another
# node than the one that handed it its token: every node verifies the
# tokens any node signs, with the same [oauth2] JWT_SECRET and [server]
# LFS_JWT_SECRET (#285).
say "C12. A token one node signs, honoured by the others"
REGISTRY_TOKEN="$(curl -sf -m 15 -u "$ADMIN:$TOKEN" "$(node_url 1)/v2/token" | jq -r '.token // empty')"
# What the checks print of a token: never the token.
said() { if [ -n "$1" ]; then echo "handed out"; else echo "missing${2:+: $2}"; fi; }
for n in 2 3; do
  code="$(curl -s -o /dev/null -w '%{http_code}' -m 15 -H "Authorization: Bearer $REGISTRY_TOKEN" "$(node_url "$n")/v2/")"
  if [ -n "$REGISTRY_TOKEN" ] && [ "$code" = 200 ]; then
    pass "C.registry-token-node-$n" "the container registry's token from node 1, HTTP 200 on node $n"
  else
    flunk "C.registry-token-node-$n" "token from node 1 $(said "$REGISTRY_TOKEN"), HTTP $code on node $n"
  fi
done
if [ -n "${LFS_SUM:-}" ] && [ -n "${KEY_ID:-}" ] && [ "$KEY_ID" != null ]; then
  # git-lfs-authenticate over SSH: the token git lfs sends with its HTTP
  # requests, signed with the LFS key of the node that answered.
  LFS_AUTH="$(ssh -i "$WORK/id_e2e" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
    -p 2221 git@127.0.0.1 git-lfs-authenticate "$REPO" download 2>"$WORK/err" | jq -r '.header.Authorization // empty')"
  batch="$(jq -nc --arg oid "$LFS_SUM" '{operation: "download", transfers: ["basic"], objects: [{oid: $oid, size: 1048576}]}')"
  for n in 2 3; do
    code="$(curl -s -o "$WORK/lfs-batch-$n.json" -w '%{http_code}' -m 30 -X POST -H "Authorization: $LFS_AUTH" \
      -H 'Accept: application/vnd.git-lfs+json' -H 'Content-Type: application/vnd.git-lfs+json' --data "$batch" \
      "$(node_url "$n")/$REPO.git/info/lfs/objects/batch")"
    href="$(jq -r '.objects[0].actions.download.href // empty' "$WORK/lfs-batch-$n.json" 2>/dev/null)"
    if [ -n "$LFS_AUTH" ] && [ "$code" = 200 ] && [ -n "$href" ]; then
      pass "C.lfs-token-node-$n" "the LFS token node 1 signed over SSH, the object's download on node $n"
    else
      flunk "C.lfs-token-node-$n" "token from node 1 $(said "$LFS_AUTH" "$(tail -n 2 "$WORK/err" | tr '\n' ' ')"), HTTP $code on node $n $(jq -r '.message // .objects[0].error.message // empty' "$WORK/lfs-batch-$n.json" 2>/dev/null)"
    fi
  done
else
  flunk C.lfs-token "no LFS object (C8) or no SSH key (C10) to ask a token for"
fi

finish
