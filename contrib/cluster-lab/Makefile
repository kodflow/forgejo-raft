# Forgejo cluster lab. `make help` lists the targets.
SHELL := /bin/bash
export COMPOSE_PROJECT_NAME ?= forgejo-cluster-lab
DC := docker compose --project-directory $(CURDIR) -f $(CURDIR)/compose.yaml
# The lab's secrets, random, in the git-ignored .env, before compose reads it.
# It runs at every parse of this Makefile, `make help` included, on purpose:
# compose refuses every command (down too) without them. It only adds what
# is missing and never changes a value.
_ := $(shell $(CURDIR)/scripts/secrets.sh)
DB := consul-1 consul-2 consul-3 pg-1 pg-2 pg-3 haproxy

.PHONY: help build build-db up up-db down reset bootstrap status failover chaos chaos-e2e heal e2e witnesses recover-e2e vault-renew-e2e nomad-e2e secret

help: ## This list
	@grep -hE '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

build-db: ## Build the Patroni image
	$(DC) build pg-1

FORGEJO_SRC ?= $(abspath $(CURDIR)/../../../forgejo-raft-cluster)
# Empty by default: the tree's go.mod pins a published SDK. Set it to an SDK
# worktree to build against unpublished SDK changes through go.work.
SDK_SRC ?=
export FORGEJO_SRC SDK_SRC
export FORGEJO_DOCKERFILE := $(CURDIR)/.build/Dockerfile.forgejo

build: build-db ## Build the Patroni and Forgejo images (Forgejo: --target forgejo)
	forgejo/dockerfile.sh "$(FORGEJO_SRC)" "$(CURDIR)/.build/Dockerfile.forgejo" $(if $(SDK_SRC),sdk)
	$(DC) build forgejo-1
	$(DC) build nomad

up-db: build-db ## Consul, Patroni and HAProxy only
	$(DC) up -d --wait $(DB)

up: build up-db ## The whole lab (Forgejo healthy only once bootstrapped)
	$(DC) up -d --no-build

down: ## Stop the lab, keep the volumes
	$(DC) down

reset: ## Stop the lab and wipe every volume (PKI included); the secrets in .env stay
	$(DC) down -v --remove-orphans
	@# What .env learnt from the volumes goes with them (witness keys, renewer).
	@sed -i.bak -E '/^(WITNESS_URLS|WITNESS_KEYS|WITNESS_OVERRIDE_SHA256|RENEW_VAULT_ADDR|RENEW_VAULT_TOKEN_FILE)=/d' .env && rm -f .env.bak

bootstrap: ## Found the Forgejo cluster (init then bootstrap), wait for it
	scripts/bootstrap.sh
	$(DC) up -d --wait --no-build $(DB) forgejo-1 forgejo-2 forgejo-3

status: ## Consul, Patroni, HAProxy and Forgejo at a glance
	@echo "== Consul"; $(DC) exec -T consul-1 consul members || true
	@echo "== Patroni"; $(DC) exec -T pg-1 patronictl -c /etc/patroni/patroni.yml list 2>/dev/null \
	  || $(DC) exec -T pg-2 patronictl -c /etc/patroni/patroni.yml list || true
	@echo "== HAProxy :5000 ->"; source scripts/lib.sh && haproxy_primary || true
	@echo "== Forgejo"; for n in 1 2 3; do printf 'forgejo-%s healthz ' $$n; \
	  curl -s -o /dev/null -w '%{http_code}  ' http://127.0.0.1:300$$n/api/healthz || printf 'down  '; \
	  curl -s http://127.0.0.1:300$$n/metrics | grep -E '^forgejo_cluster_(role|term|applied_index|failure_tolerance)[ {]' | tr '\n' ' '; echo; done

failover: ## Prove a Patroni failover behind HAProxy
	scripts/pg-failover.sh

chaos: ## SCENARIO="forgejo 1" | "pg-primary" | "partition 2"
	scripts/chaos.sh $(SCENARIO)

heal: ## Undo every chaos scenario
	scripts/chaos.sh heal

e2e: ## End to end on a lab that is up and bootstrapped
	scripts/e2e.sh

chaos-e2e: ## Partitions and restarts on a lab that is up and bootstrapped
	scripts/chaos-e2e.sh

witnesses: ## Start the two Recover witnesses and turn on the boot fence
	scripts/witnesses.sh

recover-e2e: ## Lose the majority, Recover with both witnesses, prove the old pair is fenced
	scripts/recover-e2e.sh

vault-renew-e2e: ## Vault dev, pki-consensus with the lab CA, renew each node's certificate in place
	scripts/vault-renew-e2e.sh

nomad-e2e: ## Nomad dev: Vault login by workload identity, witness prestart exit 77, JOIN refused on a draining node
	scripts/nomad-e2e.sh

secret: ## Print one lab secret from .env: make secret NAME=LAB_ADMIN_PASSWORD
	@grep "^$(NAME)=" .env | cut -d= -f2-
