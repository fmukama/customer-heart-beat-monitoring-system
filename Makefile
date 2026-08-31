.PHONY: help build lint lint-fix test test-unit test-integration test-all up down restart logs ps topics simulator producer consumer psql shell clean

COMPOSE := docker compose
DEV     := $(COMPOSE) run --rm dev

POSTGRES_USER ?= heartbeat_user
POSTGRES_DB   ?= heartbeat

help:
	@echo Everything runs in Docker. No host Python required.
	@echo   ---- build ----
	@echo   make build       Build the application images
	@echo   make lint        Run Ruff
	@echo   make lint-fix    Run Ruff with --fix
	@echo   make test        Run unit tests
	@echo   make test-integration  Run integration tests, needs make up
	@echo   ---- stack ----
	@echo   make up          Start the full stack
	@echo   make down        Stop the stack
	@echo   make restart     Rebuild and restart producer and consumer
	@echo   make logs        Follow all logs
	@echo   make ps          Show service status
	@echo   ---- inspect ----
	@echo   make topics      List Kafka topics
	@echo   make simulator   Print raw events, no Kafka
	@echo   make producer    Follow producer logs
	@echo   make consumer    Follow consumer logs
	@echo   make psql        Open a psql shell
	@echo   make shell       Open a shell in the dev image
	@echo   ---- reset ----
	@echo   make clean       Stop the stack and delete all volumes


build:
	$(COMPOSE) build


lint:
	$(DEV) ruff check .


lint-fix:
	$(DEV) ruff check --fix .


test: test-unit


test-unit:
	$(DEV) pytest


test-integration:
	$(DEV) pytest tests/integration -m integration


test-all: test-unit test-integration


up:
	$(COMPOSE) up -d --build


down:
	$(COMPOSE) down


restart:
	$(COMPOSE) up -d --build producer consumer


logs:
	$(COMPOSE) logs -f


ps:
	$(COMPOSE) ps


topics:
	$(COMPOSE) exec kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 \
		--list


simulator:
	$(COMPOSE) run --rm simulator


producer:
	$(COMPOSE) logs -f producer


consumer:
	$(COMPOSE) logs -f consumer


psql:
	$(COMPOSE) exec postgres \
		psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)


shell:
	$(DEV) bash


clean:
	$(COMPOSE) down -v
