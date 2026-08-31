.PHONY: help build lint lint-fix test test-unit test-integration test-all up down restart logs ps topics simulator producer consumer psql shell clean load-up load-down lag throughput urls adminer grafana prometheus metrics inject show-raw show-daily show-late show-dlq reconcile diagrams alerts show-notifications alert-demo

COMPOSE := docker compose
DEV     := $(COMPOSE) run --rm dev

POSTGRES_USER ?= heartbeat_user
POSTGRES_PASSWORD ?= heartbeat_password
POSTGRES_DB   ?= heartbeat
POSTGRES_PORT ?= 5432
ADMINER_PORT ?= 8080
ALERTMANAGER_PORT ?= 9093
NOTIFIER_PORT ?= 9091
GRAFANA_PORT ?= 3000
GRAFANA_ADMIN_USER ?= admin
GRAFANA_ADMIN_PASSWORD ?= admin_password
PROMETHEUS_PORT ?= 9090
METRICS_PORT ?= 8000
KAFKA_CONSUMER_GROUP_ID ?= heartbeat-consumer-group
KAFKA_HEART_RATE_DLQ_TOPIC ?= heart-rate-events-dlq
WHAT ?= normal
N ?= 1

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
	@echo   ---- dashboards ----
	@echo   make urls        Print every UI endpoint and credential
	@echo   make adminer     PostgreSQL browser  http://localhost:8080
	@echo   make grafana     Dashboards          http://localhost:3000
	@echo   make prometheus  Metrics and targets http://localhost:9090
	@echo   make metrics     Raw consumer metrics
	@echo   ---- inspect ----
	@echo   make topics      List Kafka topics
	@echo   make simulator   Print raw events, no Kafka
	@echo   make producer    Follow producer logs
	@echo   make consumer    Follow consumer logs
	@echo   make psql        Open a psql shell
	@echo   make shell       Open a shell in the dev image
	@echo   ---- verify the pipeline ----
	@echo   make inject WHAT=late   Publish one crafted event
	@echo   make show-raw    Recent raw events
	@echo   make show-daily  Daily aggregates
	@echo   make show-late   Late event summary
	@echo   make show-dlq    Read the dead letter topic
	@echo   make reconcile   Check aggregates match raw counts
	@echo   ---- alerting ----
	@echo   make alerts      Show alert rules and their state
	@echo   make show-notifications  Alert history from PostgreSQL
	@echo   make alert-demo  Stop the consumer and watch ConsumerDown fire
	@echo   ---- load ----
	@echo   make load-up RATE=1000 CONSUMERS=3   Scale up under load
	@echo   make lag         Show Kafka consumer group lag and assignment
	@echo   make throughput  Sample throughput and lag from Prometheus
	@echo   make load-down   Tear down the load overlay
	@echo   make diagrams    Re-render the PlantUML diagrams
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


PSQL := $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

urls:
	@echo   Adminer     http://localhost:$(ADMINER_PORT)    server=postgres db=$(POSTGRES_DB) user=$(POSTGRES_USER) pass=$(POSTGRES_PASSWORD)
	@echo   Grafana     http://localhost:$(GRAFANA_PORT)    user=$(GRAFANA_ADMIN_USER) pass=$(GRAFANA_ADMIN_PASSWORD)
	@echo   Prometheus  http://localhost:$(PROMETHEUS_PORT)
	@echo   Alertmanager http://localhost:$(ALERTMANAGER_PORT)
	@echo   Notifier    http://localhost:$(NOTIFIER_PORT)/health
	@echo   Metrics     http://localhost:$(METRICS_PORT)/metrics
	@echo   PostgreSQL  localhost:$(POSTGRES_PORT)

adminer: urls
grafana: urls
prometheus: urls

metrics:
	$(DEV) python -c "import urllib.request;print(urllib.request.urlopen('http://consumer:8000/metrics').read().decode())"


inject:
	$(DEV) python scripts/inject.py $(WHAT)


show-raw:
	$(PSQL) -c "SELECT customer_id, heart_rate, status, is_late, round(lateness_seconds::numeric,1) AS late_s, event_time FROM heart_rate_events ORDER BY ingestion_time DESC LIMIT 15;"


show-daily:
	$(PSQL) -c "SELECT customer_id, window_start, event_count, round(average_heart_rate::numeric,1) AS avg, minimum_heart_rate AS min, maximum_heart_rate AS max, abnormal_count, is_finalized FROM heart_rate_daily ORDER BY window_start DESC, customer_id LIMIT 15;"


show-late:
	$(PSQL) -c "SELECT count(*) FILTER (WHERE is_late) AS late, count(*) AS total, round(max(lateness_seconds)::numeric,1) AS worst_lateness FROM heart_rate_events;"


show-dlq:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic $(KAFKA_HEART_RATE_DLQ_TOPIC) --from-beginning --max-messages $(N) --timeout-ms 15000


alerts:
	$(DEV) python -c "import json,urllib.request as u; d=json.load(u.urlopen('http://prometheus:9090/api/v1/rules')); rs=[r for g in d['data']['groups'] for r in g['rules']]; print('%-24s %-9s %s' % ('ALERT','STATE','SEVERITY')); [print('%-24s %-9s %s' % (r['name'], r.get('state','-'), r['labels'].get('severity','-'))) for r in rs]"


show-notifications:
	$(PSQL) -c "SELECT notification_id, alert_type, severity, status, created_at, resolved_at FROM notifications ORDER BY created_at DESC LIMIT 20;"


# ConsumerDown has for:1m and Alertmanager adds group_wait, so this
# deliberately waits rather than polling tightly.
alert-demo:
	@echo Stopping the consumer...
	$(COMPOSE) stop consumer
	@echo Waiting 150s for ConsumerDown to fire and reach the notifier...
	$(DEV) python -c "import time; time.sleep(150)"
	$(MAKE) alerts
	$(MAKE) show-notifications
	@echo Restarting the consumer...
	$(COMPOSE) start consumer
	@echo Waiting 150s for the alert to resolve...
	$(DEV) python -c "import time; time.sleep(150)"
	$(MAKE) show-notifications


reconcile:
	$(PSQL) -c "SELECT d.customer_id, d.window_start::date AS window, d.event_count AS aggregated, r.raw, d.event_count = r.raw AS reconciles FROM heart_rate_daily d JOIN (SELECT customer_id, date_trunc('day', event_time) AS w, count(*) AS raw FROM heart_rate_events GROUP BY 1,2) r ON r.customer_id = d.customer_id AND r.w = d.window_start WHERE NOT d.is_finalized ORDER BY 1 LIMIT 10;"
	$(PSQL) -c "SELECT count(*) AS open_windows, count(*) FILTER (WHERE ok) AS reconciling FROM (SELECT d.event_count = r.raw AS ok FROM heart_rate_daily d JOIN (SELECT customer_id, date_trunc('day', event_time) AS w, count(*) AS raw FROM heart_rate_events GROUP BY 1,2) r ON r.customer_id = d.customer_id AND r.w = d.window_start WHERE NOT d.is_finalized) s;"


LOAD := $(COMPOSE) -f docker-compose.yml -f docker-compose.load.yml

RATE      ?= 100
CONSUMERS ?= 2

# Target-specific export: the inline "VAR=x cmd" prefix is not portable
# to cmd.exe, which make uses on Windows.
load-up: export RATE := $(RATE)
load-up: export CONSUMERS := $(CONSUMERS)
load-up:
	$(LOAD) up -d --build

load-down:
	$(LOAD) down --remove-orphans

lag:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group $(KAFKA_CONSUMER_GROUP_ID)

SAMPLE ?= 0

throughput: export SAMPLE := $(SAMPLE)
throughput:
	$(DEV) python scripts/measure.py


diagrams:
	docker run --rm -v "$(CURDIR)/docs:/data" plantuml/plantuml:latest -tpng /data/dataflow.puml /data/deployment.puml
	docker run --rm -v "$(CURDIR)/docs:/data" plantuml/plantuml:latest -tsvg /data/dataflow.puml /data/deployment.puml


clean:
	$(COMPOSE) down -v
