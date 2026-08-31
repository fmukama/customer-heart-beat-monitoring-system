
# `make help` prints the same sequence.

.PHONY: help \
        build lint lint-fix test test-unit simulator \
        up ps urls adminer grafana prometheus alertmanager notifier \
        logs producer consumer notifier-logs topics \
        psql show-raw show-daily show-late show-dlq reconcile \
        inject \
        metrics throughput \
        alerts show-notifications alert-demo \
        test-integration test-all \
        load-up lag load-down \
        shell diagrams restart \
        down clean

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

WHAT      ?= normal
N         ?= 1
RATE      ?= 100
CONSUMERS ?= 2
SAMPLE    ?= 0

# Command shortcuts. Defined after the settings above, because `:=`
# expands immediately and would otherwise capture empty values.
COMPOSE := docker compose
DEV     := $(COMPOSE) run --rm dev
PSQL    := $(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
LOAD    := $(COMPOSE) -f docker-compose.yml -f docker-compose.load.yml


# Echo arguments are unquoted so cmd.exe does not print literal quotes,
# which means multiple spaces collapse under sh. A dash separator keeps
# this readable whichever shell make picks.
help:
	@echo Everything runs in Docker. No host Python required.
	@echo Targets are listed in the order you run them.
	@echo == 1. BEFORE STARTING - no stack needed ==
	@echo make build - build the application images
	@echo make lint - run Ruff
	@echo make lint-fix - run Ruff with --fix
	@echo make test - run the 83 unit tests
	@echo make simulator - preview raw events, no Kafka involved
	@echo == 2. START THE STACK ==
	@echo make up - start everything
	@echo make ps - confirm every service is healthy
	@echo make urls - print every UI endpoint and credential
	@echo == 3. WATCH IT RUN ==
	@echo make logs - follow all logs
	@echo make producer - follow producer logs
	@echo make consumer - follow consumer logs
	@echo make notifier-logs - follow notifier logs
	@echo make topics - list Kafka topics
	@echo == 4. INSPECT THE DATA ==
	@echo make show-raw - recent raw events
	@echo make show-daily - daily aggregates, open and finalized
	@echo make show-late - late event summary
	@echo make show-dlq - read the dead letter topic
	@echo make reconcile - open windows vs raw counts
	@echo make psql - open a psql shell
	@echo == 5. EXERCISE THE EDGE CASES ==
	@echo make inject WHAT=normal - valid reading
	@echo make inject WHAT=abnormal - 185 bpm, stored and tagged
	@echo make inject WHAT=invalid - bad UUID, goes to the DLQ
	@echo make inject WHAT=outofrange - 600 bpm, schema violation
	@echo make inject WHAT=late - backdated 2h, flagged late
	@echo make inject WHAT=duplicate - same id twice, stored once
	@echo make inject WHAT=future - advances watermark, closes windows
	@echo make inject WHAT=toolate - raw kept, finalized window untouched
	@echo == 6. OBSERVABILITY ==
	@echo make metrics - raw consumer metrics
	@echo make throughput - rates, latency percentiles, watermark lag
	@echo make grafana - dashboards
	@echo make prometheus - metrics and targets
	@echo make adminer - PostgreSQL browser
	@echo == 7. ALERTING ==
	@echo make alerts - alert rules and their state
	@echo make show-notifications - alert history from PostgreSQL
	@echo make alert-demo - stop the consumer, watch ConsumerDown fire
	@echo == 8. TEST AGAINST THE LIVE STACK ==
	@echo make test-integration - the 27 integration tests
	@echo make test-all - unit plus integration
	@echo == 9. LOAD AND SCALING ==
	@echo make load-up RATE=1000 CONSUMERS=2 - scale up under load
	@echo make lag - per-partition lag and consumer ownership
	@echo make load-down - back to the single-consumer stack
	@echo == 10. MAINTENANCE ==
	@echo make shell - shell in the dev image
	@echo make diagrams - re-render the PlantUML diagrams
	@echo make restart - rebuild and restart producer and consumer
	@echo == 11. SHUT DOWN ==
	@echo make down - stop the stack, keep the data
	@echo make clean - stop and delete all volumes


# ==============================================================
# 1. Before starting - nothing running yet
# ==============================================================

build:
	$(COMPOSE) build


lint:
	$(DEV) ruff check .


lint-fix:
	$(DEV) ruff check --fix .


test: test-unit


test-unit:
	$(DEV) pytest


# Standalone generator. Prints JSON to stdout, never touches Kafka.
simulator:
	$(COMPOSE) run --rm simulator


# ==============================================================
# 2. Start the stack
# ==============================================================

up:
	$(COMPOSE) up -d --build


ps:
	$(COMPOSE) ps


urls:
	@echo   Adminer      http://localhost:$(ADMINER_PORT)    server=postgres db=$(POSTGRES_DB) user=$(POSTGRES_USER) pass=$(POSTGRES_PASSWORD)
	@echo   Grafana      http://localhost:$(GRAFANA_PORT)    user=$(GRAFANA_ADMIN_USER) pass=$(GRAFANA_ADMIN_PASSWORD)
	@echo   Prometheus   http://localhost:$(PROMETHEUS_PORT)
	@echo   Alertmanager http://localhost:$(ALERTMANAGER_PORT)
	@echo   Notifier     http://localhost:$(NOTIFIER_PORT)/health
	@echo   Metrics      http://localhost:$(METRICS_PORT)/metrics
	@echo   PostgreSQL   localhost:$(POSTGRES_PORT)

# Aliases, so the target name matches what you are looking for.
adminer: urls
grafana: urls
prometheus: urls
alertmanager: urls
notifier: urls


# ==============================================================
# 3. Watch it run
# ==============================================================

logs:
	$(COMPOSE) logs -f


producer:
	$(COMPOSE) logs -f producer


consumer:
	$(COMPOSE) logs -f consumer


notifier-logs:
	$(COMPOSE) logs -f notifier


topics:
	$(COMPOSE) exec kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 \
		--list


# ==============================================================
# 4. Inspect the data
# ==============================================================

show-raw:
	$(PSQL) -c "SELECT customer_id, heart_rate, status, is_late, round(lateness_seconds::numeric,1) AS late_s, event_time FROM heart_rate_events ORDER BY ingestion_time DESC LIMIT 15;"


show-daily:
	$(PSQL) -c "SELECT customer_id, window_start, event_count, round(average_heart_rate::numeric,1) AS avg, minimum_heart_rate AS min, maximum_heart_rate AS max, abnormal_count, is_finalized FROM heart_rate_daily ORDER BY window_start DESC, customer_id LIMIT 15;"


show-late:
	$(PSQL) -c "SELECT count(*) FILTER (WHERE is_late) AS late, count(*) AS total, round(max(lateness_seconds)::numeric,1) AS worst_lateness FROM heart_rate_events;"


show-dlq:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic $(KAFKA_HEART_RATE_DLQ_TOPIC) --from-beginning --max-messages $(N) --timeout-ms 15000


# Only open windows can reconcile with raw counts. A finalized window is
# immutable, so events arriving afterwards are stored raw and
# deliberately left out of the aggregate.
reconcile:
	$(PSQL) -c "SELECT d.customer_id, d.window_start::date AS window, d.event_count AS aggregated, r.raw, d.event_count = r.raw AS reconciles FROM heart_rate_daily d JOIN (SELECT customer_id, date_trunc('day', event_time) AS w, count(*) AS raw FROM heart_rate_events GROUP BY 1,2) r ON r.customer_id = d.customer_id AND r.w = d.window_start WHERE NOT d.is_finalized ORDER BY 1 LIMIT 10;"
	$(PSQL) -c "SELECT count(*) AS open_windows, count(*) FILTER (WHERE ok) AS reconciling FROM (SELECT d.event_count = r.raw AS ok FROM heart_rate_daily d JOIN (SELECT customer_id, date_trunc('day', event_time) AS w, count(*) AS raw FROM heart_rate_events GROUP BY 1,2) r ON r.customer_id = d.customer_id AND r.w = d.window_start WHERE NOT d.is_finalized) s;"


psql:
	$(COMPOSE) exec postgres \
		psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)


# ==============================================================
# 5. Exercise the edge cases
# ==============================================================

# WHAT = normal | abnormal | invalid | outofrange
#      | late | toolate | future | duplicate
inject:
	$(DEV) python scripts/inject.py $(WHAT)


# ==============================================================
# 6. Observability
# ==============================================================

metrics:
	$(DEV) python -c "import urllib.request;print(urllib.request.urlopen('http://consumer:8000/metrics').read().decode())"


# Pass SAMPLE to let rates settle first, e.g. make throughput SAMPLE=90
throughput: export SAMPLE := $(SAMPLE)
throughput:
	$(DEV) python scripts/measure.py


# ==============================================================
# 7. Alerting
# ==============================================================

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


# ==============================================================
# 8. Test against the live stack
# ==============================================================

test-integration:
	$(DEV) pytest tests/integration -m integration


test-all: test-unit test-integration


# ==============================================================
# 9. Load and scaling
# ==============================================================

# Target-specific export: the inline "VAR=x cmd" prefix is not portable
# to cmd.exe, which make uses on Windows.
load-up: export RATE := $(RATE)
load-up: export CONSUMERS := $(CONSUMERS)
load-up:
	$(LOAD) up -d --build


lag:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group $(KAFKA_CONSUMER_GROUP_ID)


load-down:
	$(LOAD) down --remove-orphans


# ==============================================================
# 10. Maintenance
# ==============================================================

shell:
	$(DEV) bash


diagrams:
	docker run --rm -v "$(CURDIR)/docs:/data" plantuml/plantuml:latest -tpng /data/dataflow.puml /data/deployment.puml
	docker run --rm -v "$(CURDIR)/docs:/data" plantuml/plantuml:latest -tsvg /data/dataflow.puml /data/deployment.puml


restart:
	$(COMPOSE) up -d --build producer consumer


# ==============================================================
# 11. Shut down
# ==============================================================

down:
	$(COMPOSE) down


clean:
	$(COMPOSE) down -v
