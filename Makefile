COMPOSE := docker compose
CORE_SERVICES := postgres dagster-webserver dagster-daemon
ALL_SERVICES := $(CORE_SERVICES) metabase

.PHONY: config build up up-d up-core up-core-d logs logs-daemon logs-metabase down restart restart-core ps

config:
	$(COMPOSE) config

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up --build $(ALL_SERVICES)

up-d:
	$(COMPOSE) up --build -d $(ALL_SERVICES)

up-core:
	$(COMPOSE) up --build $(CORE_SERVICES)

up-core-d:
	$(COMPOSE) up --build -d $(CORE_SERVICES)

logs:
	$(COMPOSE) logs -f dagster-webserver

logs-daemon:
	$(COMPOSE) logs -f dagster-daemon

logs-metabase:
	$(COMPOSE) logs -f metabase

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up --build -d $(ALL_SERVICES)

restart-core:
	$(COMPOSE) down
	$(COMPOSE) up --build -d $(CORE_SERVICES)

ps:
	$(COMPOSE) ps