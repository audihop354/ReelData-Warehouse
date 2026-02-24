COMPOSE := docker compose
SERVICES := postgres dagster-webserver dagster-daemon

config:
	$(COMPOSE) config

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up --build $(SERVICES)

up-d:
	$(COMPOSE) up --build -d $(SERVICES)

logs:
	$(COMPOSE) logs -f dagster-webserver

logs-daemon:
	$(COMPOSE) logs -f dagster-daemon

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up --build -d $(SERVICES)

ps:
	$(COMPOSE) ps