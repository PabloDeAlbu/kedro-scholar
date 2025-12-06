include .env
DATE := $(shell date +%Y%m%d)

COMPOSE_PROJECT_NAME := $(notdir $(CURDIR))

default: up

bash:
	docker exec -it $(COMPOSE_PROJECT_NAME)_postgres bash

dump:
	docker exec $(COMPOSE_PROJECT_NAME)_postgres pg_dump -U $(DW_USER) $(DB_NAME) > ../data/01_raw/dw/$(DB_NAME)-$(DATE).sql

up:
	docker compose -f docker-compose.yml up -d

stop:
	docker compose -f docker-compose.yml stop
