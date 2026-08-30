.PHONY: help install lint test simulator up down logs clean

help:
	@echo "Available commands:"
	@echo "  make install     Install Python dependencies"
	@echo "  make lint        Run Ruff"
	@echo "  make test        Run tests"
	@echo "  make simulator   Run the simulator"
	@echo "  make up          Start Docker services"
	@echo "  make down        Stop Docker services"
	@echo "  make logs        Show Docker logs"
	@echo "  make clean       Remove containers and volumes"


install:
	python -m pip install --upgrade pip
	pip install -e ".[dev]"


lint:
	ruff check .


test:
	pytest


simulator:
	python -m simulator


up:
	docker compose up -d


down:
	docker compose down


logs:
	docker compose logs -f


clean:
	docker compose down -v