# Makefile
# Variables
DOCKER_COMPOSE = docker compose
DOCKER_COMPOSE_FILE = docker-compose.local.yml
SERVICE_NAME = cv-performance-service
PYTHON = python3
PIP = $(PYTHON) -m pip
# Commands
.PHONY: help install run test lint format clean build up down logs
help:
	@echo "Available commands:"
	@echo "  install  - Install dependencies"
	@echo "  run      - Run the service locally"
	@echo "  test     - Run tests"
	@echo "  lint     - Lint the code"
	@echo "  format   - Format the code"
	@echo "  clean    - Clean temporary files"
	@echo "  build    - Build Docker image"
	@echo "  up       - Start services using Docker Compose"
	@echo "  down     - Stop Docker Compose services"
	@echo "  logs     - View service logs"
	@echo "  re       - Restart services using Docker Compose (make down && up)"
install:
	$(PIP) install -r requirements.txt
run:
	uvicorn app.main:app --reload
test:
	pytest
lint:
	flake8 .
format:
	black .
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
build:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) build $(SERVICE_NAME)
up:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) up -d
down:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down --remove-orphans
re:
	$(MAKE) down && $(MAKE) up
logs:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) logs -f $(SERVICE_NAME)
