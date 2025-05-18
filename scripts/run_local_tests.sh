#!/bin/bash

# Build and run tests in Docker
docker-compose build
docker-compose run --rm spark

# Clean up
docker-compose down 