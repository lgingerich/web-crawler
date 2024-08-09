#!/bin/bash

# Ensure dependencies are up to date
poetry install

# Run the Python script
poetry run python src/web_crawler/main.py