#!/usr/bin/env bash
python -m pytest --cov=jobqueue_features .
coverage html
