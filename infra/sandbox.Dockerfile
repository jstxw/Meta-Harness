# Per-trial sandbox image (Phase 5, Docker-per-trial mode).
#
#   docker build -t meta-harness-sandbox -f infra/sandbox.Dockerfile infra
#
# pytest is baked in because trial containers run with --network none:
# nothing can be installed at runtime, deliberately.
FROM python:3.11-slim
RUN pip install --no-cache-dir pytest
