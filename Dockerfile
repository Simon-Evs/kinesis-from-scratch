FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /usr/local/bin/

WORKDIR /workspace

CMD [ "sleep", "infinity" ]