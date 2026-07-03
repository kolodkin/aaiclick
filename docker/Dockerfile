# syntax=docker/dockerfile:1
# Base aaiclick image: API server + SPA, also usable as a runner base.
# Build-arg AAICLICK_VERSION pins the wheel to install from PyPI.
FROM python:3.13-slim

ARG AAICLICK_VERSION
ARG PIP_INDEX_URL=https://pypi.org/simple/

LABEL org.opencontainers.image.source="https://github.com/kolodkin/aaiclick"
LABEL org.opencontainers.image.description="aaiclick API server + runner base"

# Install the released wheel with all extras (distributed + ai + server).
RUN pip install --no-cache-dir \
    --index-url "${PIP_INDEX_URL}" \
    "aaiclick[all]==${AAICLICK_VERSION}"

# Run as a non-root user.
RUN useradd --create-home --uid 1000 aaiclick
WORKDIR /home/aaiclick
USER aaiclick

EXPOSE 5255

# Health probe uses stdlib (slim image has no curl).
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:5255/health').status==200 else 1)"

CMD ["uvicorn", "aaiclick.server.app:app", "--host", "0.0.0.0", "--port", "5255"]
