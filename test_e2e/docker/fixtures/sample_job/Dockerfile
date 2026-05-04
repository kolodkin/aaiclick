FROM python:3.10-slim

# Framework-forwarded build-args (see _collect_build_args in docker_build.py)
ARG GIT_REMOTE
ARG GIT_SHA
ARG GIT_BRANCH
ARG BUILD_CONTEXT
ARG PIP_INDEX_URL
ARG AAICLICK_VERSION

# Image metadata (visible via `docker inspect <image>`)
LABEL org.opencontainers.image.source="${GIT_REMOTE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.ref.name="${GIT_BRANCH}"
LABEL aaiclick.build_context="${BUILD_CONTEXT}"

# Runtime env (visible to task code via os.environ)
ENV GIT_REMOTE=${GIT_REMOTE} \
    GIT_SHA=${GIT_SHA} \
    GIT_BRANCH=${GIT_BRANCH} \
    BUILD_CONTEXT=${BUILD_CONTEXT}

# Install the framework first — the test pypi (pypiserver) serves the wheel
# under test, and PIP_INDEX_URL is forwarded as a build-arg.
RUN pip install --no-cache-dir \
    --index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}" \
    --extra-index-url https://pypi.org/simple/ \
    "aaiclick[distributed]==${AAICLICK_VERSION}"

# Copy and install the user's repo as a package so `sample_jobs.entry_task`
# resolves via normal importlib without depending on the container's CWD.
COPY . /src
RUN pip install --no-cache-dir /src
