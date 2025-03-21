ARG PYTHON_VERSION=3.12-bullseye

# define an alias for the specfic python version used in this file.
FROM python:${PYTHON_VERSION} as python

# Python build stage
FROM python as python-build-stage

# Install apt packages
RUN apt-get update && apt-get install --no-install-recommends -y \
  # dependencies for building Python packages
  build-essential \
  # psycopg2 dependencies
  libpq-dev \
  # postgis dependencies
  proj-bin \
  libgeos-dev \
  libxml2 \
  libjson-c-dev \
  gdal-bin \
  libsqlite3-mod-spatialite

# Requirements are installed here to ensure they will be cached.
COPY ./requirements/* .

# Create Python Dependency and Sub-Dependency Wheels.
RUN pip wheel --wheel-dir /usr/src/app/wheels  \
  -r requirements.txt -r requirements-dev.txt

# Python 'run' stage
FROM python as python-run-stage

ARG BUILD_ENVIRONMENT=production
ARG APP_HOME=/app

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR ${APP_HOME}

RUN addgroup --system smartsetter \
    && adduser --system --ingroup smartsetter smartsetter

# Install required system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
  # cleaning up unused files
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*

# All absolute dir copies ignore workdir instruction. All relative dir copies are wrt to the workdir instruction
# copy python dependency wheels from python-build-stage
COPY --from=python-build-stage /usr/src/app/wheels  /wheels/

# use wheels to install python dependencies
RUN pip install --no-cache-dir --no-index --find-links=/wheels/ /wheels/* \
  && rm -rf /wheels/

# copy application code to WORKDIR
COPY . ${APP_HOME}

ENTRYPOINT [ "sleep", "infinity" ]
