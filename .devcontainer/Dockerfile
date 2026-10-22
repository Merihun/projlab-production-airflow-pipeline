# The lab's workspace image (services workspace and storefront). This is not the Airflow image: that one is
# the Dockerfile you write at the top of the repository in DE-1802.
ARG BASE_IMAGE=mcr.microsoft.com/devcontainers/python:1-3.12-bookworm
FROM ${BASE_IMAGE}

# The base image ships a global pipx `pytest` that would shadow the pinned one.
RUN rm -f /usr/local/py-utils/bin/pytest /usr/local/py-utils/bin/py.test

COPY .devcontainer/requirements.txt /tmp/projlab/requirements.txt
RUN pip install --no-cache-dir -r /tmp/projlab/requirements.txt && rm -rf /tmp/projlab
