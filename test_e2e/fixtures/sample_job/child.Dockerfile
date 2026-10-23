# syntax=docker/dockerfile:1
# Second Dockerfile for the child-build e2e: the same commit as a distinct image.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY . /src
WORKDIR /src
