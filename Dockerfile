# syntax=docker/dockerfile:1
# MessageBrokerEngine Dockerfile
# Build context MUST be parent directory containing: MessageBrokerEngine/, LoggingEngine/
# Build: docker build -f MessageBrokerEngine/Dockerfile -t messagebroker-engine:latest .

ARG RUST_VERSION=1.82.0
ARG APP_NAME=program

# Cache-busting args: when these change, Docker invalidates the cache for subsequent layers
ARG LOGGING_ENGINE_SHA=latest

################################################################################
# Stage 1: Build the application
FROM rust:${RUST_VERSION}-slim-bookworm AS build
ARG APP_NAME

# Re-declare cache-busting args in this stage
ARG LOGGING_ENGINE_SHA

# Install necessary build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Cache bust: this label changes when dependency SHA changes
LABEL logging_engine_sha="${LOGGING_ENGINE_SHA}"

# Copy dependencies first (for better caching)
COPY LoggingEngine/ ./LoggingEngine/

# Copy MessageBrokerEngine
COPY MessageBrokerEngine/ ./MessageBrokerEngine/

# Build from MessageBrokerEngine directory
WORKDIR /app/MessageBrokerEngine

RUN cargo build --locked --release && \
    cp target/release/$APP_NAME /bin/server

################################################################################
# Stage 2: Create a smaller runtime image
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libc6 \
    net-tools \
    procps \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create the health check script
RUN echo '#!/bin/sh' > /usr/local/bin/health_check.sh \
&& echo 'if ! pgrep "server"; then exit 1; fi' >> /usr/local/bin/health_check.sh \
&& chmod +x /usr/local/bin/health_check.sh

# Create the liveness probe script
RUN echo '#!/bin/sh' > /usr/local/bin/liveness_check.sh \
&& echo 'if ! pgrep "server"; then exit 1; fi' >> /usr/local/bin/liveness_check.sh \
&& chmod +x /usr/local/bin/liveness_check.sh

# Create a non-privileged user to run the app
ARG UID=10001
RUN adduser --disabled-password --gecos "" --home "/nonexistent" --shell "/sbin/nologin" --no-create-home --uid "${UID}" appuser

# Copy the built application from the build stage
COPY --from=build /bin/server /bin/server

# Ensure the binary is executable
RUN chmod +x /bin/server

EXPOSE 3200

# Switch to non-privileged user
USER appuser

# Set the command to run the application
CMD ["/bin/server"]