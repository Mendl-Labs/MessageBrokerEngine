# =============================================================================
# Nwagbara Group Trading Platform — Root Makefile
# =============================================================================
# Usage:
#   make init          — clone / update all submodules
#   make test          — run tests for every engine
#   make coverage      — generate coverage reports (requires tarpaulin / jest)
#   make build         — build all engines
#   make clean         — remove build artefacts
#   make status        — show submodule status

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Engines that use Rust
RUST_ENGINES := MessageBrokerEngine LoggingEngine

# Engines that use Node / TypeScript
NODE_ENGINES := BacktestingEngine

# PAT may be needed to init private submodules:
#   export GITHUB_TOKEN=<your-pat>  && make init
GH_TOKEN ?= $(GITHUB_TOKEN)

# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
.PHONY: init
init: ## Initialise and update all git submodules
	@echo "==> Initialising submodules…"
	git submodule update --init --recursive
	@echo "Done."

# ---------------------------------------------------------------------------
.PHONY: update
update: ## Pull the latest commit for every submodule on its default branch
	@echo "==> Updating all submodules to their remote default branch…"
	git submodule foreach --recursive 'git fetch origin && git checkout main 2>/dev/null || git checkout master 2>/dev/null || true && git pull --ff-only 2>/dev/null || true'
	@echo "Done."

# ---------------------------------------------------------------------------
.PHONY: build
build: build-rust build-node ## Build all engines

.PHONY: build-rust
build-rust: ## Build all Rust engines
	@for engine in $(RUST_ENGINES); do \
		if [ -f "$$engine/Cargo.toml" ]; then \
			echo "==> Building $$engine (Rust)…"; \
			cargo build --workspace --manifest-path "$$engine/Cargo.toml" || exit 1; \
		fi; \
	done

.PHONY: build-node
build-node: ## Build all Node/TypeScript engines
	@for engine in $(NODE_ENGINES); do \
		if [ -f "$$engine/package.json" ]; then \
			echo "==> Building $$engine (Node)…"; \
			npm ci --prefix "$$engine" && npm run build --prefix "$$engine" 2>/dev/null || true; \
		fi; \
	done

# ---------------------------------------------------------------------------
.PHONY: test
test: test-rust test-node ## Run all tests

.PHONY: test-rust
test-rust: ## Run tests for all Rust engines
	@for engine in $(RUST_ENGINES); do \
		if [ -f "$$engine/Cargo.toml" ]; then \
			echo "==> Testing $$engine (Rust)…"; \
			cargo test --workspace --manifest-path "$$engine/Cargo.toml" || exit 1; \
		fi; \
	done

.PHONY: test-node
test-node: ## Run tests for all Node engines (non-watch mode)
	@for engine in $(NODE_ENGINES); do \
		if [ -f "$$engine/package.json" ]; then \
			echo "==> Testing $$engine (Node)…"; \
			CI=true npm test --prefix "$$engine" -- --watchAll=false --passWithNoTests || exit 1; \
		fi; \
	done

# ---------------------------------------------------------------------------
.PHONY: coverage
coverage: coverage-rust coverage-node ## Generate coverage reports for all engines

.PHONY: coverage-rust
coverage-rust: ## Generate Rust coverage reports (requires cargo-tarpaulin)
	@command -v cargo-tarpaulin >/dev/null 2>&1 || cargo install cargo-tarpaulin --locked
	@for engine in $(RUST_ENGINES); do \
		if [ -f "$$engine/Cargo.toml" ]; then \
			echo "==> Coverage for $$engine (Rust)…"; \
			cd "$$engine" && \
			cargo tarpaulin --workspace --out xml json --output-dir ./coverage --timeout 300; \
			cd ..; \
		fi; \
	done

.PHONY: coverage-node
coverage-node: ## Generate Node coverage reports
	@for engine in $(NODE_ENGINES); do \
		if [ -f "$$engine/package.json" ]; then \
			echo "==> Coverage for $$engine (Node)…"; \
			CI=true npm test --prefix "$$engine" -- \
				--watchAll=false \
				--coverage \
				--coverageReporters=text-summary \
				--passWithNoTests || true; \
		fi; \
	done

# ---------------------------------------------------------------------------
.PHONY: lint
lint: lint-rust lint-node ## Lint all engines

.PHONY: lint-rust
lint-rust: ## Lint all Rust engines with clippy
	@for engine in $(RUST_ENGINES); do \
		if [ -f "$$engine/Cargo.toml" ]; then \
			echo "==> Linting $$engine…"; \
			cargo clippy --workspace --manifest-path "$$engine/Cargo.toml" -- -D warnings || true; \
		fi; \
	done

.PHONY: lint-node
lint-node: ## Lint all Node engines with ESLint
	@for engine in $(NODE_ENGINES); do \
		if [ -f "$$engine/package.json" ]; then \
			echo "==> Linting $$engine…"; \
			npm run lint --prefix "$$engine" 2>/dev/null || true; \
		fi; \
	done

# ---------------------------------------------------------------------------
.PHONY: clean
clean: ## Remove build artefacts from all engines
	@for engine in $(RUST_ENGINES); do \
		if [ -f "$$engine/Cargo.toml" ]; then \
			echo "==> Cleaning $$engine…"; \
			cargo clean --manifest-path "$$engine/Cargo.toml" 2>/dev/null || true; \
		fi; \
	done
	@for engine in $(NODE_ENGINES); do \
		if [ -d "$$engine/node_modules" ]; then \
			echo "==> Cleaning $$engine…"; \
			rm -rf "$$engine/node_modules" "$$engine/build" "$$engine/dist" 2>/dev/null || true; \
		fi; \
	done

# ---------------------------------------------------------------------------
.PHONY: status
status: ## Show submodule status
	@echo "==> Submodule status:"
	@git submodule status
