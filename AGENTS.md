# ./AGENTS.md

## WHY

- This repo uses a uniform, reproducible workflow based on **uv** and **pyproject.toml**.
- These instructions exist to prevent tool drift (e.g., pip) and OS mismatch.

## Requirements

- Use **uv** for all environment, dependency, and run commands in this repo.
- Do **not** recommend or use `pip install ...` as the primary workflow.
- This repo targets **Python 3.14**, pinned via uv.
- Commands and guidance must work on Windows, macOS, and Linux.
- If shell-specific commands are unavoidable, provide both:
  - PowerShell (Windows)
  - bash/zsh (macOS/Linux)

## Quickstart

- Install **uv** using the official method for your OS.
- Keep uv current.
- Pin Python 3.14 for this project using uv.
- Sync dependencies (dev + docs) and upgrade.

```shell
uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

uvx pre-commit install
```

## Common Tasks

Run all commands via **uv**.

Lint / format using pre-commit:

```shell
git add -A
uvx pre-commit run --all-files
# repeat if changes were made
git add -A
uvx pre-commit run --all-files
```

Run checks and build documentation:

```shell
uv run python -m ruff format .
uv run python -m ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build
```

## pre-commit

- pre-commit runs only on tracked / staged files.
- Developers should `git add -A` files before expecting hooks to run.

## Kafka 4.2

This project uses `confluent-kafka` and other dependencies
listed in `pyproject.toml`.

## Kafka Instructions

Kafka instructions are available at
[kafka](https://denisecase.github.io/pro-analytics-02/kafka/).

## Many Terminals

Multiple terminals are used:

1. If Windows, WSL: Terminal 1 to run kafka
2. If Windows, WSL: Terminal 2 to manage topics.
3. If Windows, PowerShell: Terminal 3 to run the project and a producer.
4. If Windows, PowerShell: Terminal 4 to run a consumer.
