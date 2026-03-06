# bub-contrib

Contrib packages for the `bub` ecosystem.

## Packages

- `packages/bub-codex`
  - Bub plugin entry point: `codex`
  - Provides a `run_model` hook that delegates model execution to the Codex CLI.
- `packages/bub-tg-feed`
  - Bub plugin entry point: `tg-feed`
  - Provides an AMQP-based channel adapter for Telegram feed messages.
- `packages/bub-schedule`
  - Bub plugin entry point: `schedule`
  - Provides scheduling channel/tools backed by APScheduler with a JSON job store.
- `packages/bub-discord`
  - Provides a Discord channel adapter (`DiscordChannel`) for Bub message IO.
  - Note: this package currently does not expose a Bub plugin entry point.

## Repository Layout

```text
packages/
  bub-codex/
  bub-tg-feed/
  bub-schedule/
  bub-discord/
```

## Prerequisites

- Python 3.12+ (workspace root)
- `uv` (recommended)

## Development Setup

Install all workspace dependencies:

```bash
uv sync
```

Install contrib packages in editable mode:

```bash
uv pip install -e packages/bub-tg-feed -e packages/bub-schedule -e packages/bub-discord
uv pip install -e packages/bub-codex
```

## Runtime Notes

### `bub-tg-feed` environment variables

- `AMQP_URL`: RabbitMQ/AMQP connection URL
- `BUB_TELEGRAM_TOKEN`: Telegram bot token used for chat actions and bot metadata

### `bub-codex` environment variables

- `BUB_CODEX_MODEL`: Optional model name passed to Codex CLI (`--model`)
- `BUB_CODEX_YOLO_MODE`: Optional boolean, enables `--dangerously-bypass-approvals-and-sandbox`

### `bub-schedule` persistence

- Scheduled jobs are persisted to `jobs.json` under Bub runtime home.

### `bub-discord` environment variables

- `BUB_DISCORD_TOKEN`: Discord bot token (required)
- `BUB_DISCORD_ALLOW_USERS`: Optional comma-separated sender allowlist (user id/username/global name)
- `BUB_DISCORD_ALLOW_CHANNELS`: Optional comma-separated channel id allowlist
- `BUB_DISCORD_COMMAND_PREFIX`: Optional command prefix, default `!`
- `BUB_DISCORD_PROXY`: Optional proxy URL for Discord API

## License

This repository is licensed under [LICENSE](./LICENSE).
