# Bot-Managed Runtime

## Goal

Поднять eXpress-бота, который управляет миграцией Telegram-чата только через Telethon user session.

Подробный справочник по env-переменным и источникам значений лежит в [env_variables_reference.md](/Users/aleksandrosovskii/exTG/docs/runbooks/env_variables_reference.md).
Короткая production-версия только с обязательными переменными лежит в [env_variables_operator_reference.md](/Users/aleksandrosovskii/exTG/docs/runbooks/env_variables_operator_reference.md).
Пользовательская инструкция по командам и основным сценариям лежит в [bot_user_guide.md](/Users/aleksandrosovskii/exTG/docs/runbooks/bot_user_guide.md).

## Required env

- `EXTG_POSTGRES__DSN`
- `EXTG_SECURITY__MESSAGE_HMAC_SECRET`
- `EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY`
- `EXTG_BOT__MIGRATION_ID`

Для bot UI и worker всегда нужны:

- `EXTG_EXPRESS__BOT_ID`
- `EXTG_EXPRESS__CTS_URL`
- `EXTG_EXPRESS__SECRET_KEY`
- `EXTG_BOT__VERIFY_REQUESTS`

Если Telethon работает локально в том же deployment:

- `EXTG_TELEGRAM__API_ID`
- `EXTG_TELEGRAM__API_HASH`

Если Telethon вынесен в отдельный сервис:

- `EXTG_TELETHON_SERVICE__BASE_URL`
- `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN` рекомендуется

Опционально:

- `EXTG_BOT__OPERATOR_HUIDS`
- `EXTG_BOT__FSM_STORAGE_BACKEND`
- `EXTG_POSTGRES__PAYLOAD_STORAGE_MODE`
- `EXTG_POSTGRES__PERSIST_MESSAGE_BODIES`
- `EXTG_WORKER__OUTBOX_PUBLISHER_ENABLED`
- `EXTG_WORKER__OUTBOX_PUBLISHER_BATCH_SIZE`
- `EXTG_WORKER__OUTBOX_PUBLISHER_POLL_INTERVAL_SECONDS`
- `EXTG_WORKER__OUTBOX_PUBLISHER_LEASE_DURATION_SECONDS`
- `EXTG_WORKER__BACKPRESSURE_MAX_QUEUED_JOBS`
- `EXTG_WORKER__BACKPRESSURE_MAX_RUNNING_JOBS`
- `EXTG_WORKER__RUNTIME_METRICS_ENABLED`
- `EXTG_WORKER__RUNTIME_METRICS_POLL_INTERVAL_SECONDS`
- `EXTG_WORKER__ATTACHMENT_MAX_UPLOAD_SIZE_BYTES`
- `EXTG_WORKER__ATTACHMENT_TRANSFER_CONCURRENCY`
- `EXTG_WORKER__ATTACHMENT_SPOOL_MAX_MEMORY_BYTES`
- `EXTG_TELETHON_SERVICE__HOST`
- `EXTG_TELETHON_SERVICE__PORT`
- `EXTG_TELETHON_SERVICE__REQUEST_TIMEOUT_SECONDS`
- `EXTG_KAFKA__ENABLED`
- `EXTG_KAFKA__BOOTSTRAP_SERVERS`
- `EXTG_KAFKA__COMMAND_TOPIC`
- `EXTG_KAFKA__CONSUMER_GROUP`

Рекомендуемый secure profile:

- `EXTG_POSTGRES__PERSIST_MESSAGE_BODIES=false`
- `EXTG_POSTGRES__PAYLOAD_STORAGE_MODE=redacted`
- `EXTG_BOT__FSM_STORAGE_BACKEND=in_memory`

## Start

### Local E2E Quick Start

Готовый шаблон для локального e2e лежит в [.env.e2e.example](/Users/aleksandrosovskii/exTG/.env.e2e.example), а orchestration полностью спрятан в [docker-compose.local-e2e.yml](/Users/aleksandrosovskii/exTG/docker-compose.local-e2e.yml).

Что поднимается:

- `postgres`
- `migrate` one-shot service с `alembic upgrade head`
- `telethon-service`
- `worker`
- `bot`

Поднять все локально:

```bash
cd /Users/aleksandrosovskii/exTG
cp .env.e2e.example .env.e2e
docker compose -f docker-compose.local-e2e.yml up --build -d
```

Остановить:

```bash
cd /Users/aleksandrosovskii/exTG
docker compose -f docker-compose.local-e2e.yml down
```

Логи:

```bash
docker compose -f /Users/aleksandrosovskii/exTG/docker-compose.local-e2e.yml logs -f telethon-service
docker compose -f /Users/aleksandrosovskii/exTG/docker-compose.local-e2e.yml logs -f worker
docker compose -f /Users/aleksandrosovskii/exTG/docker-compose.local-e2e.yml logs -f bot
```

Проверка, что бот поднялся:

```bash
curl -i http://127.0.0.1:8000/health
```

После старта основной flow в eXpress такой:

```text
/connect +79990001122
/chats
/migrate <source_chat_id>
/status <source_chat_id>
```

### Production Docker Compose

Для production-профиля теперь есть отдельные файлы:

- [.env.prod.example](/Users/aleksandrosovskii/exTG/.env.prod.example)
- [docker-compose.prod.yml](/Users/aleksandrosovskii/exTG/docker-compose.prod.yml)
- [.gitlab-ci.yml](/Users/aleksandrosovskii/exTG/.gitlab-ci.yml)

GitLab CI теперь поддерживает два manual deploy-контура:

- `deploy-test` читает secret variable `EXTG_TEST_ENV_FILE`;
- `deploy-prod` читает secret variable `EXTG_PROD_ENV_FILE`.

В обе переменные нужно положить полное содержимое runtime `.env.prod` для соответствующего контура.

Что поднимается:

- `postgres`
- `kafka`
- `create-topics`
- `migrate`
- `telethon-service`
- `worker`
- `bot`

Ключевые отличия от локального e2e:

- образ запускается не под `root`;
- Postgres и Kafka сидят на named volumes;
- `bot`, `worker`, `telethon-service` работают из одного image, но разными entrypoint'ами;
- `migrate` и `create-topics` идут отдельными one-shot шагами;
- сохранены `restart`, `logging`, `ulimits` и Traefik labels из production-style примеров, но они адаптированы под текущую split-архитектуру.

Минимальный запуск вручную:

```bash
cd /Users/aleksandrosovskii/exTG
cp .env.prod.example .env.prod

docker compose --env-file .env.prod -f docker-compose.prod.yml pull
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d postgres kafka
docker compose --env-file .env.prod -f docker-compose.prod.yml run --rm create-topics
docker compose --env-file .env.prod -f docker-compose.prod.yml run --rm migrate
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d telethon-service worker bot
```

Остановка:

```bash
cd /Users/aleksandrosovskii/exTG
docker compose --env-file .env.prod -f docker-compose.prod.yml down
```

Проверка:

```bash
curl -i http://127.0.0.1:8000/health
docker compose --env-file .env.prod -f docker-compose.prod.yml ps
```

### Local Telethon Runtime

```bash
set -a
source .env
set +a

uv sync --frozen --extra dev
uv run alembic upgrade head
uv run extg-worker
uv run extg-bot
```

По умолчанию:

- webhook app слушает `127.0.0.1:8080`;
- worker запускается отдельным процессом и обрабатывает durable jobs из Postgres queue;
- в том же worker runtime крутится outbox publisher; если `EXTG_KAFKA__ENABLED=false`, он работает в shadow/logging mode.
- Telethon gateway живет в bot/worker runtime и использует `EXTG_TELEGRAM__API_ID/HASH`.

### Dedicated Telethon Integration Service

```bash
set -a
source .env
set +a

uv sync --frozen --extra dev
uv run alembic upgrade head
uv run extg-telethon-service
uv run extg-worker
uv run extg-bot
```

Для такого режима:

- у `extg-telethon-service` должны быть `EXTG_TELEGRAM__API_ID/HASH`;
- у `extg-bot` и `extg-worker` должен быть `EXTG_TELETHON_SERVICE__BASE_URL=http://host:8092`;
- `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN` должен совпадать у всех трех процессов;
- если запускаешь сервис через `uvicorn --factory`, укажи тот же env, отдельный `EXTG_RUNTIME_ROLE` не нужен: entrypoint и container helper сами переводят runtime в `telethon_service`.

## Endpoints

- events: `POST ${EXTG_BOT__EVENTS_PATH}` default `/command`
- status: `GET ${EXTG_BOT__STATUS_PATH}` default `/status`
- callbacks: `POST ${EXTG_BOT__CALLBACKS_PATH}` default `/notification/callback`

## Supported commands

- `/help`
- `/connect [PHONE]`
- `/account`
- `/disconnect`
- `/chats [query=TEXT] [limit=N]`
- `/chat_users <source_chat_id>`
- `/configure <source_chat_id> [access=invite|link] [format=quote|source_id|none] [media=on|off] [skip=on|off] [from=ISO] [to=ISO]`
- `/migrate <source_chat_id> [progress=resume|ask] [batch=N] [target=create|bind|manual_review_required] [target_title=TEXT] [target_chat_id=ID]`
- `/remigrate <source_chat_id> [batch=N]`
- `/migrate_all [progress=resume|ask] [batch=N]`
- `/status [source_chat_id]`
- `/stats`
- `/retry_failed [source_chat_id] [limit=N] [batch=N]`
- `/cancel`

## Defaults

- `/migrate_all` автоматически создает недостающие chat-config записи.
- Для автосозданного конфига используются:
  - `access=link`
  - формат сообщения по умолчанию
  - `media=on`
  - `skip=off`
- `/configure <source_chat_id>` без опций показывает текущую конфигурацию.
- `/migrate` и `/remigrate` продолжают перенос с безопасного смещения.

## Behavior notes

- Управление миграцией доступно только через бота; локальный YAML/CLI control plane не используется.
- Telegram-операции идут только через личную Telethon-сессию пользователя, привязанную через `/connect`.
- `migrate_all`, `migrate`, `remigrate` и `retry_failed` enqueue-ят durable jobs в Postgres queue; выполнение идет отдельным worker-процессом по lease/heartbeat и переживает рестарт bot UI.
- enqueue path дополнительно пишет shadow event в `integration_outbox` атомарно с `migration_job`; это Phase 2 foundation для будущего Kafka cutover.
- worker применяет simple backpressure на enqueue: если queued/running jobs превышают `EXTG_WORKER__BACKPRESSURE_MAX_*`, новые `/migrate`, `/remigrate`, `/retry_failed` и fan-out jobs из `/migrate_all` будут временно отклоняться с явным сообщением.
- worker периодически пишет structured runtime metrics по `migration_job`, `integration_outbox` и `service_watermark`; интервал контролируется через `EXTG_WORKER__RUNTIME_METRICS_*`.
- Если задан `EXTG_TELETHON_SERVICE__BASE_URL`, bot UI и worker больше не трогают Telethon напрямую: `/connect`, `/chats`, `/chat_users`, history fetch и attachment download идут через отдельный internal HTTP boundary.
- Attachment pipeline теперь staged: worker сначала загружает бинарь в eXpress Files API, фиксирует `attachment_stage`, и только затем отправляет сообщение в чат. Если текущий стенд не принимает staged-file payload в direct notification, adapter делает fallback на обычный multipart send без потери идемпотентности message/attachment registry.
- Oversized attachments с известным размером пропускаются до download/upload, если превышают `EXTG_WORKER__ATTACHMENT_MAX_UPLOAD_SIZE_BYTES`; в текст сообщения добавляется явная пометка о пропуске.
- Upload staging больше не держит крупные файлы целиком в памяти по умолчанию: `EXTG_WORKER__ATTACHMENT_SPOOL_MAX_MEMORY_BYTES` ограничивает memory spool, а `EXTG_WORKER__ATTACHMENT_TRANSFER_CONCURRENCY` ограничивает одновременные тяжелые attachment download/upload операции.
- Если для migration уже есть активная job, новый запуск по тому же scope будет отклонен.
- Временное состояние login/wizard при `EXTG_BOT__FSM_STORAGE_BACKEND=in_memory` не пишется в Postgres и теряется при рестарте процесса.
- Postgres хранит операционные метаданные; plaintext сообщений и чувствительные JSON payloads по умолчанию не сохраняются.
- Если для чата нужны дополнительные шаги по участникам, правам или канальному доступу, бот сам переведет пользователя в соответствующий диалог.
- Bot UI, worker и при необходимости telethon-service больше не живут в одном процессе; для production нужны как минимум bot + worker, а при distributed rollout еще и отдельный telethon-service.
- `delta-sync` через remote telethon boundary пока сознательно не включен; текущий Phase 3 закрывает session management, dialog discovery, history fetch, participants/topics, channel access profile и attachment download.
- Если `EXTG_KAFKA__ENABLED=true`, worker перестает poll-ить `migration_job` и читает `migration.job.requested` из Kafka command topic. Postgres остается source of truth для `migration_job`, outbox, inbox, mappings и checkpoint’ов.

## Minimal smoke

После старта бота:

```bash
curl -i http://127.0.0.1:8080/status
```

Минимальный пользовательский flow:

```text
/connect +79990001122
/chats query=python
/configure -1002210509299 access=link format=quote
/migrate -1002210509299
/status -1002210509299
/stats
```
