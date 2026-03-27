# Env Variables Reference

## Goal

Справочник по env-переменным для этого репозитория:

- какие переменные реально используются;
- в каком шаблоне они уже есть;
- для каких сервисов они нужны;
- откуда брать каждое значение.

Шаблоны:

- локальный app/runtime: [.env.example](/Users/aleksandrosovskii/exTG/.env.example)
- локальный Docker e2e: [.env.e2e.example](/Users/aleksandrosovskii/exTG/.env.e2e.example)
- production Docker/compose: [.env.prod.example](/Users/aleksandrosovskii/exTG/.env.prod.example)

Правило:

- значения из категории `секрет` не коммитить;
- реальные `.env`, `.env.e2e`, `.env.prod` должны жить только локально или в secret storage CI/CD;
- значения с префиксом `EXTG_` читает приложение;
- переменные без `EXTG_` в `.env.prod` нужны compose/deploy-слою.

## Profile Map

| Профиль | Для чего | Базовый шаблон |
|---|---|---|
| local shell runtime | запуск `extg-bot`, `extg-worker`, `extg-telethon-service` без Docker | [.env.example](/Users/aleksandrosovskii/exTG/.env.example) |
| local docker e2e | полный локальный стек через compose | [.env.e2e.example](/Users/aleksandrosovskii/exTG/.env.e2e.example) |
| production docker compose | deployment через [docker-compose.prod.yml](/Users/aleksandrosovskii/exTG/docker-compose.prod.yml) | [.env.prod.example](/Users/aleksandrosovskii/exTG/.env.prod.example) |

## Infra And Deploy Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `COMPOSE_PROJECT_NAME` | production compose | обязательно для prod | задать вручную как короткое имя сервиса/проекта, обычно slug сервиса |
| `EXTG_IMAGE` | production compose, GitLab deploy | обязательно для prod | из Docker registry, обычно `${CI_REGISTRY_IMAGE}:${CI_COMMIT_SHA}` или release tag |
| `POSTGRES_DB` | production compose | обязательно для prod | выбрать вручную, обычно имя сервиса |
| `POSTGRES_USER` | production compose | обязательно для prod | выбрать вручную, обычно service account БД |
| `POSTGRES_PASSWORD` | production compose | обязательно для prod | сгенерировать в secret manager или CI/CD secret |
| `KAFKA_EXTERNAL_HOST` | production compose | опционально | DNS/host Kafka для внешнего порта, если нужен внешний доступ |
| `KAFKA_EXTERNAL_PORT` | production compose | опционально | выбрать вручную, обычно `9094` |
| `EXTG_BOT_PUBLISHED_PORT` | production compose | опционально | выбрать вручную, обычно `8000` |
| `TRAEFIK_ENABLED` | production compose | опционально | `true/false` в зависимости от ingress-схемы |
| `TRAEFIK_BOT_HOST` | production compose | если включен Traefik | домен вебхука/бота из ingress/DNS |
| `EXTG_KAFKA_TOPIC_PARTITIONS` | production compose | опционально | выбрать вручную по ожидаемой нагрузке; стартово `10` |

## Common Runtime Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_ENVIRONMENT` | все сервисы | опционально | задать вручную: `local`, `test`, `production`; в prod лучше `production` |
| `EXTG_TIMEZONE_NAME` | все сервисы | опционально | задать вручную, обычно timezone команды или сервера, например `Europe/Moscow` |

## Postgres Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_POSTGRES__DSN` | `bot`, `worker`, `telethon-service` | обязательно вне `local/test` | составить из host/user/password/db выданных DBA, managed Postgres или compose |
| `EXTG_POSTGRES__ECHO` | все сервисы | опционально | выставить вручную, обычно `false`; `true` только для отладки |
| `EXTG_POSTGRES__POOL_SIZE` | все сервисы | опционально | выбрать вручную по нагрузке и лимитам БД |
| `EXTG_POSTGRES__MAX_OVERFLOW` | все сервисы | опционально | выбрать вручную по лимитам БД и burst-нагрузке |
| `EXTG_POSTGRES__PERSIST_MESSAGE_BODIES` | все сервисы | опционально | выставить вручную; для production рекомендовано `false` |
| `EXTG_POSTGRES__PAYLOAD_STORAGE_MODE` | все сервисы | опционально | выставить вручную: `redacted` для production, `full` только для controlled debug |

## Security Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_SECURITY__MESSAGE_HMAC_SECRET` | все сервисы | обязательно вне `local/test` | сгенерировать как длинный случайный секрет в secret manager |
| `EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY` | все сервисы | обязательно вне `local/test` | сгенерировать как отдельный длинный секрет в secret manager |
| `EXTG_SECURITY__TELEGRAM_SESSION_KEY_VERSION` | все сервисы | опционально | задать вручную, обычно `v1`; менять только при ротации ключа |

## Telegram / Telethon Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_TELEGRAM__API_ID` | `telethon-service` или локальный runtime с Telethon | обязательно, если используется Telethon | взять в [Telegram API development tools](https://my.telegram.org/) для приложения |
| `EXTG_TELEGRAM__API_HASH` | `telethon-service` или локальный runtime с Telethon | обязательно, если используется Telethon | взять в [Telegram API development tools](https://my.telegram.org/) рядом с `api_id` |
| `EXTG_TELEGRAM__REQUEST_TIMEOUT_SECONDS` | Telethon path | опционально | выставить вручную по latency Telegram |
| `EXTG_TELEGRAM__CONNECTION_RETRIES` | Telethon path | опционально | выставить вручную; стартово оставить default |
| `EXTG_TELEGRAM__DEFAULT_ATTACHMENT_DIR` | Telethon path | опционально | указать вручную только если нужен отдельный каталог временных загрузок |
| `EXTG_TELEGRAM__ARCHIVE_IMPORT_DIR` | archive import path | опционально | указать вручную; для Docker уже используется `/tmp/extg-archive-imports` |
| `EXTG_TELEGRAM__ATTACHMENT_TEMP_FILE_CLEANUP_ENABLED` | Telethon attachment flow | опционально | выставить вручную, обычно `true` |
| `EXTG_TELEGRAM__ATTACHMENT_TEMP_FILE_TTL_SECONDS` | Telethon attachment flow | опционально | задать вручную по retention-политике temp files |
| `EXTG_TELEGRAM__ATTACHMENT_TEMP_FILE_CLEANUP_INTERVAL_SECONDS` | Telethon attachment flow | опционально | задать вручную по housekeeping cadence |
| `EXTG_TELEGRAM__ARCHIVE_IMPORT_CLEANUP_ENABLED` | archive import path | опционально | выставить вручную, обычно `true` |
| `EXTG_TELEGRAM__ARCHIVE_IMPORT_TTL_SECONDS` | archive import path | опционально | задать вручную по retention-политике staged archive files |
| `EXTG_TELEGRAM__ARCHIVE_IMPORT_CLEANUP_INTERVAL_SECONDS` | archive import path | опционально | задать вручную |

## Telethon Service Boundary Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_TELETHON_SERVICE__BASE_URL` | `bot`, `worker` | обязательно, если Telethon вынесен отдельно | задать из внутреннего service URL, например `http://telethon-service:8092` |
| `EXTG_TELETHON_SERVICE__HOST` | `telethon-service` | опционально | задать вручную: `127.0.0.1` локально, `0.0.0.0` в контейнере |
| `EXTG_TELETHON_SERVICE__PORT` | `telethon-service`, callers | опционально | задать вручную, обычно `8092` |
| `EXTG_TELETHON_SERVICE__REQUEST_TIMEOUT_SECONDS` | `bot`, `worker`, `telethon-service` | опционально | задать вручную по внутренней сети |
| `EXTG_TELETHON_SERVICE__ATTACHMENT_REQUEST_TIMEOUT_SECONDS` | callers Telethon service | опционально | задать вручную, если download вложений занимает заметно дольше обычных RPC |
| `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN` | все сервисы, которые говорят с `telethon-service` | рекомендуется, в prod обязательно | сгенерировать как отдельный internal secret в secret manager |

## eXpress / BotX Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_EXPRESS__BOT_ID` | legacy single-account mode | обязательно только в legacy mode | взять в BotX/eXpress admin для конкретной bot entity |
| `EXTG_EXPRESS__CTS_URL` | legacy single-account mode | обязательно только в legacy mode | взять из адреса CTS, на котором зарегистрирован бот |
| `EXTG_EXPRESS__SECRET_KEY` | legacy single-account mode | обязательно только в legacy mode | взять в настройках bot entity / bot key |
| `EXTG_EXPRESS__ACCOUNTS` | multi-account mode | рекомендуется, для multi-CTS обязательно | собрать JSON-массив из bot entities: `bot_id`, `cts_url`, `secret_key`, `role`, опционально `bot_huid`, `visible` |
| `EXTG_EXPRESS__CHAT_TYPE` | delivery/provisioning | опционально | задать вручную, обычно `GROUP_CHAT` |
| `EXTG_EXPRESS__DEFAULT_PARTICIPANT_HUIDS` | provisioning | опционально | взять из eXpress HUID системных/default участников, если нужны always-added users |
| `EXTG_EXPRESS__REQUEST_TIMEOUT_SECONDS` | all eXpress API calls | опционально | задать вручную по latency CTS |
| `EXTG_EXPRESS__LOCAL_IDEMPOTENCY_CACHE_ENABLED` | `bot`, `worker` | опционально | выставить вручную; обычно `true` |

### Как собрать `EXTG_EXPRESS__ACCOUNTS`

На каждый CTS нужен один объект:

- `role=primary` для основного user-facing bot account;
- `role=technical` для helper bot account;
- `bot_id` и `secret_key` берутся из регистрации конкретного BotX-бота;
- `cts_url` это базовый URL соответствующего CTS;
- `bot_huid` опционален и может быть не задан на старте;
- `visible=false` ставить helper-ботам, если они не должны быть видны пользователям.

## Bot Runtime Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_BOT__MIGRATION_ID` | `bot`, `worker` | обязательно | задать вручную; это стабильный logical migration namespace |
| `EXTG_BOT__HOST` | `bot` | опционально | задать вручную: `127.0.0.1` локально, `0.0.0.0` в контейнере |
| `EXTG_BOT__PORT` | `bot` | опционально | задать вручную, обычно `8000` в Docker |
| `EXTG_BOT__EVENTS_PATH` | `bot` | опционально | задать вручную, обычно `/command` |
| `EXTG_BOT__STATUS_PATH` | `bot` | опционально | задать вручную, обычно `/status` |
| `EXTG_BOT__CALLBACKS_PATH` | `bot` | опционально | задать вручную, обычно `/notification/callback` |
| `EXTG_BOT__OPERATOR_HUIDS` | `bot` | рекомендуется | взять из eXpress HUID операторов, которым разрешено управлять миграциями |
| `EXTG_BOT__VERIFY_REQUESTS` | `bot` | обязательно для production | выставить вручную: `true` в prod, `false` только локально |
| `EXTG_BOT__FSM_STORAGE_BACKEND` | `bot` | опционально | выбрать вручную: `in_memory` для simple/local, `postgres` если нужен persistence state |

## Worker Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_WORKER__ENABLED` | `worker` | опционально | обычно `true`; вручную |
| `EXTG_WORKER__CONCURRENCY` | `worker` | опционально | выбрать по CPU, Postgres, eXpress и Telethon limits |
| `EXTG_WORKER__POLL_INTERVAL_SECONDS` | `worker` | опционально | вручную, обычно default |
| `EXTG_WORKER__LEASE_DURATION_SECONDS` | `worker` | опционально | вручную, обычно default |
| `EXTG_WORKER__HEARTBEAT_INTERVAL_SECONDS` | `worker` | опционально | вручную, обычно default |
| `EXTG_WORKER__BACKPRESSURE_MAX_QUEUED_JOBS` | `worker` | опционально | выбрать по операционному лимиту очереди |
| `EXTG_WORKER__BACKPRESSURE_MAX_RUNNING_JOBS` | `worker` | опционально | выбрать по реальной емкости воркеров |
| `EXTG_WORKER__RUNTIME_METRICS_ENABLED` | `worker` | опционально | вручную, обычно `true` |
| `EXTG_WORKER__RUNTIME_METRICS_POLL_INTERVAL_SECONDS` | `worker` | опционально | вручную |
| `EXTG_WORKER__OUTBOX_PUBLISHER_ENABLED` | `worker` | опционально | вручную, обычно `true` |
| `EXTG_WORKER__OUTBOX_PUBLISHER_BATCH_SIZE` | `worker` | опционально | вручную, по throughput |
| `EXTG_WORKER__OUTBOX_PUBLISHER_POLL_INTERVAL_SECONDS` | `worker` | опционально | вручную |
| `EXTG_WORKER__OUTBOX_PUBLISHER_LEASE_DURATION_SECONDS` | `worker` | опционально | вручную |
| `EXTG_WORKER__ATTACHMENT_MAX_UPLOAD_SIZE_BYTES` | `worker` | опционально | выставить по BotX/API лимиту, сейчас production default `100 MiB` |
| `EXTG_WORKER__ATTACHMENT_TRANSFER_CONCURRENCY` | `worker` | опционально | выбрать по I/O и rate-limit профилю |
| `EXTG_WORKER__ATTACHMENT_SPOOL_MAX_MEMORY_BYTES` | `worker` | опционально | выбрать по memory budget контейнера |

## Kafka Variables

| Переменная | Где нужна | Обязательность | Откуда взять |
|---|---|---|---|
| `EXTG_KAFKA__ENABLED` | `worker` | опционально | вручную: `true` если command plane идет через Kafka |
| `EXTG_KAFKA__BOOTSTRAP_SERVERS` | `worker` | обязательно, если Kafka включена | адреса Kafka brokers от infra/platform team |
| `EXTG_KAFKA__COMMAND_TOPIC` | `worker`, `create-topics` | обязательно, если Kafka включена | выбрать вручную по naming policy проекта |
| `EXTG_KAFKA__RESULT_TOPIC` | `worker`, `create-topics` | обязательно, если Kafka включена | выбрать вручную по naming policy проекта |
| `EXTG_KAFKA__DLQ_TOPIC` | `worker`, `create-topics` | обязательно, если Kafka включена | выбрать вручную по naming policy проекта |
| `EXTG_KAFKA__CONSUMER_GROUP` | `worker` | опционально | выбрать вручную по deployment naming |
| `EXTG_KAFKA__PRODUCER_CLIENT_ID` | `worker` | опционально | выбрать вручную |
| `EXTG_KAFKA__CONSUMER_CLIENT_ID` | `worker` | опционально | выбрать вручную |
| `EXTG_KAFKA__CONSUMER_NAME` | `worker` | опционально | выбрать вручную |
| `EXTG_KAFKA__POLL_TIMEOUT_SECONDS` | `worker` | опционально | вручную |
| `EXTG_KAFKA__PRODUCER_FLUSH_TIMEOUT_SECONDS` | `worker` | опционально | вручную |

## Minimal Production Fill Order

1. Заполнить infra/deploy переменные:
   `COMPOSE_PROJECT_NAME`, `EXTG_IMAGE`, `POSTGRES_*`, `TRAEFIK_*`, `KAFKA_*`.
2. Заполнить secrets:
   `EXTG_SECURITY__MESSAGE_HMAC_SECRET`,
   `EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY`,
   `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN`.
3. Заполнить Telegram app credentials:
   `EXTG_TELEGRAM__API_ID`,
   `EXTG_TELEGRAM__API_HASH`.
4. Заполнить BotX/eXpress bot mesh:
   `EXTG_EXPRESS__ACCOUNTS`.
5. Заполнить operator allowlist:
   `EXTG_BOT__OPERATOR_HUIDS`.
6. Проверить Kafka block, если Kafka включена.

## Notes

- `EXTG_POSTGRES__DSN`, `EXTG_SECURITY__MESSAGE_HMAC_SECRET` и `EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY` обязательны в production.
- `EXTG_EXPRESS__ACCOUNTS` предпочтительнее legacy trio `BOT_ID/CTS_URL/SECRET_KEY`.
- `EXTG_BOT__VERIFY_REQUESTS=false` допустим только локально.
- Если используется выделенный `telethon-service`, у `bot` и `worker` должен быть одинаковый `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN`.
- Значения вроде `pool_size`, `concurrency`, `request_timeout_seconds` не “выдаются” извне; их подбирают по нагрузке и ограничениям среды.
