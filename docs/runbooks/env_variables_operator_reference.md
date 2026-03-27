# Production Env Operator Guide

## Goal

Короткая операторская версия без тюнинговых параметров.

Здесь только то, что реально нужно заполнить, чтобы production deployment вообще поднялся.
Полный справочник с optional-параметрами лежит в [env_variables_reference.md](/Users/aleksandrosovskii/exTG/docs/runbooks/env_variables_reference.md).

Готовый secure-by-default target шаблон лежит в [.env.prod.target.example](/Users/aleksandrosovskii/exTG/.env.prod.target.example).

## Минимальный набор для production

### 1. Compose / deploy

| Переменная | Откуда взять |
|---|---|
| `COMPOSE_PROJECT_NAME` | задать вручную, обычно короткое имя сервиса |
| `EXTG_IMAGE` | из Docker registry tag, который собрал CI |
| `POSTGRES_DB` | выбрать вручную |
| `POSTGRES_USER` | выбрать вручную |
| `POSTGRES_PASSWORD` | сгенерировать и положить в secret manager |

### 2. Обязательные runtime secrets

| Переменная | Откуда взять |
|---|---|
| `EXTG_POSTGRES__DSN` | собрать из `POSTGRES_*` и host базы |
| `EXTG_SECURITY__MESSAGE_HMAC_SECRET` | сгенерировать как длинный случайный секрет |
| `EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY` | сгенерировать как отдельный длинный случайный секрет |
| `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN` | сгенерировать как internal shared secret между сервисами |

### 3. Telegram application credentials

| Переменная | Откуда взять |
|---|---|
| `EXTG_TELEGRAM__API_ID` | из Telegram application на [my.telegram.org](https://my.telegram.org/) |
| `EXTG_TELEGRAM__API_HASH` | из того же Telegram application |

Важно:

- это не per-user credentials;
- это одна пара app credentials на окружение;
- сами операторы потом логинятся уже своими user sessions поверх этого приложения.

### 4. Telethon service routing

| Переменная | Откуда взять |
|---|---|
| `EXTG_TELETHON_SERVICE__BASE_URL` | внутренний URL `telethon-service`, например `http://telethon-service:8092` |

### 5. eXpress / BotX

| Переменная | Откуда взять |
|---|---|
| `EXTG_EXPRESS__ACCOUNTS` | собрать JSON из bot entities в eXpress/BotX: `bot_id`, `cts_url`, `secret_key`, `role` |

Если multi-account mesh не используете, вместо этого можно задать legacy trio:

- `EXTG_EXPRESS__BOT_ID`
- `EXTG_EXPRESS__CTS_URL`
- `EXTG_EXPRESS__SECRET_KEY`

Но для production target рекомендуется именно `EXTG_EXPRESS__ACCOUNTS`.

### 6. Bot runtime

| Переменная | Откуда взять |
|---|---|
| `EXTG_BOT__MIGRATION_ID` | задать вручную, стабильный logical namespace |
| `EXTG_BOT__VERIFY_REQUESTS` | вручную, для production должно быть `true` |

Практически полезно заполнить сразу:

- `EXTG_BOT__OPERATOR_HUIDS` из HUID операторов в eXpress

### 7. Kafka

Если command plane у вас идет через Kafka, еще нужен минимум:

| Переменная | Откуда взять |
|---|---|
| `EXTG_KAFKA__ENABLED` | вручную, `true` |
| `EXTG_KAFKA__BOOTSTRAP_SERVERS` | у infra/platform team |
| `EXTG_KAFKA__COMMAND_TOPIC` | по naming policy проекта |
| `EXTG_KAFKA__RESULT_TOPIC` | по naming policy проекта |
| `EXTG_KAFKA__DLQ_TOPIC` | по naming policy проекта |

## Рекомендуемый порядок заполнения

1. Скопировать [.env.prod.target.example](/Users/aleksandrosovskii/exTG/.env.prod.target.example) в `.env.prod`.
2. Заполнить `POSTGRES_*` и `EXTG_POSTGRES__DSN`.
3. Заполнить три секрета:
   `EXTG_SECURITY__MESSAGE_HMAC_SECRET`,
   `EXTG_SECURITY__TELEGRAM_SESSION_MASTER_KEY`,
   `EXTG_TELETHON_SERVICE__INTERNAL_TOKEN`.
4. Заполнить `EXTG_TELEGRAM__API_ID` и `EXTG_TELEGRAM__API_HASH`.
5. Собрать и вставить `EXTG_EXPRESS__ACCOUNTS`.
6. Указать `EXTG_BOT__OPERATOR_HUIDS`.
7. Если используете Kafka, заполнить Kafka block.

## Что я рекомендую как target profile

- dedicated `telethon-service`
- `EXTG_EXPRESS__ACCOUNTS` вместо legacy single-bot mode
- `EXTG_BOT__VERIFY_REQUESTS=true`
- `EXTG_POSTGRES__PERSIST_MESSAGE_BODIES=false`
- `EXTG_POSTGRES__PAYLOAD_STORAGE_MODE=redacted`
- `EXTG_KAFKA__ENABLED=true`
- `EXTG_BOT__FSM_STORAGE_BACKEND=in_memory` как secure-by-default вариант

Трейд-офф по FSM:

- `in_memory` безопаснее, потому что wizard/auth state не пишется в БД;
- `postgres` устойчивее к рестартам;
- если нужен максимально безопасный профиль, оставлять `in_memory`;
- если нужен максимально непрерывный operator UX при рестартах, можно осознанно переключить на `postgres`.
