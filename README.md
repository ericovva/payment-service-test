# Асинхронный сервис процессинга платежей

Тестовый сервис на Python 3.12, FastAPI / Pydantic v2, SQLAlchemy 2 async,
PostgreSQL, RabbitMQ / FastStream и Alembic. Платёжный шлюз эмулируется:
обработка занимает 2–5 секунд, вероятность успеха отдельной попытки — 90%.
Реальные списания денег не выполняются.

## Быстрый запуск

Нужны Docker Engine и Docker Compose v2.

```bash
cp .env.example .env
# Замените API_KEY в .env на случайный ключ длиной не менее 16 символов:
python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

Укажите в `.env` точный HTTPS-хост вашего webhook-приёмника:

```dotenv
WEBHOOK_ALLOWED_HOSTS=["hooks.example.com"]
WEBHOOK_ALLOW_HTTP=false
```

`hooks.example.com` — пример, а не готовый сервер. Нужен ваш работающий
приёмник, возвращающий HTTP 2xx. По умолчанию список пуст: ни один адрес
webhook не разрешён. Для локального тестового HTTP-приёмника можно явно
добавить его хост и включить `WEBHOOK_ALLOW_HTTP=true`; это не настройка
для публичного развёртывания. `localhost` внутри consumer — сам контейнер,
а не компьютер разработчика.

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f api consumer
```

Compose поднимает ровно четыре сервиса: `postgres`, `rabbitmq`, `api`,
`consumer`. API ждёт готовности БД/брокера, применяет миграции, затем
запускает HTTP-сервер. Consumer стартует после успешной проверки API,
поэтому миграции уже выполнены. Только API доступен с хоста, и только
на `127.0.0.1:8000`. БД и брокер сохраняют данные в именованных volumes.

```bash
docker compose down
# Полный сброс тестовых данных (необратимо):
# docker compose down -v
```

## API

Все эндпоинты, включая `/health`, требуют `X-API-Key`. Укажите тот же ключ,
который записан в `.env`:

```bash
export API_KEY='ваш-ключ-из-env'
curl --fail-with-body http://localhost:8000/health \
  -H "X-API-Key: $API_KEY"
```

Создание платежа:

```bash
curl --fail-with-body -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-1001' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Заказ 1001",
    "metadata": {"order_id": "1001"},
    "webhook_url": "https://hooks.example.com/payments"
  }'
```

Ответ — `202 Accepted`, `payment_id`, `status`, `created_at`.
Сумма передаётся строкой, чтобы не терять точность decimal.
Валюты: `RUB`, `USD`, `EUR`.

```bash
export PAYMENT_ID='uuid-из-ответа'
curl --fail-with-body "http://localhost:8000/api/v1/payments/$PAYMENT_ID" \
  -H "X-API-Key: $API_KEY"
```

Повторите POST с тем же ключом и тем же телом: новый платёж не создаётся.
Использование того же ключа с другим содержимым возвращает конфликт.
Платёж принимает статусы `pending`, `succeeded`, `failed`.

## Как устроены доставка и повторы

1. API сохраняет платёж и запись `outbox` в одной транзакции PostgreSQL.
   Нет окна, в котором платёж уже принят, но событие потеряно из-за
   недоступности RabbitMQ. Уникальное ограничение на idempotency key
   защищает также от конкурентных POST.
2. Фоновый publisher в процессе consumer читает готовые записи outbox с
   `FOR UPDATE SKIP LOCKED`, публикует persistent-события в durable-очередь
   `payments.new` и отмечает публикацию только после publisher confirm.
3. Единственный обработчик загружает платёж, эмулирует шлюз, сохраняет
   результат и отправляет webhook. Повторная доставка терминального
   платежа не вызывает повторную оплату; незавершённая доставка webhook
   может быть повторена.
4. Исключения при обращении к шлюзу или отправке webhook допускают всего три попытки обработки,
   с экспоненциальными задержками 1 и 2 секунды перед повторными попытками.
   Число попыток хранится в БД. Повторы планируются через outbox, поэтому
   перезапуск процесса не стирает таймеры и счётчик.
5. После третьей ошибки событие для `payments.dlq` также записывается
   через outbox. Ошибка инфраструктуры БД не считается бизнес-попыткой:
   исходное сообщение возвращается в очередь до восстановления БД.

10% ответов эмулятора — штатный терминальный статус `failed`, а не
транспортное исключение. Такой результат отправляется клиенту через webhook
и не вызывает повторную оплату.

### Формат webhook

POST с JSON, заголовками `X-Event-ID` и `Idempotency-Key`, содержащими
один стабильный идентификатор `payment.<payment_id>.completed`:

```json
{
  "event_id": "payment.<payment_id>.completed",
  "payment_id": "<payment_id>",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "metadata": {"order_id": "1001"},
  "processed_at": "2026-10-24T12:00:00+00:00"
}
```

Ответ 2xx подтверждает доставку; HTTP-ошибки и сетевые исключения
вызывают повтор. Редиректы не выполняются и считаются ошибкой.

### Важные ограничения

- Гарантия — **at least once**, не exactly once. Если брокер подтвердил
  публикацию, но процесс упал до фиксации outbox, событие будет опубликовано
  повторно. Consumer должен выдерживать такие дубли.
- Приёмник webhook должен дедуплицировать уведомления по стабильному ключу
  события: сетевой timeout не доказывает, что получатель не обработал запрос.
  Нельзя атомарно зафиксировать PostgreSQL и HTTP-вызов чужого сервиса.
- Ошибка webhook не отменяет уже успешный платёж. DLQ означает проблему
  обработки/доставки, а не обязательно неуспешное списание.
- Случайный шлюз — демонстрация. В реальной интеграции потребуется
  idempotency key на стороне провайдера и сверка неопределённых результатов.
- Consumer в этом задании один. Перед масштабированием нужно отдельно
  проверить конкурентную обработку одного платежа и lease/locking.

## Безопасность

API key берётся из окружения; пример из `.env.example` предназначен только
для локального запуска. `.env` не следует коммитить. Webhook требует HTTPS
(кроме явно включённого локального режима), точного allowlist хостов и не
следует редиректам. Это снижает SSRF-риск, но не заменяет production-защиту:
нужны контроль исходящего трафика, DNS/IP-проверки против rebinding,
блокировка private/loopback/link-local/metadata IP и доверенный список
получателей. Не добавляйте произвольные пользовательские хосты в allowlist.

Для production также нужны TLS на входе, безопасные пароли БД/брокера,
хранилище секретов, подпись webhook, rate limiting, наблюдаемость и
политика очистки outbox / хранения idempotency keys. Тестовые credentials
`payments:payments` нельзя использовать публично.

## Разработка и проверки

Установите Python 3.12 и uv; зависимости фиксируются в `uv.lock`.

```bash
uv sync --frozen
uv run pytest -q
uv run ruff check .
```

Юнит-тесты используют отдельную SQLite БД и не требуют Docker. Проверка
конкурентных запросов с настоящим PostgreSQL выполняется отдельно:

```bash
TEST_DATABASE_URL='postgresql+asyncpg://payments:payments@localhost:5432/payments' \
  uv run python -m pytest tests/test_api.py -m integration -q
```

Нужен доступный PostgreSQL и пользователь с правами CREATE/DROP SCHEMA:
тест создаёт уникальную временную схему, не удаляя существующие таблицы.
Compose по умолчанию не публикует порт PostgreSQL; для запуска теста с хоста
используйте отдельную тестовую БД или временный Compose override с
`ports: ["127.0.0.1:5432:5432"]` у сервиса `postgres`.
Проверка настоящего RabbitMQ (publisher confirms, persistent-сообщения,
mandatory/unroutable и DLQ) тоже включается явно:

```bash
TEST_RABBITMQ_URL='amqp://payments:payments@localhost:5672/' \
  uv run python -m pytest tests/test_broker.py -m integration -q
```

Нужен доступный тестовый брокер; для Compose временно опубликуйте
`127.0.0.1:5672:5672` у `rabbitmq`. Тест создаёт и удаляет только свои
очереди с уникальным префиксом, не трогая очереди приложения. Если переменные
`TEST_DATABASE_URL` / `TEST_RABBITMQ_URL` не заданы, соответствующие
интеграционные тесты пропускаются. Остальные тесты используют SQLite,
моки и тестовый транспорт FastStream, а не настоящую инфраструктуру.

Миграции в работающем окружении:

```bash
docker compose exec api alembic current
docker compose exec api alembic upgrade head
```

## Структура

Сначала выделяется приложение, внутри него — слои. Общие настройки и
инфраструктура вынесены отдельно; новые предметные приложения можно
добавлять рядом с `payments`.

```text
app/
├── main.py                 # сборка FastAPI и общие HTTP-эндпоинты
├── core/                   # настройки и аутентификация
├── payments/
│   ├── api/                # HTTP-маршруты платежей
│   ├── schemas/            # входные/выходные схемы и события
│   ├── models/             # модель Payment
│   ├── repositories/       # создание платежа с transactional outbox
│   ├── services/           # обработка платежа и политика повторов
│   └── integrations/       # эмулятор шлюза и webhook-клиент
└── infrastructure/
    ├── database/           # engine, сессии, Base и модель Outbox
    └── messaging/          # RabbitMQ consumer и outbox publisher
```

- `migrations/` — Alembic и исходная схема `payments` / `outbox`.
- `tests/` — проверки API, идемпотентности, обработки и брокера.
- `compose.yaml` — PostgreSQL, RabbitMQ, API и один consumer.

Точки входа: `uvicorn app.main:app` и
`python -m app.infrastructure.messaging.worker`.

## Проверка сценариев вручную

1. Отправьте POST и затем GET: сначала `pending`, затем терминальный статус.
2. Повторите тот же POST: тот же ID и отсутствие повторного создания.
3. Для проверки retry используйте ваш webhook-приёмник, отвечающий HTTP 500:
   после трёх ошибок сообщение должно попасть в `payments.dlq`.
4. Остановите брокер после запуска сервисов, создайте платёж, затем запустите
   брокер снова: outbox должен доставить сохранённое событие после восстановления.
5. Перезапустите consumer между попытками: счётчик и отложенный повтор
   должны сохраниться в PostgreSQL.

Логи: `docker compose logs -f consumer`. Проверить состояние очередей:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```
