# PushOccurrence

PushOccurrence — это высокопроизводительный сервис на Go, предназначенный для надежной доставки уведомлений о событиях базы данных (PostgreSQL) в очередь сообщений (RabbitMQ).

Система работает по принципу подписки на события PostgreSQL (LISTEN/NOTIFY), извлекает данные события из специальных таблиц и гарантирует отправку в RabbitMQ, используя механизм подтверждений (Publisher Confirms) и паттерн Outbox.

## 🚀 Возможности

*   Реактивность: Мгновенная реакция на события в БД через механизм LISTEN/NOTIFY.
*   Надежность:
    *   Использование FOR UPDATE SKIP LOCKED для предотвращения состояния гонки (race conditions) при параллельной обработке.
    *   Подтверждение доставки от RabbitMQ (Publisher Confirms).
    *   Для переотправки сообщений реализован паттерн transactional Outbox в локальной бд(mongo).
*   Масштабируемость: Возможность запуска нескольких экземпляров сервиса (благодаря блокировкам в БД).

## 🛠 Технические требования

*   Go: 1.25.4 или выше
*   PostgreSQL: С поддержкой схем и механизма NOTIFY.
*   RabbitMQ: Брокер сообщений с поддержкой протокола AMQP 0.9.1.

## 🏗 Архитектура и Принцип работы

1.  Событие в БД: Триггер вставляет запись в логирующие таблицы (message_queue_log, message_queue_log_data) и отправляет уведомление NOTIFY <channel> '<message_id>'.
2.  Слушатель (Listener): Сервис PushOccurrence, подписанный на канал, получает message_id.
3.  Обработчик (Handler):
    *   Блокирует соответствующую строку в message_queue_log для обработки.
    *   Извлекает тело сообщения из message_queue_log_data.
    *   Отправляет сообщение во внутренний канал отправки.
    *   Помечает запись в БД как transferred = true.
4.  Отправка (MQ Producer):
    *   Пытается отправить сообщение в RabbitMQ.
    *   Ожидает подтверждения (ACK) от брокера.
    *   В случае сбоя или отсутствия связи используем outbox в локальной бд.

## 🧠 Детали реализации пакета mq (internal/mq)

Реализация пакета построена на взаимодействии нескольких горутин, управляемых через каналы.

### Структура и Инициализация
Функция CreateMq(ctx, url, queue) инициализирует структуру Mq и запускает три основные горутины:
1.  Monitor(ctx) — Мониторинг состояния соединения с MQ.
2.  ConnectManager(ctx, url) — Управление подключением.
3.  MessageManager(ctx) — Управление сообщениями.

Поля структуры Mq включают каналы для синхронизации:
*   ConnectStatus (chan bool): Сигналы о состоянии соединения.
*   RePublishStatus (chan bool): Сигналы о состоянии соединения.
*   Messages (chan Message): Входящий поток сообщений от PostgreSQL.
*   Buffer (chan Message): Буфер для повторной отправки (емкость 100).

### 1. Управление соединением
Логика разделена на мониторинг и восстановление:

*   Monitor:
    *   Работает в отдельной горутине с тикером 5 секунд.
    *   Проверяет connected := conn != nil && ch != nil && !conn.IsClosed().
    *   Отправляет статус в каналы ConnectStatus и RePublishStatus: true (активно) или false (разорвано). Используется select с default для неблокирующей отправки.

*   connectManager:
    *   Слушает канал ConnectStatus.
    *   При получении false (разрыв) ждет 3 секунды и вызывает mq.connect(url).
    *   connect(url): Создает TCP-соединение (amqp.Dial), открывает канал, переводит его в режим подтверждений (Confirm(false)) и декларирует очередь.

### 2. Обработка сообщений
Центральным узлом является функция messageManager, которая слушает два канала:

*   Входящие сообщения (<-Messages):
    *   Если соединение активно: вызывает sendToRabbit(msg).
    *   Если соединение отсутствует: вызывает sendToBuffer(msg).

*   Сигнал восстановления (<-RePublishStatus):
    *   При получении true (соединение восстановлено) инициирует cleaningBuffer() для отправки накопленных сообщений.

### 3. Отправка и Буферизация (Sender)

*   sendToRabbit(msg):
    *   Публикует сообщение в очередь с ожиданием подтверждения (PublishWithDeferredConfirm).
    *   Если получен ACK — успех.
    *   Если получен NACK, ошибка сети или таймаут — вызывает sendToBuffer(msg).

*   sendToBuffer(msg):
    *   Пишет сообщение в канал Buffer.
    *   Если канал полон, сообщение отбрасывается (лог "buffer full").

*   cleaningBuffer:
    *   Вычитывает сообщения из Buffer и пытается отправить их (в данном контексте логика дублируется для исключения рекурсии sendToRabbit и возможности deadlock из за мутекса на sendToRabbit).
    *   Прекращает работу, если буфер пуст или при ошибке отправки.

### 4. Завершение работы
Метод Close() (вызывается через defer) корректно закрывает AMQP канал и соединение, используя мьютекс для безопасности.

### ⚠️ Текущие уязвимости (Known Issues)
*   OutBox реализован в локальной бд а не в основной. В следствии есть возможность потери сообщения при не доступности mongo, а также теряется атомарность(одно из ключевых приемуществ) этого подхода.

## 📦 Структура Базы Данных

Сервис ожидает наличие схемы data_exchange и следующих таблиц:

SQL

CREATE SCHEMA IF NOT EXISTS data_exchange;

-- Таблица статусов сообщений
CREATE TABLE IF NOT EXISTS data_exchange.message_queue_log (
    message_id UUID PRIMARY KEY, -- или другой уникальный идентификатор (string)
    transferred BOOLEAN DEFAULT FALSE,
    transfer_time TIMESTAMP
);

-- Таблица данных сообщений
CREATE TABLE IF NOT EXISTS data_exchange.message_queue_log_data (
    message_id UUID REFERENCES data_exchange.message_queue_log(message_id),
    message_body BYTEA -- или JSONB/TEXT
);


> Примечание: Типы данных для message_id должны соответствовать (в коде используется string).
## ⚙️ Конфигурация

Конфигурация находится в файле config/config.json. Пример структуры:

JSON

{
  "postgres": {
    "host": "localhost",
    "port": your_port,
    "database": "your_db_name",
    "user": "your_db_user",
    "password": "your_db_password",
    "ssl_mode": "disable"
  },

  "listener": {
    "channels": [
      "your_notify_channel"
    ]
  },

  "rabbitmq": {
    "host": "localhost",
    "port": your_port,
    "user": "guest",
    "password": "guest",
    
    "queue": {
      "name": "your_queue_name",
      "durable": true,
      "auto_delete": false,
      "exclusive": false,
      "no_wait": false
    },
    "retry": {
      "buffer_size": 100,
      "retry_delay_sec": 5,
      "idle_sleep_ms": 100
    }
  },

  "mongo": {
    "host": "localhost",
    "port": your_port,
    "database": "your_database"
    }
}


### Параметры

*   postgres: Параметры подключения к PostgreSQL.
*   rabbitmq:
    *   Параметры подключения к RabbitMQ.
    *   queue: Настройки очереди (должны совпадать с настройками в брокере).
*   listener:
    *   channels: Список каналов PostgreSQL (LISTEN channel_name), которые сервис будет слушать.
*   mongo: Параметры подключения к MongoDB. 

## ▶️ Запуск

1.  Убедитесь, что PostgreSQL, MongoDB и RabbitMQ запущены.
2.  Создайте таблицы в БД (см. раздел "Структура Базы Данных").
3.  Настройте config/config.json.
4.  Запустите сервис:

Bash

go run .cmd/app/


Или скомпилируйте бинарный файл:

Bash

go build -o bin/PushOccurrence .cmd/app
./bin/PushOccurrence


## 📂 Структура проекта

.  
├── cmd/app/main.go         # Точка входа в приложение    
├── config/                 # Загрузка конфигурации и JSON файл    
├── internal/  
│     ├── db/                 # Логика работы с БД (подключение, слушатель, main loop)         
│     ├── handlers/           # Обработка полученных уведомлений    
│     ├── mq/                 # Логика работы с RabbitMQ (подключение, буфер, отправка)    
│     └── service/            # Инициализация и старт сервиса       
└── go.mod                  # Зависимости  


## 🤝 Вклад в развитие (Contributing)

Проект использует стандартные инструменты Go.
- Линтер: golangci-lint (рекомендуется)
- Форматирование: go fmt
