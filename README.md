
Система состоит из трёх компонентов:

 **Сервис-Брокер (СБ)** | приём и append-only хранение сообщений в файлах .log | 8080 
 **Менеджер Очередей (МО)** | учёт offset по группам подписчиков, ACK, статус брокеров | 8081 
 **SDK**  библиотека для издателей и подписчиков 




- **Издатель** пишет напрямую в брокер: `POST /topics/{topic}/messages`
- **Подписчик** опрашивает менеджер: `POST /topics/{topic}/groups/{group}/poll`
- Менеджер назначает offset группе, читает тело сообщения у брокера и возвращает клиенту
- После обработки подписчик отправляет **ACK**: `POST .../ack`
- Offset группы хранится в `registry.json` (переживает перезапуск)

## старт (Docker)

```bash
docker compose up -d --build
```

Проверка:

```bash
curl http://localhost:8081/status
curl http://localhost:8080/health
```

### Публикация и подписка

```bash
# Отправить сообщение
docker compose run --rm publisher //app/publisher \
  -broker=http://broker:8080 -manager=http://manager:8081 \
  -topic=orders -message='{"order_id":1}'

# Подписчик (polling)
docker compose run --rm subscriber //app/subscriber \
  -broker=http://broker:8080 -manager=http://manager:8081 \
  -topic=orders -group=warehouse
```


## API

### Брокер (`:8080`)
 GET  `/health`  Health check 
 GET  `/topics` Список топиков 
 GET  `/topics/{topic}`  Количество сообщений 
 POST  `/topics/{topic}/messages`  Запись сообщения (200 OK) 
 GET  `/topics/{topic}/messages?offset=N&limit=M`  Чтение с offset 

### Менеджер (`:8081`)
 GET  `/status`  Статус брокеров, топиков и групп 
 POST  `/brokers/register`  Регистрация брокера 
 POST  `/topics/{topic}/groups/{group}/poll` Получить новые сообщения 
 POST  `/topics/{topic}/groups/{group}/ack`  Подтвердить обработку 
 GET  `/topics/{topic}/groups/{group}/offset`  Текущий committed offset 

## Демонстрация тест-кейсов

Автоматические сценарии:
```bash
# Multiple Groups — полностью автоматический
docker compose run --rm publisher //app/demo multiple-groups \
  -broker=http://broker:8080 -manager=http://manager:8081

# Load Balancing — два подписчика в одной группе
docker compose run --rm publisher //app/demo load-balance \
  -broker=http://broker:8080 -manager=http://manager:8081

# Restart from Offset
docker compose run --rm publisher //app/demo restart-offset \
  -broker=http://broker:8080 -manager=http://manager:8081
```

### Data Safety (1000 сообщений + kill -9)
```bash
# 1. Опубликовать 1000 сообщений
docker compose run --rm publisher //app/publisher \
  -broker=http://broker:8080 -manager=http://manager:8081 \
  -topic=data-safety -count=1000 -prefix=msg

# 2. «Убить» брокер
docker compose kill -s SIGKILL broker

# 3. Запустить снова (данные в volume broker-data)
docker compose up -d broker

# 4. Прочитать все сообщения
docker compose run --rm subscriber //app/subscriber \
  -broker=http://broker:8080 -manager=http://manager:8081 \
  -topic=data-safety -group=read-all -max=1000 -interval=100ms
```



Терминал 3 — 10 сообщений:

```bash
go run ./cmd/publisher -topic=lb -count=10
```



## SDK (пример)

```go
client := sdk.NewClient("http://localhost:8080", "http://localhost:8081")

pub := sdk.NewPublisher(client)
msg, _ := pub.Publish("orders", `{"id":1}`)

sub := sdk.NewSubscriber(client, "orders", "group-a", "consumer-1")
messages, _ := sub.Poll(10)
for _, m := range messages {
    // обработка m.Payload
    sub.Ack(m.Offset)
}
```



