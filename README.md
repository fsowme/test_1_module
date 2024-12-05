# test_1_module

## Запуск

### Запуск кластера:
```shell
docker-compose up
```

### Создание топика
```shell
docker compose exec kafka-0 kafka-topics.sh \
  --create --topic topic-1 --partitions 3 --replication-factor 3 \
  --bootstrap-server localhost:9092 --config min.insync.replicas=2
```

### Установка зависимостей
```shell
pip install -r requirements.txt
```

### Запуск продюсера
```shell
python app/producer.py
```

### Запуск консьюмеров
```shell
python app/consumer_pull.py
python app/consumer_push.py
```


## Проверка падения ноды кластера
### Запустить продюсера и консьюмеров
```shell
python app/consumer_pull.py
python app/consumer_push.py
```
### Отключить ноду от сети
```shell
docker network disconnect test_1_module_default test_1_module-kafka-1-1 test_1_module-kafka-0-1
```
Продюсер должен продолжать успешно отправлять сообщения, а консьюмеры потреблять. Текущая конфигурация кластера 
выдерживает падения 1 ноды
