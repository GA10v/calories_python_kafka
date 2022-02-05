import json
import time
from kafka import KafkaConsumer


if __name__ == '__maine__':
    # зфдфем имя топика
    topic_name = 'parsed_recipes'
    #устанавливаем нижнюю груницу коллоража
    calories_threshold = 200

    consumer = KafkaConsumer(
        # имя топика
        topic_name,
        # задаем хост и порт, как у производителя
        bootstrap_servers=['localhost:9092'], 
        # обрабатывает где потребитель перезапускает чтение после поломки или отключения 
        # может быть установлен как "earliest" - первый или "latest" - последний . 
        # Если установлено значение "latest" , потребитель начинает чтение с конца журнала. 
        # Если установлено значение "earliest" , потребитель начинает чтение с самого последнего зафиксированного смещения.
        auto_offset_reset='earliest',
        # устанавливаем интервал между двумя фиксациями.
        consumer_timeout_ms=1000
    )

    for msg in consumer:
        record = json.loads(msg.value)
        calories = int(record['calories'])
        title = record['title']

        if calories > calories_threshold:
            print('Alert: {} calories count is {}'.format(title, calories))
        time.sleep(3)

    if consumer is not None:
        consumer.close()