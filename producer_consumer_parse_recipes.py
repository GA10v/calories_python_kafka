# -*- coding: utf-8 -*-
import json
import time
from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer

from producer_raw_recipies import publish_msg, connect_kafka_producer

def parse(markup):
    title = '-'
    submit_by = '-'
    description = '-'
    calories = 0
    ingredients = []
    rec = {}

    try:

        soup = BeautifulSoup(markup, 'lxml')
        # название рецепта
        title_section = soup.select('.headline heading-content elementFont__display')
        # автор рецепта
        submitter_section = soup.select('.author-name author-text__block elementFont__detailsLinkOnly--underlined elementFont__details--bold')
        # описание рецепта
        description_section = soup.select('.margin-0-auto')
        # инградиенты
        ingredients_section = soup.select('.ringredients-item-name elementFont__body')
        # каллории
        calories_section = soup.select('.semi-bold')
        if calories_section:
            calories = calories_section[0].text.replace('cals', '').strip()

        if ingredients_section:
            for ingredient in ingredients_section:
                ingredient_text = ingredient.text.strip()
                if 'Add all ingredients to list' not in ingredient_text and ingredient_text != '':
                    ingredients.append({'step': ingredient.text.strip()})

        if description_section:
            description = description_section[0].text.strip().replace('"', '')

        if submitter_section:
            submit_by = submitter_section[0].text.strip()

        if title_section:
            title = title_section[0].text

        rec = {'title': title, 'submitter': submit_by, 'description': description, 'calories': calories,
               'ingredients': ingredients}

    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)

if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'

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
        html = msg.value
        result = parse(html)
        parsed_records.append(result)

    consumer.close()
    time.sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_msg(producer, parsed_topic_name, 'parsed', rec)