from email import header
from ensurepip import bootstrap
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

def publish_msg(producer_instance, topic_name, key, value):
    ''' функция для отправки записей брокеру '''
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published succefully.')
    except Exception as ex:
        print('Exeption in publishing message')
        print(str(ex))

def connect_kafka_producer():
    ''' функция для подключения продюсера к локальному хосту'''
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=headers) # создаем GET запрос 
        if r.status_code == 200: # проверяем успешность GET запроса
            html = r.text # помещаем содержание странццы в переменную html
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip() # возвращаем содержание GET запроса без лишних пробелов 

def get_recipes():
    ''' функция парсинга сайта с рецептами '''
    recipies = []
    salad_url = 'https://www.allrecipes.com/recipes/96/salad/'
    url = 'https://www.allrecipes.com/recipes/96/salad/'
    print('Accessing list')

    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            links = soup.select('.fixed-recipe-card__h3 a')
            idx = 0
            for link in links:

                time.sleep(2)
                recipe = fetch_raw(link['href'])
                recipies.append(recipe)
                idx += 1
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return recipies


if __name__ == '__main__':
    # headers см. в брауузере (F12 -> Network -> Headers -> Response Headers "Pragma", Request Headers "User-agent")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
        'Pragma': 'no-cache'
    }

    all_recipes = get_recipes()
    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_recipes:
            publish_msg(kafka_producer, 'raw_recipes', 'raw', recipe.strip())
        if kafka_producer is not None:
            kafka_producer.close()
        
