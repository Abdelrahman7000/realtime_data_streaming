from airflow.sdk import dag, task
import uuid
from datetime import datetime
import requests
import json
from kafka import KafkaProducer
import time 
import logging

def get_data():
    try:
        res=requests.get('https://randomuser.me/api/')
        res=res.json()
        res=res['results'][0]
        return res 
    except Exception as e:
        logging.error(f'Error while fetching the data {e}')

def transform(res):
    data={}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                    f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data 
@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ETL",'USERS'],
)
def stream_user_data():
    
    @task()
    def push_data():
        
        producer=KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
        current_time=time.time()
        
        while True:
            if time.time()>current_time+60:
                break
            try:
                data=transform(get_data())
                producer.send('user_created',json.dumps(data).encode('utf-8'))
            except Exception as e:
                logging.error(f'Error while pushing the data into Kafka {e}')
                continue
       

    push_data()

stream_user_data()