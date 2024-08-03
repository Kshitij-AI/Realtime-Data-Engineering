import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'kshitij',
    'start_date': datetime(2024, 7, 16, 10, 00)
}

# Get data from Random Users Generator API - https://randomuser.me/
def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    # Get JSON data in the Python dictionary format
    res = res.json()
    res = res['results'][0]
    return res

# Formatting and extracting the required information
def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    location = res['location']
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

def stream_data():
    import json
    import time
    import logging
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        # Streaming data for 1 min
        if time.time() > curr_time+60:
            break
        try:
            # Prepare data
            res = get_data()
            res = format_data(res)

            # Stream data to Kafka
            producer.send(topic='user_info', value=json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
    return

with DAG(dag_id = 'stream_user_info',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False):
    streaming_data = PythonOperator(
        task_id = 'Stream_Data_From_API',
        python_callable=stream_data
    )