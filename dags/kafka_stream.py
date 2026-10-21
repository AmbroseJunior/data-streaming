from datetime import datetime
import json
import requests
from kafka import KafkaProducer
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

default_args = {
    'owner': 'nnamdi',
    'start_date': datetime(2026, 1, 1)
}


def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res
    
    # print(json.dumps(res, indent=3))
    
    
def format_data(res):
    data = {}
    

    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    
    data['address'] = str(res['location']['street']['number']) + ' ' + str(res['location']['street']['name'])  + ' ' + str(res['location']['city']) + ' ' + str(res['location']['state']) + str(res['location']['postcode']) + ' ' + str(res['location']['country'])
    data['email'] = res['email']
    data['phone'] = res['phone']
    
    data['username'] = res['login']['username']
    data['password'] = res['login']['password']
    
    data['date_of_birth'] = str(res['dob']['date']) + ' ' + str(res['dob']['age'])
    
    data['picture'] = res['picture']['large']
    
    data['date_registered'] = str(res['registered']['date']) + ' ' + str(res['registered']['age'])
    
    return data



def stream_data():
    try:
        
        
        
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        curr_time = time.time()
        while True:
            if time.time() > curr_time + 60:
                break
            try:
                res = get_data()
                res = format_data(res)
                metadata = producer.send('users_created', json.dumps(res).encode('utf-8')).get(timeout=5)
                
            except Exception as e:
                logging.error(f'Error pushing message: {e}')
                time.sleep(5)
                continue
            
                
        
        print(f'Data sent successfully with metadata: {metadata.topic}, {metadata.partition}')
    except Exception as e:
        print(f'Error pushing message to kafka: {e}')
    

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )



# format_data()
# get_data()
