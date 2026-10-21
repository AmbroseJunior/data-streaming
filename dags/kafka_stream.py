from datetime import datetime
import json
import requests
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 1, 1)
}


def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    res = format_data(res)
    
    print(json.dumps(res, indent=3))
    

    return res
    

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
    
# def stream_data():
    
    
    
# with DAG(
#     'user Automation',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_task,
#         provide_context=True
#     )


# stream_data()
# format_data()
get_data()
