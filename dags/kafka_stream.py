from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'zano',
    'start_date': datetime(2024, 3, 3, 10, 0)
}

def get_data():
    import json
    import  requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
   # print(json.dumps(res, indent=3))
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    location = res['location']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, " \
                      f"{location['city']},  {location['state']}, {location['postcode']}, {location['country']}"
    data['city'] = res['location']['city']
    data['postcode'] = res['location']['postcode']
    data['state'] = res['location']['state']
    data['country'] = res['location']['country']
    coordinates = location['coordinates']  # Extract coordinates data
    data['coordinates'] = (coordinates['latitude'], coordinates['longitude'])
    data['dob'] = res['dob']['date']
    data['age'] = res['dob']['age']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['nat'] = res['nat']
    data['email']

    return  data

def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3))



# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#
#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )

stream_data()