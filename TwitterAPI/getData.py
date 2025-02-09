import pandas as pd # type: ignore
import os
import sys
sys.path.append('dags/TwitterAPI')
sys.path.append("home/airflow/.local/lib/python3.8/site-packages") 

import time
import tweepy # type: ignore

from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.exceptions import AirflowSkipException # type: ignore

from Authentication import connection


# getting data from the API
def getData(ti):
    api = connection()

    if api is None:
        raise ValueError("Failed to authenticate. API object is None.")

    try:
        column_names = ["User_id", "Username", "Bio", "Followers", "Following", "Total_Tweets"]
        Twitter_df = pd.DataFrame(columns=column_names)

        usernames = ['realDonaldTrump','elonmusk','narendramodi','WarrenBuffett','gautam_adani']

        users_data = api.get_users(usernames=usernames, user_fields=["description", "public_metrics"])  # Use v2 users lookup
        user_list = users_data.data

        print(user_list)

        user_data_list = []
        for data in user_list:
            try:
                print(data)
                user_info = {
                    "User_id": data.id,
                    "Username": data.username,
                    "Name": data.name,
                    "Bio": data.description,
                    "Followers": data.public_metrics['followers_count'],
                    "Following": data.public_metrics['following_count'],
                    "Total_Tweets": data.public_metrics['tweet_count']
                }

                user_data_list.append(user_info)

                time.sleep(5)

            except tweepy.TooManyRequests:
                print("Rate limit reached. Waiting before retrying...")
                time.sleep(900) 

        print(user_info)
        Twitter_df = pd.concat(user_data_list)

        ti.xcom_push(key='Twitter_user_info',value = Twitter_df.to_dict('records')) 

    except Exception as e:
        raise AirflowSkipException(f"Skipping task: could not get the user required. Error: {e}") 

# inserting data into the table
def insertData(ti):
    user_data = ti.xcom_pull(key='Twitter_user_info', task_ids='fetch_User_Info')

    postgres_hook = PostgresHook(postgres_conn_id = 'Twitter_API')

    insert_query = """
        INSERT INTO twitter (User_id,Username,Name,Bio,Followers,Following,Total_Tweets)
        VALUES(%s, %s, %s, %s, %s, %s, %s)
    """

    if user_data is not None:
        for u in user_data:
            postgres_hook.run(insert_query, parameters=(u['User_id'], u['Username'], u['Name'], u['Bio'], u['Followers'], u['Following'], u['Total_Tweets']))
    else:
        print("Error: user_data is None")


