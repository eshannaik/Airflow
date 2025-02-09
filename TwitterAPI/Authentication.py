import sys
import os
sys.path.append("home/airflow/.local/lib/python3.8/site-packages") 

import tweepy # type: ignore
from dotenv import load_dotenv # type: ignore

sys.path.append('/dags/TwitterAPI')

from airflow.exceptions import AirflowSkipException # type: ignore


# connecting to the twitter API to extract data
def connection():
    
    #load credentials, stored in env file
    load_dotenv(dotenv_path='/config/.env')

    access_key = os.getenv("ACCESS_KEY")
    access_secret = os.getenv("ACCESS_SECRET")
    consumer_key = os.getenv("CONSUMER_KEY")
    consumer_secret = os.getenv("CONSUMER_SECRET")
    bearer_token = os.getenv("BEARER_TOKEN")
    
    if not all([access_key, access_secret, consumer_key, consumer_secret, bearer_token]):
        raise ValueError("Missing Twitter API credentials in the .env file")

    try:
        auth = tweepy.OAuthHandler(access_key,access_secret)
        auth.set_access_token(consumer_key,consumer_secret)

        #v1.1 commands
        #api = tweepy.API(auth, wait_on_rate_limit=True) 
        #user = api.verify_credentials()

        #if(user):
            #print(f"Authenticated as: {user.screen_name}")
    

        client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

        if(client):
            print(f"Authenticated successful")
            return client
        else:
            print("Failed to authenticate")
            return None
        
    except tweepy.TweepyException as e:
        raise AirflowSkipException(f"Skipping task: Authentication failed.")