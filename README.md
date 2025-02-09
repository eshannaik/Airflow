Use Cases with the following Stack and flow - 

Tech Stack being used -
- Python -> For extraction of data from the various sources
- SQL -> For the transformations on the data
- PostgreSQL -> Store the processed data
- Airflow -> Orchestrate the ETL pipeline
- Docker -> Containerize and deploy the application
- ELK -> monitoring logs and system performance
  
ETL Flow -
- Extract - Extract data from a source
- Transform - do transformations on the table
- Load - Load the data into a database
- Monitor → Use ELK to track logs and system performance.

Setup -
- Download the following packages
  - Tweepy -> access data from the Twitter API
  - dotenv -> load config file ( stores your API keys )
- For the config file
  - Store it in a folder (Example - dags/config/.env)
  - Store all the API keys
  - API Keys can be got from the TwitterAPI website ( https://developer.x.com/en/portal/dashboard )

This project is licensed under the MIT License



