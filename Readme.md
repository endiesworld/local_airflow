## Configuring airflow
>> Setup and activate a virtual environment
>> pip install "apache-airflow[celery]==<version>" --constraint "<constraintfile>"
>> airflow version
>> airflow -h
>> airflow cheat-
>> airflow info <!-- To get info about your current airflow installation -->
>> airflow db init
>> airflow users -h <!-- to show what command can be used to create users -->
>> airflow users list 
>> airflow users create \
    -e endiesworld@gmail.com \
    -f emmanuel \
    -l okoro \
    -p my_password \
    -r Admin \
    -u emmanuel.user
>> airflow scheduler
>> airflow 
>> cd /home/endie/airflow
>> code .

## Conecting to a postgres database
>> pip install psycopg2-binary
>> pip install apache-airflow-providers-postgres

## Running postgres docker container
>> docker pull postgres
>> docker run --name my_postgres -e POSTGRES_PASSWORD=mysecretpassword -d -p 5432:5432 postgres
>> docker exec -it my_postgres bash
>> psql -U postgres
>> CREATE DATABASE mydatabase;
>> \c mydatabase

## Using a postgres database as Airflow metadata store
>> connect to your postgres database
>> CREATE DATABASE airflow_db;
>> switch to airflow.cfg and do the followings:
    >> change the 'executor' value from 'SequentialExecutor' to any other Executor that allows parallelism (LocalExecutor, CeleryExecutor, KubernetesExecutor)
    >> change the 'sql_alchemy_conn' to your postgres or mysql connection value i.e
        sql_alchemy_conn = postgressql://postgres@localhost:5432/airflow_db