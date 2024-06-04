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