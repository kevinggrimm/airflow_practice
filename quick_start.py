# Airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
export AIRFLOW_HOME=/airflow

# install from pypi
pip3 install apache-airflow

# initialize the databasef
airflow initdb

# start the webserver, default port is 8080
airflow webserver -p 8080

# start the scheduler 
airflow scheduler

# visit localhost:8080 in the browser and enable the 
# example dag in the homepage