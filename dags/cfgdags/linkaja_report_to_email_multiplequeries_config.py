'''
# pip install apache-airflow[postgres,gcp,mysql]
# apt-get install default-libmysqlclient-dev
'''

from datetime import datetime, timedelta

lastmonth = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time())
