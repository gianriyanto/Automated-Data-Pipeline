"""
DESCRIPTION : 
        Airflow Oracle to GCS Operator Extenstion
        This Library intended to Spool and Oracle Query and Export it to Google Cloud Storage
        Future plan this library wiil be add with rowcount and sizecount
AUTHOR : rio_h_pangihutan@linkaja.id

"""

import datetime
import json
import time
from decimal import Decimal

from airflow.hooks.oracle_hook import OracleHook
# from airflow.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator
from airflow.contrib.operators.sql_to_gcs import BaseSQLToGoogleCloudStorageOperator
from airflow.utils.decorators import apply_defaults


class OracleToGoogleCloudStorageOperator(BaseSQLToGoogleCloudStorageOperator):
    """
    Copy data from Oracle to Google Cloud Storage in JSON or CSV format.
    
    :param oracle_conn_id: Reference to a specifig Oracle hook.
    :type postgres_conn_id: str
    """
    ui_color = '#a0e08c'

    type_map = {
        1114: 'TIMESTAMP',
        1184: 'TIMESTAMP',
        1082: 'TIMESTAMP',
        1083: 'TIMESTAMP',
        1005: 'INTEGER',
        1007: 'INTEGER',
        1016: 'INTEGER',
        20: 'INTEGER',
        21: 'INTEGER',
        23: 'INTEGER',
        16: 'BOOLEAN',
        700: 'FLOAT',
        701: 'FLOAT',
        1700: 'FLOAT',
    }

    @apply_defaults
    def __init__(self,
                 oracle_conn_id='oracle_default',
                #  parameters=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        # self.parameters = parameters

    def query(self):
        """
        Queries Oracle and returns a cursor to the results.
        """
        self.log.info('Executing: %s %s', self.sql, self.parameters)
        hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor

    def field_to_bigquery(self, field):
        return {
            'name': field[0],
            'type': self.type_map.get(field[1], "STRING"),
            'mode': 'REPEATED' if field[1] in (1009, 1005, 1007,
                                               1016) else 'NULLABLE'
        }

    def convert_type(self, value, schema_type):
        """
        Takes a value from Postgres, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Times are converted to seconds.
        """
        if isinstance(value, (datetime.datetime, datetime.date)):
            return time.mktime(value.timetuple())
        if isinstance(value, datetime.time):
            formated_time = time.strptime(str(value), "%H:%M:%S")
            return datetime.timedelta(
                hours=formated_time.tm_hour,
                minutes=formated_time.tm_min,
                seconds=formated_time.tm_sec).seconds
        if isinstance(value, dict):
            return json.dumps(value)
        if isinstance(value, Decimal):
            return float(value)
        return value