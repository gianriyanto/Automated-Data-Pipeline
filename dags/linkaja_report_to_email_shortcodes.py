from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_acl_operator import GoogleCloudStorageObjectCreateAclEntryOperator
from airflow.operators.email_operator import EmailOperator

from customize.oracle_to_gcs_base import OracleToGoogleCloudStorageOperator
from cfgdags.linkaja_report_to_email_shortcodes_rpt import REPORT_CONFIG
from cfgdags.linkaja_report_to_email_shortcodes_config import lastmonth, EmailTemplate

from datetime import timedelta
import datetime
import json

DEFAULT_DAG_ARGS = {
    'owner': 'linkaja_report_BI_Analyst',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 2, 10),  # TODO: last month
    'email': ['rio_h_pangihutan@linkaja.id'],
    'email_on_failure': False,  # True
    'email_on_retry': False,  # True
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'linkaja_report_to_email_shortcodes',
    schedule_interval='@daily',  # "0 18 * * *"
    default_args=DEFAULT_DAG_ARGS)

task_start = BashOperator(
    task_id='start_task',
    bash_command='echo start',
    dag=dag
)

task_finish = BashOperator(
    task_id='finish_task',
    bash_command='echo finish',
    dag=dag
)

rpt_cnt = 0
report_config = json.loads(REPORT_CONFIG)

rpt_name = report_config[rpt_cnt]['report_name']
rpt_pic = report_config[rpt_cnt]['pic']
rpt_rcpnt = report_config[rpt_cnt]['recipient']
rpt_all_rcpnt = rpt_pic + rpt_rcpnt

task_start_report = BashOperator(
    task_id='start_' + rpt_name,
    bash_command='echo start this report',
    dag=dag
)

task_send_report_email = EmailOperator(
    task_id='send_report_email_' + rpt_name,
    to=rpt_rcpnt,
    cc=rpt_pic,
    subject='Report ' + rpt_name + "_" + "{{ next_ds_nodash }}",
    params={
        'report_name': rpt_name,
        'report_period': '{{ next_ds_nodash }}',
    },
    html_content=EmailTemplate,
    dag=dag
)

rpt_qry_cnt = 0
for itr_rpt_qry in report_config[rpt_cnt]['queries']:
    rpt_query_name = report_config[rpt_cnt]['queries'][rpt_qry_cnt]['query_name']
    rpt_query = report_config[rpt_cnt]['queries'][rpt_qry_cnt]['query']

    task_queryoracle_to_gcs = OracleToGoogleCloudStorageOperator(
        task_id='oraclespoolreport_to_gcs_' + rpt_query_name,
        oracle_conn_id='oracle_oprtpdb',
        google_cloud_storage_conn_id='gcp_project_deng',
        provide_context=True,
        sql=rpt_query,
        # params={
        #     'table_name': tbl,
        #     'control': ctrl_tbl
        # },
        bucket='la_reporting_external',
        filename='%s/%s_%s.csv' % (rpt_name, rpt_query_name, "{{ next_ds_nodash }}"),
        export_format='csv',
        field_delimiter=',',
        dag=dag
    )

    task_gcs_give_acl = GoogleCloudStorageObjectCreateAclEntryOperator(
        task_id='giveaclaccess_gcs_' + rpt_query_name,
        object_name='%s/%s_%s.csv' % (rpt_name, rpt_query_name, "{{ next_ds_nodash }}"),
        entity='allUsers',
        role='READER',
        bucket='la_reporting_external',
        google_cloud_storage_conn_id='gcp_project_deng',
        dag=dag
    )

    task_start >> task_start_report >> task_queryoracle_to_gcs >> task_gcs_give_acl
    rpt_qry_cnt += 1
    task_gcs_give_acl >> task_send_report_email >> task_finish
