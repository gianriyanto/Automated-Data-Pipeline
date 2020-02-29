from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_acl_operator import GoogleCloudStorageObjectCreateAclEntryOperator
from airflow.operators.email_operator import EmailOperator

from customize.oracle_to_gcs_base import OracleToGoogleCloudStorageOperator
from cfgdags.linkaja_report_to_email_external_dags_config import lastmonth, EmailTemplate
from cfgdags.linkaja_report_to_email_external_dags_rpt import REPORT_CONFIG

from datetime import timedelta
import json
import logging

DEFAULT_DAG_ARGS = {
    'owner': 'linkaja_report_BI_Analyst',
    'depends_on_past': False,
    'start_date': lastmonth,
    'email': ['rio_h_pangihutan@linkaja.id'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'linkaja_report_to_email_external_dags_v0.17', 
    schedule_interval="0 18 * * *", 
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

for itr_rpt in report_config:
    rpt_name = report_config[rpt_cnt]['report_name']
    rpt_pic = report_config[rpt_cnt]['pic']
    rpt_rcpnt = report_config[rpt_cnt]['recipient']
    rpt_all_rcpnt = rpt_pic + rpt_rcpnt
    rpt_query = report_config[rpt_cnt]['query']

    task_queryoracle_to_gcs = OracleToGoogleCloudStorageOperator(
        task_id='oraclespoolreport_to_gcs_' + rpt_name,
        oracle_conn_id='oracle_oprtpdb',
        google_cloud_storage_conn_id='gcp_project_deng',
        provide_context=True,
        sql=rpt_query,
        # params={
        #     'table_name': tbl,
        #     'control': ctrl_tbl
        # },
        bucket='la_reporting_external',
        filename='%s/%s_%s.csv' % (rpt_name, rpt_name, "{{ next_ds_nodash }}"),
        export_format='csv',
        field_delimiter=',',
        dag=dag
    )

    task_gcs_give_acl = GoogleCloudStorageObjectCreateAclEntryOperator(
        task_id='giveaclaccess_gcs_' + rpt_name,
        object_name='%s/%s_%s.csv' % (rpt_name, rpt_name, "{{ next_ds_nodash }}"),
        entity='allUsers',
        role='READER',
        bucket='la_reporting_external',
        google_cloud_storage_conn_id='gcp_project_deng',
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
            'report_link': "http://la_reporting_external.storage.googleapis.com/%s/%s" % (rpt_name, rpt_name)
        },
        html_content=EmailTemplate,
        dag=dag
    )

    task_start.set_downstream(task_queryoracle_to_gcs)
    task_queryoracle_to_gcs.set_downstream(task_gcs_give_acl)
    task_gcs_give_acl.set_downstream(task_send_report_email)
    task_send_report_email.set_downstream(task_finish)
    rpt_cnt = rpt_cnt + 1

