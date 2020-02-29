from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_acl_operator import GoogleCloudStorageObjectCreateAclEntryOperator
from airflow.operators.email_operator import EmailOperator

from customize.oracle_to_gcs_base import OracleToGoogleCloudStorageOperator
from cfgdags.linkaja_report_to_email_multiplequeries_rpt import REPORT_CONFIG

from datetime import timedelta
import datetime
import json


DEFAULT_DAG_ARGS = {
    'owner': 'linkaja_report_BI_Analyst',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 2, 3),  # TODO: last month
    'email': ['rio_h_pangihutan@linkaja.id'],
    'email_on_failure': False,  # True
    'email_on_retry': False,  # True
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'linkaja_report_to_email_multiple_queries',
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
for itr_rpt in report_config:
    rpt_name = report_config[rpt_cnt]['report_name']
    rpt_pic = report_config[rpt_cnt]['pic']
    rpt_rcpnt = report_config[rpt_cnt]['recipient']

    additions_to_template = []

    this_qry_cnt = 0
    for itr_rpt_qry in report_config[rpt_cnt]['queries']:
        rpt_query_name = report_config[rpt_cnt]['queries'][this_qry_cnt]['query_name']
        rpt_query_link = "http://la_reporting_external.storage.googleapis.com/%s/%s" % (rpt_name, rpt_query_name)
        add_to_template = """
                              <table border="0" cellpadding="0" cellspacing="0" class="btn btn-primary" style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; width: 100%; box-sizing: border-box;">
                              <p style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px;">This is Report <b>{}</b> for period <b>{{{{ next_ds_nodash }}}}</b></p>
                              <tbody>
                                <tr>
                                  <td align="left" style="font-family: sans-serif; font-size: 14px; vertical-align: top; padding-bottom: 15px;">
                                    <table border="0" cellpadding="0" cellspacing="0" style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; width: auto;">
                                      <tbody>
                                        <tr>
                                            <td style="font-family: sans-serif; font-size: 14px; vertical-align: top; background-color: #3498db; border-radius: 5px; text-align: center;"> <a href=" {}_{{{{ ds_nodash }}}}.csv" target="_blank" style="display: inline-block; color: #ffffff; background-color: #3498db; border: solid 1px #3498db; border-radius: 5px; box-sizing: border-box; cursor: pointer; text-decoration: none; font-size: 14px; font-weight: bold; margin: 0; padding: 12px 25px; text-transform: capitalize; border-color: #3498db;">{}</a> </td>
                                        </tr>
                                      </tbody>
                                    </table>
                                  </td>
                                </tr>
                              </tbody>
                              """.format(rpt_query_name, rpt_query_link, rpt_query_name)
        additions_to_template.append(add_to_template)
        this_qry_cnt += 1

    EmailTemplate = """
    <!doctype html>
    <html>
      <head>
        <meta name="viewport" content="width=device-width">
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Simple Transactional Email</title>
        <style>
        /* -------------------------------------
            INLINED WITH htmlemail.io/inline
        ------------------------------------- */
        /* -------------------------------------
            RESPONSIVE AND MOBILE FRIENDLY STYLES
        ------------------------------------- */
        @media only screen and (max-width: 620px) {
          table[class=body] h1 {
            font-size: 28px !important;
            margin-bottom: 10px !important;
          }
          table[class=body] p,
                table[class=body] ul,
                table[class=body] ol,
                table[class=body] td,
                table[class=body] span,
                table[class=body] a {
            font-size: 16px !important;
          }
          table[class=body] .wrapper,
                table[class=body] .article {
            padding: 10px !important;
          }
          table[class=body] .content {
            padding: 0 !important;
          }
          table[class=body] .container {
            padding: 0 !important;
            width: 100% !important;
          }
          table[class=body] .main {
            border-left-width: 0 !important;
            border-radius: 0 !important;
            border-right-width: 0 !important;
          }
          table[class=body] .btn table {
            width: 100% !important;
          }
          table[class=body] .btn a {
            width: 100% !important;
          }
          table[class=body] .img-responsive {
            height: auto !important;
            max-width: 100% !important;
            width: auto !important;
          }
        }

        /* -------------------------------------
            PRESERVE THESE STYLES IN THE HEAD
        ------------------------------------- */
        @media all {
          .ExternalClass {
            width: 100%;
          }
          .ExternalClass,
                .ExternalClass p,
                .ExternalClass span,
                .ExternalClass font,
                .ExternalClass td,
                .ExternalClass div {
            line-height: 100%;
          }
          .apple-link a {
            color: inherit !important;
            font-family: inherit !important;
            font-size: inherit !important;
            font-weight: inherit !important;
            line-height: inherit !important;
            text-decoration: none !important;
          }
          #MessageViewBody a {
            color: inherit;
            text-decoration: none;
            font-size: inherit;
            font-family: inherit;
            font-weight: inherit;
            line-height: inherit;
          }
          .btn-primary table td:hover {
            background-color: #34495e !important;
          }
          .btn-primary a:hover {
            background-color: #34495e !important;
            border-color: #34495e !important;
          }
        }
        </style>
      </head>
      <body class="" style="background-color: #f6f6f6; font-family: sans-serif; -webkit-font-smoothing: antialiased; font-size: 14px; line-height: 1.4; margin: 0; padding: 0; -ms-text-size-adjust: 100%; -webkit-text-size-adjust: 100%;">
        <table border="0" cellpadding="0" cellspacing="0" class="body" style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; width: 100%; background-color: #f6f6f6;">
          <tr>
            <td style="font-family: sans-serif; font-size: 14px; vertical-align: top;">&nbsp;</td>
            <td class="container" style="font-family: sans-serif; font-size: 14px; vertical-align: top; display: block; Margin: 0 auto; max-width: 580px; padding: 10px; width: 580px;">
              <div class="content" style="box-sizing: border-box; display: block; Margin: 0 auto; max-width: 580px; padding: 10px;">

                <!-- START CENTERED WHITE CONTAINER -->
                <span class="preheader" style="color: transparent; display: none; height: 0; max-height: 0; max-width: 0; opacity: 0; overflow: hidden; mso-hide: all; visibility: hidden; width: 0;">This is preheader text. Some clients will show this text as a preview.</span>
                <table class="main" style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; width: 100%; background: #ffffff; border-radius: 3px;">

                  <!-- START MAIN CONTENT AREA -->
                  <tr>
                    <td class="wrapper" style="font-family: sans-serif; font-size: 14px; vertical-align: top; box-sizing: border-box; padding: 20px;">
                      <table border="0" cellpadding="0" cellspacing="0" style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; width: 100%;">
                        <tr>
                          <td style="font-family: sans-serif; font-size: 14px; vertical-align: top;">
                            <p style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px;">Dear all</p>

                            """ + ('\n'.join(map(str, additions_to_template))) + '\n' + """

                            </table>
                            <br>
                            <br>
                            <hr>
                            <br>
                            <span style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px; color: #999999;">
                            <p style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px;">"This electronic mail and its attachment(s) is intended only for the recipient(s) to whom it is addressed. It may contain information which may be confidential and/or protected by legal privilege. If you are not the intended recipient(s), reading, disclosing, printing, copying, forwarding this electronic mail and its attachment(s) and/or taking any action in reliance on the information in this electronic mail and its attachment(s) are prohibited. Telkomsel shall not be liable in respect of communication made by its employee which is contrary to the company policy and/or outside the scope of the employment of the individual concerned. The employee will be personally liable for any damages or other liability arising."</p>
                            <p style="font-family: sans-serif; font-size: 14px; font-weight: normal; margin: 0; Margin-bottom: 15px;">"Surat elektronik ini beserta lampirannya dimaksudkan hanya untuk penerima kepada siapa surat tersebut ditujukan. Informasi yang terdapat di dalamnya dapat bersifat rahasia dan/atau dilindungi oleh hukum. Jika Anda bukan penerima yang dituju, Anda dilarang untuk membaca, mengungkapkan, mencetak, menyalin, meneruskan surat elektronik ini beserta lampirannya dan/atau mengambil tindakan apapun berdasarkan informasi yang terdapat dalam surat elektronik ini beserta lampirannya. Telkomsel tidak bertanggung jawab atas setiap komunikasi karyawan yang bertentangan dengan kebijakan perusahaan dan/atau berada di luar lingkup pekerjaannya. Segala resiko dan akibat yang ditimbulkan merupakan tanggung jawab personal. "</p>
                            </span>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>

                <!-- END MAIN CONTENT AREA -->
                </table>

                <!-- START FOOTER -->
                <div class="footer" style="clear: both; Margin-top: 10px; text-align: center; width: 100%;">
                  <table border="0" cellpadding="0" cellspacing="0" style="border-collapse: separate; mso-table-lspace: 0pt; mso-table-rspace: 0pt; width: 100%;">
                    <tr>
                      <td class="content-block" style="font-family: sans-serif; vertical-align: top; padding-bottom: 10px; padding-top: 10px; font-size: 12px; color: #999999; text-align: center;">
                        <span class="apple-link" style="color: #999999; font-size: 12px; text-align: center;">This Report created by</span>
                        <br><a href="https://www.linkaja.id/" style="text-decoration: underline; color: #999999; font-size: 12px; text-align: center;">LinkAja Big Data and Analytics</a>.
                        <br>#BereshTanpaCash
                      </td>
                    </tr>
                  </table>
                </div>
                <!-- END FOOTER -->

              <!-- END CENTERED WHITE CONTAINER -->
              </div>
            </td>
            <td style="font-family: sans-serif; font-size: 14px; vertical-align: top;">&nbsp;</td>
          </tr>
        </table>
      </body>
    </html>
    """

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
            bucket='la_reporting_external',
            filename='%s/%s_%s.csv' % (rpt_name, rpt_query_name, "{{ ds_nodash }}"),
            export_format='csv',
            field_delimiter=',',
            dag=dag
        )

        task_gcs_give_acl = GoogleCloudStorageObjectCreateAclEntryOperator(
            task_id='giveaclaccess_gcs_' + rpt_query_name,
            object_name='%s/%s_%s.csv' % (rpt_name, rpt_query_name, "{{ ds_nodash }}"),
            entity='allUsers',
            role='READER',
            bucket='la_reporting_external',
            google_cloud_storage_conn_id='gcp_project_deng',
            dag=dag
        )

        task_start >> task_start_report >> task_queryoracle_to_gcs >> task_gcs_give_acl
        rpt_qry_cnt += 1
        task_gcs_give_acl >> task_send_report_email >> task_finish

    rpt_cnt += 1





