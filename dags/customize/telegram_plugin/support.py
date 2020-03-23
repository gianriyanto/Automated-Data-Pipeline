from customize.telegram_plugin.operators.telegram_operator import TelegramOperator


def notify_on_failure(context):

    failed_alert = TelegramOperator(
        task_id='notify_on_failure',
        telegram_conn_id='telegram_bot_conn_test',
        text="""
        Tasked Failed: Error in "{dag}"

        Task: {task}  
        Dag: {dag} 
        Execution Time: {exec_date} 
        Log Url: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    )
    return failed_alert.execute(context=context)
