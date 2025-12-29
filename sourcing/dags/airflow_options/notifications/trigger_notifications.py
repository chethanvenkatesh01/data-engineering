from dataflow_options.utils import Logger

log = Logger(__name__)


def send_trigger_success_notification(context):
    from airflow.models import Variable
    import traceback
    from airflow_options import slack_integration as sl
    notification_types = eval(Variable.get('trigger_success_notification_types', default_var='[]'))
    try:
        for notification_type in notification_types:
            if notification_type.lower() == 'mail':
                send_trigger_success_mail(context)
            elif notification_type.lower() == 'slack':
                sl.slack__on_task_success(context)
    except Exception as e:
        Variable.set("send_trigger_success_notification_error", traceback.format_exc())


def send_trigger_failure_notification(context):
    from airflow.models import Variable
    import traceback
    from airflow_options import slack_integration as sl
    notification_types = eval(Variable.get('trigger_failure_notification_types', default_var='[]'))
    try:
        for notification_type in notification_types:
            if notification_type.lower() == 'mail':
                send_trigger_failed_mail(context)
            elif notification_type.lower() == 'slack':
                sl.slack__on_task_failure(**context)
    except Exception as e:
        Variable.set("send_trigger_failure_notification_error", traceback.format_exc())

def send_trigger_success_mail(context):
    from airflow_options.notifications.mail import send_mail
    from airflow.models import Variable
    import pytz
    import traceback
    from datetime import datetime
    try:
        task_id = context['task'].task_id
        client_name = f"{Variable.get('tenant')} {Variable.get('pipeline')}"
        product_name = Variable.get('product', default_var='ImpactSmartSuite')
        dag_start_dt = context['dag_run'].start_date.astimezone(pytz.timezone(Variable.get('dag_timezone', default_var='UTC')))
        trigger_success_time = datetime.now().astimezone(pytz.timezone(Variable.get('dag_timezone', default_var='UTC'))).strftime('%H:%M')
        mailing_list = eval(Variable.get('notification_recipient'))
        email_body = {
        "subject":"",
        "mail_body":"",
        "client_name":client_name,
        "product_name":product_name,
        "time":trigger_success_time,
        "time_zone": dag_start_dt.tzname() if dag_start_dt.tzname() is not None else 'UTC',
        "validation_table": Variable.get(f"{task_id}_trigger_validation_table"),
        "mailing_list": mailing_list
        }
        email_body['subject'] = f"Ingestion Start Update - {product_name} - {client_name}"
        #email_body['mail_body'] = Variable.get(f"{task_id}_success_mail_body", default_var=f"{task_id} trigger success")
        res = send_mail(email_body, template_name="generic_trigger_success_mail_template.html")
        Variable.set(f"send_trigger_success_mail_response", res)
    except Exception as e:
        Variable.set('send_trigger_success_mail_error', traceback.format_exc())
        log.error(e)

def send_trigger_failed_mail(context):
    from airflow_options.notifications.mail import send_mail
    from airflow.models import Variable
    import pytz
    import traceback
    try:
        task_id = context['task'].task_id
        client_name = f"{Variable.get('tenant')} {Variable.get('pipeline')}"
        product_name = Variable.get('product', default_var='ImpactSmartSuite')
        dag_start_dt = context['dag_run'].start_date.astimezone(pytz.timezone(Variable.get('dag_timezone', default_var='UTC')))
        mailing_list = eval(Variable.get('failure_mailing_list')) if len(eval(Variable.get('failure_mailing_list')))>0 \
            else eval(Variable.get('notification_recipient'))
        email_body = {
        "subject":"",
        "client_name":client_name,
        "product_name":product_name,
        "validation_table": Variable.get(f"{task_id}_failure_mail_validation_table"),
        "received_all_mandatory_triggers": eval(Variable.get("received_all_mandatory_triggers", default_var="False")),
        "mailing_list": mailing_list
        }
        email_body['subject'] = f"Data Ingestion Failure - {product_name} - {client_name}"
        #Variable.set(f"{task_id}_failure_email_body", email_body)
        res = send_mail(email_body, template_name="generic_trigger_failure_mail_template.html")
        Variable.set(f"send_trigger_failed_mail_response", res)
    except Exception as e:
        Variable.set('send_trigger_failed_mail_error', traceback.format_exc())
        log.error(e)

def send_trigger_warning_mail(context):
    from airflow_options.notifications.mail import send_mail
    from airflow.models import Variable
    import pytz
    import traceback
    try:
        task_id = context['task'].task_id
        client_name = f"{Variable.get('tenant')} {Variable.get('pipeline')}"
        product_name = Variable.get('product', default_var='ImpactSmartSuite')
        dag_start_dt = context['dag_run'].start_date.astimezone(pytz.timezone(Variable.get('dag_timezone', default_var='UTC')))
        #trigger_cutoff_time = dag_start_dt + timedelta(seconds=int(Variable.get('trigger_timeout_seconds', default_var=5*60*60)))
        trigger_cutoff_time = Variable.get("trigger_cutoff_time", default_var="00:00")
        mailing_list = eval(Variable.get('failure_mailing_list')) if len(eval(Variable.get('failure_mailing_list')))>0 \
            else eval(Variable.get('notification_recipient'))
        email_body = {
        "subject":"",
        "client_name":client_name,
        "product_name":product_name,
        "validation_table": Variable.get(f"{task_id}_warning_mail_validation_table"),
        "cutoff_time": trigger_cutoff_time,#.strftime('%H:%M'),
        "time_zone": dag_start_dt.tzname() if dag_start_dt.tzname() is not None else 'UTC',
        "mailing_list": mailing_list
        }
        email_body['subject'] = f"Data Ingestion Failure Warning - {product_name} - {client_name}"
        #Variable.set(f"{task_id}_warning_email_body", email_body)
        res = send_mail(email_body, template_name="generic_trigger_warning_mail_template.html")
        Variable.set(f"send_trigger_warning_mail_response", res)
    except Exception as e:
        Variable.set('send_trigger_warning_mail_error', traceback.format_exc())
        log.error(e)

