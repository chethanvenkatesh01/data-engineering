from dataflow_options.utils import Logger

log = Logger(__name__)

def send_sourcing_notification_mail():
    from airflow.models import Variable
    from datetime import datetime
    import pytz
    import traceback
    import pandas as pd
    from airflow_options.utils import get_row_count_for_views
    from airflow_options.notifications.mail import send_mail
    from pretty_html_table import build_table
    try:
        if eval(Variable.get('send_sourcing_notification_mail', default_var='False')):
            entities= eval(Variable.get('unique_views', default_var = "[]"))
            tables=[]
            progress=[]
            status=[]
            row_count_after_load_job = get_row_count_for_views()
            log.info(row_count_after_load_job)
            num_rows_loaded = []
            for view in entities:
                tables.append(view)
                num_rows_before_load_job = int(Variable.get(f"{view}_param_num_rows_before_load_job", default_var="0"))
                num_rows_after_load_job = int(row_count_after_load_job.get(f"{view}", 0))
                if eval(Variable.get(f"{view}_param_replace", default_var='False')):
                    num_rows_loaded.append(num_rows_after_load_job)
                else:
                    num_rows_loaded.append(num_rows_after_load_job - num_rows_before_load_job)
                progress.append(str(Variable.get(f"{view}_load_task_status",default_var='0'))+ '%')
                status.append("Success" if Variable.get(f"{view}_load_task_status", default_var='0')=='100' else "Failed")
            
            log.info(f"Tables {tables}")
            log.info(f"Num of rows loaded {num_rows_loaded}")
            extraction_summary = pd.DataFrame({'Views': tables, 'Progress': progress, 'Status':status, 'Number of rows':num_rows_loaded})

            # add row count validation if initialized
            if eval(Variable.get("row_count_validation", default_var = "False")):
                row_count_validation_info = eval(Variable.get("row_count_validation_info"))
                views, manifestfile_row_count, rowcount_match_flag = [], [], []
                for view in row_count_validation_info:
                    views.append(view)
                    manifestfile_row_count.append(row_count_validation_info[view]["client_row_count"])
                    rowcount_match_flag.append(
                        row_count_validation_info[view]["client_row_count"] == row_count_validation_info[view]["sourced_row_count"]
                    )
                row_count_validation_summary = pd.DataFrame(
                    {
                        'Views': views, 
                        'manifestfile_row_count': manifestfile_row_count, 
                        'rowcount_match_flag':rowcount_match_flag 
                    }
                )
                extraction_summary = pd.merge(
                    left = extraction_summary, 
                    right = row_count_validation_summary, 
                    on = ['Views'], 
                    how = 'left'
                )
                extraction_summary.loc[:, 'manifestfile_row_count'] = pd.to_numeric(extraction_summary['manifestfile_row_count'],  downcast='integer')
                extraction_summary.loc[extraction_summary['manifestfile_row_count'].isnull(),('manifestfile_row_count','rowcount_match_flag')] = "NA"

            extraction_summary_html = build_table(
                extraction_summary, "blue_light", font_size="10px", text_align="center"
            ).replace('"', "'")
            
            summary_condition=len(extraction_summary[extraction_summary['Progress']=='0%'].Views.to_list())
            summary_status= "completed successfully" if summary_condition<1  else "failed"
            if summary_condition<1:
                mailing_list = eval(Variable.get('notification_recipient'))
            else:
                mailing_list = eval(Variable.get('failure_mailing_list'))
            email_body = {
                "subject":f"{Variable.get('tenant')} {Variable.get('pipeline')} sourcing pipeline notification - {str(datetime.today())}",
                "header": "Data sourcing pipeline status",
                "completion_time_header":"Sourcing completion time: ",
                "summary_table_header":"Extraction summary:",
                "summary": f"Sourcing extraction {summary_status}. Below is the detailed summary for the tables extracted: ",
                "end_time":str(datetime.now(pytz.timezone("UTC"))),
                "validation": {
                    "summary_table": extraction_summary_html
                }, 
                "footer": " This is auto generated mail, please do not reply! ",
                "mailing_list":mailing_list
            }
            Variable.set("sourcing_email_body",email_body)
            res = send_mail(email_body, template_name='sourcing_mail_template.html')
            for view in entities:
                Variable.set(f"{view}_load_task_status",0)
            Variable.set(f"send_sourcing_notification_mail_response", res)
    except Exception as e:
        Variable.set('send_sourcing_notification_mail_error', traceback.format_exc())
        log.error(e)


def send_success_notification_mail_and_slack(context):
    from airflow.models import Variable
    from datetime import datetime
    import pytz
    import traceback
    from airflow_options import slack_integration as sl
    from airflow_options.notifications.mail import send_mail
    sl.slack__on_task_success(context)
    try:
        task_id = context['task'].task_id
        mailing_list = eval(Variable.get('notification_recipient'))
        email_body = {
        "subject":"",
        "header": "",
        "completion_time_header":"",
        "summary_table_header":"",
        "summary": "",
        "end_time":str(datetime.now(pytz.timezone("UTC"))),
        "validation": {
            "summary_table": ""
        }, 
        "footer": " This is auto generated mail, please do not reply! ",
        "mailing_list":mailing_list
        }
        email_body['subject'] = f"{Variable.get('tenant')} {Variable.get('pipeline')} trigger success {str(datetime.today())}"
        email_body['validation']['summary_table'] = Variable.get(f"{task_id}_success_mail_body", default_var=f"{task_id} trigger success")
        email_body['header'] = f"{Variable.get('tenant')}  {Variable.get('pipeline')} - trigger status"
        email_body['completion_time_header'] = f"Trigger completion time: "
        email_body['summary_table_header'] = f"Triggers status: "
        email_body['summary'] = f"Trigger status for {Variable.get('tenant')} {Variable.get('pipeline')}"
        res = send_mail(email_body, template_name='sourcing_mail_template.html')
        Variable.set(f"send_success_notification_mail_and_slack_response", res)
    except Exception as e:
        Variable.set('send_success_notification_mail_and_slack_error', traceback.format_exc())
        log.error(e)
