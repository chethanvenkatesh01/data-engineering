def create_email_body(notification_type: str):
    
    import pandas as pd
    from airflow.models import Variable
    from constants.constant import NotificationType
    from pretty_html_table import build_table
    from datetime import datetime
    from database_utility.GenericDatabaseConnector import WarehouseConnector
    from database_utility.utils import escape_column_name

    warehouse = Variable.get("warehouse")
    warehouse_kwargs = Variable.get("warehouse_kwargs")
    wc = WarehouseConnector(warehouse, warehouse_kwargs)
    ANOMALY_SUFFIX = '_anomaly_summary'
    summary_tables=wc._get_table_list("%" + ANOMALY_SUFFIX)
    entities=eval(Variable.get("entity_type"))
    sub_query = []
    summary_enitity_tables = [x for x in summary_tables if x.replace(ANOMALY_SUFFIX,'') in entities]
    summary_derived_preprocessing_tables = [x for x in summary_tables if x.replace(ANOMALY_SUFFIX,'') not in entities]

    # if notification_type == NotificationType.SUCCESS.value or notification_type == NotificationType.FAILURE.value:
    #     for i in summary_enitity_tables:
    #         sub_query.append(
    #             f""" (SELECT TABLE,
    #                             CAST(no_original_rows AS int64) as original_rows,
    #                             rule_display_name as qc_name,
    #                             rule_description as qc_description,
    #                             action,
    #                             CAST(affected_rows AS string) as rows_affected
    #                             FROM `{Variable.get('gcp_project')}.{Variable.get('dataset')}.{i}`
    #                 ) """
    #         )
    # else:
    #     for i in summary_derived_preprocessing_tables:
    #         sub_query.append(
    #             f""" (SELECT TABLE,
    #                             CAST(no_original_rows AS int64) as original_rows,
    #                             rule_display_name as qc_name,
    #                             rule_description as qc_description,
    #                             action,
    #                             CAST(affected_rows AS string) as rows_affected
    #                             FROM `{Variable.get('gcp_project')}.{Variable.get('dataset')}.{i}`
    #                 ) """
    #         )


    table_col_str = escape_column_name("table", warehouse)
    common_query = """
                            SELECT {table_col_str},
                            CAST(no_original_rows AS integer) as original_rows,
                            rule_display_name as qc_name,
                            rule_description as qc_description,
                            action,
                            status,
                            CAST(affected_rows AS string) as rows_affected,
                            FROM {table}
                            where concat(rule,insertion_date) in (select concat(rule,insertion_date) from
                            (select rule,max(insertion_date) as insertion_date from {table} group by 1))
                    """
                


    sub_query = []

    if notification_type == NotificationType.SUCCESS.value or notification_type == NotificationType.FAILURE.value:
        tables = summary_enitity_tables
    else:
        tables = summary_derived_preprocessing_tables

    for i in tables:
        complete_table_name = wc._get_complete_table_name(i, False)
        sub_query.append(common_query.format(table_col_str = table_col_str, table=complete_table_name))

    unioned_query = f" union all ".join(sub_query)

    final_query=f"""
            SELECT
                {table_col_str},
                qc_name,
                action,
                max(original_rows) OVER(PARTITION BY {table_col_str} ) AS original_rows,
                rows_affected,
                concat(cast(round(
                (CASE 
                    WHEN MAX(original_rows) OVER(PARTITION BY {table_col_str}) = 0 THEN 0
                    ELSE CAST(rows_affected AS INTEGER) / MAX(original_rows) OVER(PARTITION BY {table_col_str})
                END) *100,2) as string), '%') as per_rows_affected,
                status,
                qc_description
                FROM (
                SELECT
                    {table_col_str},
                    qc_name,
                    qc_description,
                    action,
                    original_rows,
                    CASE
                    WHEN action = 'DELETE' THEN MAX(original_rows) OVER(PARTITION BY {table_col_str}) - SUM(CAST(rows_affected AS integer)) OVER (PARTITION BY {table_col_str} ORDER BY original_rows DESC, rows_affected)
                    ELSE
                    MAX(original_rows) OVER(PARTITION BY {table_col_str})
                END
                    AS cal_or,
                    rows_affected,
                    status
                FROM ( {unioned_query} )
                ) 
            order by {table_col_str},
            action,
            original_rows DESC,
            rows_affected
    """

    anomaly_query = f"""
    select * from
    ({final_query})
    """

    anomaly_table = wc.execute_query(anomaly_query, True)
    anomaly_table_html = build_table(
        anomaly_table, "blue_light", font_size="10px", text_align="center"
    ).replace('"', "'").replace("<table","<table style='display: block; overflow-y: auto; white-space: nowrap; height:400px; width:100%'")

    if notification_type == NotificationType.SUCCESS.value:
        email_body = {
            "header": "Data Sourcing & Ingestion Pipeline Status",
            "summary": "Ingestion completed successfully with the latest data synced to the tool. Below is the summary of the ingested data:",
            "end_time":str(datetime.utcnow()),
            "tenant":Variable.get("tenant_alias"),
            "pipeline": Variable.get("pipeline"),
            "date": str(datetime.today().date()),
            "share_anomaly_data": eval(Variable.get("share_anomaly_data", default_var="False")),
            "validation": {
                "anomaly_table": anomaly_table_html
            },
            "product_name": Variable.get("product"),  
            "footer": " This is auto generated mail, please do not reply! ",
        }
    elif notification_type == NotificationType.FAILURE.value:
        email_body = {
            "header": "Data Sourcing & Ingestion Pipeline Status",
            "summary": "Ingestion failed at validation due to data loss threshold breach. System is up with the data available from last successful refresh . Below is the summary of the ingested data:",
            "end_time":str(datetime.utcnow()),
            "tenant":Variable.get("tenant_alias"),
            "pipeline": Variable.get("pipeline"),
            "date": str(datetime.today().date()),
            "share_anomaly_data": eval(Variable.get("share_anomaly_data", default_var="False")),
            "validation": {
                "anomaly_table": anomaly_table_html
            },
            "product_name": Variable.get("product"), 
            "footer": " This is auto generated mail, please do not reply! ",
        }
    elif notification_type == NotificationType.QC.value:
        email_body = {
            "header": "Data Sourcing & Ingestion Pipeline Status",
            "summary": "Ingestion completed successfully with the latest data synced to the tool. Below is the summary of the ingested data:",
            "end_time":str(datetime.utcnow()),
            "tenant":Variable.get("tenant_alias"),
            "pipeline": Variable.get("pipeline"),
            "date": str(datetime.today().date()),
            "validation": {
                "anomaly_table": anomaly_table_html
            },
            "product_name": Variable.get("product"), 
            "footer": " This is auto generated mail, please do not reply! ",
        }
    Variable.set("email_body",email_body)
    return email_body

def get_mailing_info(notification_type: str):
    
    import urllib
    import pandas as pd
    from airflow.models import Variable
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    
    conn_string = (
        "postgresql://"
        + Variable.get("PGXUSER")
        + ":"
        + urllib.parse.quote(Variable.get("PGXPASSWORD"))
        + "@"
        + Variable.get("PGXHOST")
        + ":"
        + Variable.get("PGXPORT")
        + "/"
        + Variable.get("PGXDATABASE")
    )
    db_engine = create_engine(conn_string, poolclass=NullPool)
    
    mailing_info = pd.read_sql( 
        f"""select * FROM global.mail_notification_mapping
            where tenant='{Variable.get('tenant_alias')}' and pipeline='{Variable.get('pipeline')}' and type='{notification_type}' """ , db_engine)
    subject=mailing_info.subject.iloc[0]
    recipients=mailing_info.recipients.iloc[0]
    mail_url=mailing_info.url.iloc[0]
    return subject,recipients,mail_url
    
def send_mail(email_body, notification_type: str):
    from airflow.models import Variable
    from tasks.utils import idtoken_from_metadata_server
    import json
    import requests
    url = Variable.get("mail_url")
    token = idtoken_from_metadata_server(url)
    subject,recipients,url = get_mailing_info(notification_type=notification_type)

    payload = json.dumps(
        {
            "subject": f"{subject}",
            "body": eval(Variable.get("email_body")),
            "recipients": eval(recipients),
            "template_directory":"/notifications/mail_templates/",
            "template": "ingestion_mail_template.html",
        }
    )
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
        }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response

def send_validation_notification():
    from airflow.models import Variable
    from constants.constant import NotificationType
    from airflow.exceptions import AirflowFailException
    if eval(Variable.get("ignore_data_loss_threshold", default_var="True")):
        return
    entities = eval(Variable.get('entity_type'))
    validation_pass = True
    for entity in entities:
        if not eval(Variable.get(f"validation_status_{entity}", default_var="True")):
            validation_pass = False
            break
    if validation_pass:
        return
    send_notification(notification_type=NotificationType.FAILURE.value)
    raise AirflowFailException("Task failed as data loss after validation crossed the configured threshold.")

def send_notification(notification_type: str):
    from airflow.models import Variable
    email_body=create_email_body(notification_type=notification_type)
    response=send_mail(email_body=email_body, notification_type=notification_type)
    Variable.set("email_api_response", response.text)
