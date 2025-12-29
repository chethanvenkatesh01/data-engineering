from dataflow_options.utils import Logger

log = Logger(__name__)

def send_mail(email_body, template_name, template_dir="/notifications/mail_templates"):
    from airflow.models import Variable
    from airflow_options.utils import idtoken_from_metadata_server
    import json
    import requests
    url = Variable.get("mail_url")
    token = idtoken_from_metadata_server(url)
    payload = json.dumps(
        {
            "subject": email_body.get("subject"),
            "body": email_body,
            "recipients": email_body.get('mailing_list'),
            "template_directory": template_dir,
            "template": template_name,
        }
    )
    Variable.set("send_email_payload", payload)
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response