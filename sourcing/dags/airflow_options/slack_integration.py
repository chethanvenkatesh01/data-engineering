from enum import Enum
from dataflow_options.utils import Logger

log = Logger(__name__)


class slackWebapis(str, Enum):
    postMessage = "https://slack.com/api/chat.postMessage"
    listConversations = "https://slack.com/api/conversations.list"
    createConversation = "https://slack.com/api/conversations.create"


class slack_notification:
    def __init__(self, project_name, slack_secret_name):
        from google.cloud import secretmanager
        from airflow.models import Variable
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_name}/secrets/{slack_secret_name}/versions/latest"
        response = client.access_secret_version(name=name)
        self.slack_token = response.payload.data.decode("UTF-8")
        self.slack_channel = Variable.get("slack_channel_name")

    def send_slack_message(self, message, thread_ts=""):
        from airflow.models import Variable
        import json
        import requests
        data = {
            "channel": self.slack_channel,
            "thread_ts": Variable.get("slack_notification_thread_ts", default_var="")
            if not thread_ts
            else thread_ts,
        }
        data.update(message)

        data = json.dumps(data)
        try:
            response = requests.post(
                url=slackWebapis.postMessage.value,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": self.slack_token,
                },
                data=data,
            )

            return response.json()
        except Exception as e:
            Variable.set("ERROR:send_slack_message", str(e))

    def create_channel_if_not_exist(self):
        import requests
        from airflow.models import Variable
        try:
            response = requests.get(
                url=slackWebapis.listConversations.value,
                headers={"Authorization": self.slack_token},
            )
            self.channels = []
            for _ in response.json()["channels"]:
                self.channels.append(_["name"])
            Variable.set("channels", self.channels)
            if self.slack_channel not in self.channels:
                response = requests.post(
                    url=slackWebapis.createConversation.value,
                    headers={"Authorization": self.slack_token},
                    data={"name": self.slack_channel},
                )
                return response.json()
        except Exception as e:
            Variable.set("ERROR:create_channel_if_not_exist", str(e))

    def first_task_started(self, context):
        from airflow.models import Variable
        from datetime import datetime
        try:
            self.create_channel_if_not_exist()
            today = datetime.now().strftime("%d, %b %Y  %I:%M:%S %p")
            log.info(f"context: {str(context)}")
            message = {
                "text": f"{context['task_instance_key_str'].split('__')[0]} has started :white_check_mark:",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{context['task_instance_key_str'].split('__')[0]} has started :white_check_mark:",
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Name:* {Variable.get('tenant')} {Variable.get('pipeline')}",
                        },
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"_{today}_"},
                    },
                    {"type": "divider"},
                ],
            }
            log.info(f"message : {message}")
            Variable.set("slack_notification_thread_ts", "")
            response = self.send_slack_message(message)
            log.info(f"response sent : {str(response)}")
            Variable.set("slack_notification_thread_ts", response["message"]["ts"])
        except Exception as e:
            Variable.set("ERROR:first_task_started", e)

    def on_task_success(self, context):
        from airflow.models import Variable
        try:
            message = {
                "text": f"""*{context["task_instance_key_str"].split("__")[1]}* :white_check_mark:"""
            }
            response = self.send_slack_message(message)
        except Exception as e:
            Variable.set("ERROR:on_task_success", str(e))

    def on_task_failure(self, **context):
        from airflow.models import Variable
        import os
        from airflow.utils.state import TaskInstanceState
        try:
            Variable.set("slack_failure_context", context)
            exception = context.get("exception")
            # formatted_exception = ''.join(
            #     traceback.format_exception(etype=type(exception),
            #         value=exception, tb=exception.__traceback__
            #     )
            # ).strip()
            # formatted_exception = str(exception)
            ti = context["task_instance"]
            di = ""
            tk = ""
            lu = ""
            message_list = []
            newline = " \n "
            for i, t in enumerate(
                ti.get_dagrun().get_task_instances(state=TaskInstanceState.FAILED)
            ):  # type: TaskInstance
                di = t.dag_id
                tk = t.task_id
                lu = t.log_url
                message = f"""Failed task-{i+1}: {di}, task: {tk}, """
                if os.getenv("vm_ip"):
                    message += f"""url: {lu.replace('localhost',os.getenv('vm_ip'))}{newline}"""
                else:
                    message += f"Configure vm_ip in pipeline to get the link to error log on slack.{newline}"
                message_list.append(message)

            message = {
                "text": f"""
                    *Pipeline Has failed, below are the failed tasks* :x:{newline}{newline.join(message_list)}
                    """
            }
            if len(message_list):
                response = self.send_slack_message(message)
        # Variable.delete('slack_notification_thread_ts')

        except Exception as e:
            Variable.set("ERROR:on_task_failure", str(e))

    def last_task_completed(self, context):
        from airflow.models import Variable
        try:
            message = {
                "text": f"{context['task_instance_key_str'].split('__')[0]} has completed :white_check_mark:  "
            }
            response = self.send_slack_message(message)
            # Variable.delete('slack_notification_thread_ts')
        except Exception as e:
            Variable.set("ERROR:last_task_completed", str(e))


def slack__first_task_success(context):
    from airflow.models import Variable
    sn = slack_notification(
        project_name=Variable.get("gcp_project"),
        slack_secret_name="slack_notification_token",
    )
    sn.first_task_started(context)


def slack__on_task_success(context):
    from airflow.models import Variable
    sn = slack_notification(
        project_name=Variable.get("gcp_project"),
        slack_secret_name="slack_notification_token",
    )
    sn.on_task_success(context)


def slack__on_task_failure(**context):
    from airflow.models import Variable
    sn = slack_notification(
        project_name=Variable.get("gcp_project"),
        slack_secret_name="slack_notification_token",
    )
    sn.on_task_failure(**context)


def slack__last_task_success(context):
    from airflow.models import Variable
    sn = slack_notification(
        project_name=Variable.get("gcp_project"),
        slack_secret_name="slack_notification_token",
    )
    sn.last_task_completed(context)
