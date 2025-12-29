from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SendNotificationMailOperator(BaseOperator):
    """
    Custom operator to send notification email based on the trigger type.
    """

    template_fields = ['trigger_type']

    @apply_defaults
    def __init__(
        self,
        trigger_type: str,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.trigger_type = trigger_type

    def _render_template_fields(self, context, fields):
        """Render template fields."""
        for field in fields:
            setattr(
                self,
                field,
                self.render_template(getattr(self, field), context)
            )

    def execute(self, context):
        from airflow.operators.python_operator import PythonOperator
        from airflow.utils.trigger_rule import TriggerRule
        from airflow_options.notifications.notification_summary import send_sourcing_notification_mail

        self._render_template_fields(context, self.template_fields)

        if self.trigger_type == 'all_done':
            trigger_rule = TriggerRule.ALL_DONE
        else:
            trigger_rule = TriggerRule.ALL_SUCCESS

        send_notification_mail = PythonOperator(
            task_id="send_notification_mail",
            python_callable=send_sourcing_notification_mail,
            trigger_rule=trigger_rule,
            provide_context=True,
            dag=self.dag
        )

        return send_notification_mail.execute(context)