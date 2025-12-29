from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

def validate_start_of_intermediate_queries():
    import logging
    from airflow.models import Variable
    log = logging.getLogger(__name__)
    view_trigger_mandatory_mapping = eval(
        Variable.get("view_trigger_mandatory_mapping", default_var="{}")
    )

    view_trigger_status_mapping = eval(
        Variable.get("view_trigger_status_mapping", default_var="{}")
    )
    mandatory_views = [
        view for view, trigger in view_trigger_mandatory_mapping.items() if trigger
    ]
    log.info(f"Mandatory Views: {mandatory_views}")
    for view in mandatory_views:
        if not view_trigger_status_mapping[view]:
            raise Exception(f"The trigger for {view} view is mandatory")

    return True

class ValidateIntermediateOperator(BaseOperator):
    """
    Custom operator to send notification email based on the trigger type.
    """

    template_fields = ['extraction_flag']

    @apply_defaults
    def __init__(
        self,
        extraction_flag: str,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.extraction_flag = extraction_flag

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

        self._render_template_fields(context, self.template_fields)

        if self.extraction_flag:
            trigger_rule = TriggerRule.ALL_DONE
        else:
            trigger_rule = TriggerRule.NONE_FAILED

        validate_intermediate_queries = PythonOperator(
            task_id="validate_start_of_intermediate_queries",
            python_callable=validate_start_of_intermediate_queries,
            trigger_rule=trigger_rule,
            provide_context=True
        )

        return validate_intermediate_queries.execute(context)