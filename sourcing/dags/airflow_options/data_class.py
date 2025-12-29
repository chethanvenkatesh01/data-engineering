from dataclasses import dataclass
from datetime import timedelta

@dataclass
class DagOptions:
    owner: str 
    retries: int 
    retry_delay: timedelta
    email_on_failure: bool 
    email_on_retry: bool 
