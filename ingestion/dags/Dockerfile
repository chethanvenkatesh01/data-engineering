FROM python:3.7-slim
#FROM gcr.io/data-ingestion-342714/sourcing-dataflow-base:latest

WORKDIR /pipeline

COPY requirements.txt .

# RUN apt-get update && \
#     apt-get install -y postgresql-server-dev-all gcc python3-dev musl-dev

RUN pip install -r requirements.txt \
    && pip check

# RUN pip install --no-cache-dir --upgrade pip \
#     && pip install --no-cache-dir -r requirements.txt \
#     && pip check

COPY constants ./constants
COPY tasks ./tasks
COPY * ./

COPY --from=apache/beam_python3.7_sdk:2.36.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT [ "/opt/apache/beam/boot" ]