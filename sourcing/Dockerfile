FROM apache/airflow:2.6.3-python3.8
USER root
# Set vm ip
RUN export vm_ip=$(hostname -i)
# Install OpenJDK-11
RUN apt update && \
    apt-get install ca-certificates-java -y && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install gcc -y && \
    apt-get install libpq-dev python-dev -y && \
    apt-get install jq -y && \
    apt-get clean;
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

RUN chmod -R 777 logs/

USER airflow
COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt 

# Create volume for logs
VOLUME /usr/local/airflow/logs
RUN mkdir -p dags plugins
