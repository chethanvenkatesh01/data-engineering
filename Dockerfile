FROM apache/airflow:2.5.3

ARG module=sourcing

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install ca-certificates-java -y && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install gcc -y && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk -y

# Create volume for logs
USER airflow

# Install python libraries
COPY $module/requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy dags
COPY $module/dags ./dags
RUN chmod -R 777 logs/