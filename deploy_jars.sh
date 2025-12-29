 

export CURRENT_WORK_DIR=$(pwd)

# Setting up Java
if [[ $(javac -version ) =~ .*"11".* ]];
then
    echo "Java 11 alredy exists"
else
    echo "Java 11 not found. Installing openjdk-11-jdk"
    sudo apt-get update && \
    sudo apt-get install ca-certificates-java -y && \
    sudo apt-get install openjdk-11-jdk -y

    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
    export PATH=$PATH:$JAVA_HOME/bin

    java -version

    if [[ $? == 0 ]];
    then
        echo "Java SDK 11 installation successful"
    else
        echo "Java SDK 11 installation failed"
        exit 1
    fi
fi

# Setting up maven
echo "Installing Maven"
curl -O https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz
tar -xzf apache-maven-3.9.6-bin.tar.gz
export PATH=$PATH:$CURRENT_WORK_DIR/apache-maven-3.9.6/bin

echo "PATH: ${PATH}"

mvn -version

if [[ $? == 0 ]];
then
    echo "Maven Successfully Installed"
fi

# Build JAR files
DATAFLOW_DIR=$CURRENT_WORK_DIR/ingestion/dags/dataflow-pipelines-java

declare -A JAR_TYPE_TO_DIR_MAPPING_ARRAY

JAR_TYPE_TO_DIR_MAPPING_ARRAY["bigquery-to-db"]="bq-to-db/bq-to-db-common;bq-to-db/bq-to-db-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["bigquery-to-db-copy-multi"]="bq-to-db/bq-to-db-common;bq-to-db/bq-to-db-copy-multi-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["db-to-bigquery"]="db-to-bq/db-to-bq-common;db-to-bq/db-to-bq-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["gcs-to-bigquery"]="gcs-to-bigquery/gcs-to-bigquery-common;gcs-to-bigquery/gcs-to-bigquery-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["gcs-to-snowflake"]="gcs-to-snowflake/gcs-to-snowflake-common;gcs-to-snowflake/gcs-to-snowflake-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["sftp-to-bigquery"]="sftp-to-bigquery/sftp-to-bigquery-common;sftp-to-bigquery/sftp-to-bigquery-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["snowflake-to-bigquery"]="snowflake-to-bigquery/snowflake-to-bigquery-common;snowflake-to-bigquery/snowflake-to-bigquery-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["snowflake-to-db"]="sf-to-db/sf-to-db-common;sf-to-db/sf-to-db-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["snowflake-to-db-copy-multi"]="sf-to-db/sf-to-db-common;sf-to-db/sf-to-db-copy-multi-pipeline"
JAR_TYPE_TO_DIR_MAPPING_ARRAY["db-to-snowflake"]="db-to-sf/db-to-sf-common;db-to-sf/db-to-sf-pipeline"


if [ $1 == 'all' ];
then
    echo "Building all JAR files"
    cd $DATAFLOW_DIR
    mvn -q clean install
    echo "Successfully created all jar files"
else
    cd $DATAFLOW_DIR/common
    echo "Building jar file for $(pwd)"
    mvn -q clean install
    echo "Successfully created jar file for $(pwd)"
    for dir_path in $(echo ${JAR_TYPE_TO_DIR_MAPPING_ARRAY[$1]} | tr ";" "\n")
    do
        cd $DATAFLOW_DIR/$dir_path
        echo "Building jar file for $(pwd)"
        mvn -q clean install
        echo "Successfully created jar file for $(pwd)"
    done
fi

# Copy JAR files
pip3 install google-cloud-storage==2.14.0

mkdir $CURRENT_WORK_DIR/jars
cp $DATAFLOW_DIR/*/*/*/*.jar $CURRENT_WORK_DIR/jars/
ls -l $CURRENT_WORK_DIR/jars/

cd $CURRENT_WORK_DIR

python3 ./copy_jars.py --jar_type=$3 --branch=$BITBUCKET_BRANCH --jars_directory=$CURRENT_WORK_DIR/jars



