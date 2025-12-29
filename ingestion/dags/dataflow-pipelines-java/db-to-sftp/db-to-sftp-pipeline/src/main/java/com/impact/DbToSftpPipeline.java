package com.impact;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.db.DbManager;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.sftp.SftpSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

@Slf4j
public class DbToSftpPipeline {
    public static void main(String[] args) throws Exception {
        DbToSftpPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(DbToSftpPipelineOptions.class);
        run(pipelineOptions);
    }

    private static void run(DbToSftpPipelineOptions pipelineOptions) throws Exception {

        String projectId = pipelineOptions.as(GcpOptions.class).getProject();

        //DB Params
        String dbHost = pipelineOptions.getDbHost();
        String dbPort = pipelineOptions.getDbPort();
        String dbUser = pipelineOptions.getDbPort();
        String dbPassword = pipelineOptions.getDbPassword();
        String database = pipelineOptions.getDbName();
        String dbSchemaName = pipelineOptions.getDbSchemaName();
        String dbType = pipelineOptions.getDbType();
        String dbSecretName = pipelineOptions.getDbSecretName();
        String dbTable = pipelineOptions.getDbTable();
        String sourceQuery = pipelineOptions.getSourceQuery();

        //SFTP params
        String sftpHost = pipelineOptions.getSftpHost();
        String sftpUsername = pipelineOptions.getSftpUsername();
        String sftpPassword = pipelineOptions.getSftpPassword();
        String sftpSecretName = pipelineOptions.getSftpSecretName();
        String fieldDelimiter = pipelineOptions.getFieldDelimiter();
        String outputDirectory = pipelineOptions.getOutputDirectory();
        String outputFilePrefix = pipelineOptions.getOutputFilePrefix();

        if(dbSecretName!=null) {
            log.info("Fetching the DB credentials from secert manager");
            log.info(String.format("Secret name %s", dbSecretName));
            String secret = SecretManager.getSecret(projectId, dbSecretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);

            for(String key : config.keySet()) {
                if(key.contains("host") || key.contains("server")) dbHost = config.get(key).getAsString();
                else if(key.contains("port")) dbPort = config.get(key).getAsString();
                else if(key.contains("user")) dbUser = config.get(key).getAsString();
                else if(key.contains("password")) dbPassword = config.get(key).getAsString();
                else if(key.contains("database") || key.contains("db")) database = config.get(key).getAsString();
                else if(key.contains("schema") && dbSchemaName==null) dbSchemaName = config.get(key).getAsString();
                else if(key.contains("dbType")) dbType = config.get(key).getAsString();
            }
            log.info(String.format("Host: %s; Port: %s; Database: %s; Schema: %s", dbHost, dbPort, database, dbSchemaName));
        }

        if(sftpSecretName!=null) {
            log.info("Fetching the SFTP credentials from secert manager");
            log.info(String.format("Secret name %s", sftpSecretName));
            String secret = SecretManager.getSecret(projectId, sftpSecretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);

            for(String key : config.keySet()) {
                if(key.contains("host") || key.contains("server")) sftpHost = config.get(key).getAsString();
                else if(key.contains("user")) sftpUsername = config.get(key).getAsString();
                else if(key.contains("password")) sftpPassword = config.get(key).getAsString();
            }
            log.info(String.format("Host: %s; Port: %s; Database: %s; Schema: %s", dbHost, dbPort, database, dbSchemaName));
        }

        String finalQuery = null;

        if(sourceQuery!=null) {
            finalQuery = sourceQuery;
        }
        else if(dbTable!=null) {
            finalQuery = String.format("SELECT * FROM %s.%s", dbSchemaName, dbTable);
        }
        else {
            throw new Exception("Either sourceQuery or dbTable argument should be provided");
        }

        DbManager dbManager = new DbManager(dbHost, dbPort, dbUser, dbPassword, database, dbType);
        String driverClassName = DbManager.getDbDriver(dbType);
        String connectionUrl = dbManager.getConnectionUrl();
        log.info(String.format("Driver: %s\nConnection Url: %s", driverClassName, connectionUrl));

        Pipeline p = Pipeline.create(pipelineOptions);

        PCollection<String> rows = p.apply("Read from DB", JdbcIO.<String>read().withDataSourceConfiguration(
                        JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl).withUsername(dbUser).withPassword(dbPassword))
                .withQuery(finalQuery)
                .withRowMapper(new MapToString(fieldDelimiter)));

        rows.apply("Write To File", ParDo.of(SftpSink.builder().withConfiguration(sftpHost, sftpUsername, sftpPassword)
                .withDirectory(outputDirectory).withFilePrefix(outputFilePrefix)));

        p.run();


    }
}
