package com.impact;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.sftp.SftpSink;
import com.impact.utils.snowflake.SnowflakeClient;
import com.impact.utils.snowflake.SnowflakeSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.*;

@Slf4j
public class SnowflakeToSftpPipeline {
    public static void main(String[] args) throws Exception {
        SnowflakeToSftpPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(SnowflakeToSftpPipelineOptions.class);
        run(pipelineOptions);
    }

    public static SnowflakeIO.CsvMapper<String> getCsvMapper(String fieldDelimiter) {
        return (SnowflakeIO.CsvMapper<String>) parts -> {
            return String.join(fieldDelimiter, parts);
        };
    }
    private static void run(SnowflakeToSftpPipelineOptions pipelineOptions) throws Exception {

        String projectId = pipelineOptions.as(GcpOptions.class).getProject();

        // Snowflake Params
        String snowflakeTable = pipelineOptions.getSnowflakeTable();
        String snowflakeQuery = pipelineOptions.getSnowflakeQuery();
        String storageIntegration = pipelineOptions.getStorageIntegration();
        String stagingBucket = pipelineOptions.getStagingBucket();
        String snowflakeSecretName = pipelineOptions.getSnowflakeSecretName();
        String snowflakeServer = pipelineOptions.getSnowflakeServerName();
        String snowflakeUserName = pipelineOptions.getSnowflakeUserName();
        String snowflakePassword = pipelineOptions.getSnowflakePassword();
        String snowflakeDatabase = pipelineOptions.getSnowflakeDatabase();
        String snowflakeSchema = pipelineOptions.getSnowflakeSchema();
        String snowflakeWarehouse = pipelineOptions.getSnowflakeWarehouse();
        String snowflakeUserRole = pipelineOptions.getSnowflakeUserRole();

        // SFTP params
        String sftpHost = pipelineOptions.getSftpHost();
        String sftpUsername = pipelineOptions.getSftpUsername();
        String sftpPassword = pipelineOptions.getSftpPassword();
        String sftpSecretName = pipelineOptions.getSftpSecretName();
        String fieldDelimiter = pipelineOptions.getFieldDelimiter();
        String outputDirectory = pipelineOptions.getOutputDirectory();
        String outputFilePrefix = pipelineOptions.getOutputFilePrefix();


        if (sftpSecretName != null) {
            log.info("Fetching the SFTP credentials from secret manager");
            log.info(String.format("Secret name %s", sftpSecretName));
            String secret = SecretManager.getSecret(projectId, sftpSecretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);

            for (String key : config.keySet()) {
                if (key.contains("host") || key.contains("server")) sftpHost = config.get(key).getAsString();
                else if (key.contains("user")) sftpUsername = config.get(key).getAsString();
                else if (key.contains("password")) sftpPassword = config.get(key).getAsString();
            }
            log.info(String.format("Host: %s; User: %s", sftpHost, sftpUsername));
        }


        if(snowflakeSecretName!=null) {
            log.info("Fetching the snowflake credentials from secert manager");
            String secret = SecretManager.getSecret(projectId, snowflakeSecretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);
            for(String key : config.keySet()) {
                if(key.contains("server")) snowflakeServer = config.get(key).getAsString();
                else if (key.contains("account")) snowflakeServer = String.format("%s.snowflakecomputing.com", config.get(key).getAsString());
                else if(key.contains("user")) snowflakeUserName = config.get(key).getAsString();
                else if(key.contains("password")) snowflakePassword = config.get(key).getAsString();
                else if(key.contains("database") || key.contains("db")) snowflakeDatabase = config.get(key).getAsString();
                else if(key.contains("schema")) snowflakeSchema = config.get(key).getAsString();
                else if(key.contains("warehouse")) snowflakeWarehouse = config.get(key).getAsString();
                else if(key.contains("role") && snowflakeUserRole==null) snowflakeUserRole = config.get(key).getAsString();
                else if(key.contains("storage_integration")) storageIntegration = config.get(key).getAsString();
                else if(key.contains("staging_bucket")) stagingBucket = config.get(key).getAsString();
            }
        }

        SnowflakeClient sfClient = SnowflakeClient.builder().withServer(snowflakeServer)
                .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                .withDatabase(snowflakeDatabase)
                .withDbSchema(snowflakeSchema)
                .withWarehouse(snowflakeWarehouse)
                .withUserRole(snowflakeUserRole);

        SnowflakeSchema sfSchema = null;
        String header =null;
        String query = null;

        Map<String, String> sfSchemaMap = new LinkedHashMap<>();
        if(snowflakeQuery!=null) {
            log.info(snowflakeQuery);
            query = snowflakeQuery;
            sfSchema = sfClient.getQueryResultSchema(query);
            sfSchemaMap = sfClient.convertSchemaToMap(sfSchema);
            StringJoiner joiner = new StringJoiner(fieldDelimiter);
            for (String key : sfSchemaMap.keySet()) {
                joiner.add(key);
            }
            header = joiner.toString();
        }
        else if(snowflakeTable!=null) {
            query = String.format("SELECT * FROM %s.%s.%s", snowflakeDatabase, snowflakeSchema, snowflakeTable);
            log.info(query);
            sfSchema = sfClient.getQueryResultSchema(query);
            log.info(sfSchema.toString());
            sfSchemaMap = sfClient.convertSchemaToMap(sfSchema);
            StringJoiner joiner = new StringJoiner(fieldDelimiter);
            for (String key : sfSchemaMap.keySet()) {
                joiner.add(key);
            }
            header = joiner.toString();
        }

        String finalQuery = null;

        if (snowflakeQuery != null) {
            finalQuery = snowflakeQuery;
        } else if (snowflakeTable != null) {
            finalQuery = String.format("SELECT * FROM %s.%s.%s ", snowflakeDatabase, snowflakeSchema, snowflakeTable);
        } else {
            throw new Exception("Either sourceQuery or bigQueryTable argument should be provided");
        }

        SnowflakeIO.DataSourceConfiguration snowflakeConfig =
                SnowflakeIO.DataSourceConfiguration.create()
                        .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                        .withServerName(snowflakeServer)
                        .withWarehouse(snowflakeWarehouse)
                        .withDatabase(snowflakeDatabase)
                        .withSchema(snowflakeSchema)
                        .withRole(snowflakeUserRole);

        Pipeline p = Pipeline.create(pipelineOptions);

        PCollection<String> rows = p.apply("Read From Snowflake", SnowflakeIO.<String>read()
                .withDataSourceConfiguration(snowflakeConfig)
                .fromQuery(finalQuery)
                .withStagingBucketName(stagingBucket)
                .withStorageIntegrationName(storageIntegration)
                .withCsvMapper(getCsvMapper(fieldDelimiter))
                .withCoder(StringUtf8Coder.of()));

        rows.apply("Write To File", ParDo.of(SftpSink.builder().withConfiguration(sftpHost, sftpUsername, sftpPassword)
                .withDirectory(outputDirectory).withFilePrefix(outputFilePrefix).withHeader(header).withDelimiter(fieldDelimiter)));

        p.run();
    }
}
