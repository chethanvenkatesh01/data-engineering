package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.db.DbManager;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.snowflake.SnowflakeClient;
import com.impact.utils.snowflake.SnowflakeSchema;
import com.impact.utils.snowflake.SnowflakeSchemaConversion;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
// import org.apache.beam.sdk.transforms.Reshuffle;

import java.util.LinkedHashMap;
import java.util.Map;
@Slf4j
public class SfToDbPipeline {

    public static void main(String [] args) throws Exception {
        SfToDbPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SfToDbPipelineOptions.class);
        run(options);
    }

    public static SnowflakeIO.CsvMapper<String> getCsvMapper(String fieldDelimiter) {
        return (SnowflakeIO.CsvMapper<String>) parts -> {
            return String.join(fieldDelimiter, parts);
        };
    }

    public static void run(SfToDbPipelineOptions options) throws Exception {

        String project = options.as(GcpOptions.class).getProject();

        // SF params
        String snowflakeTable = options.getSnowflakeTable();
        String storageIntegration = options.getStorageIntegration();
        String stagingBucket = options.getStagingBucket();
        boolean replaceSpecialChars = options.getReplaceSpecialChars();
        String snowflakeSecretName = options.getSnowflakeSecretName();
        String snowflakeServer = options.getSnowflakeServerName();
        String snowflakeUserName = options.getSnowflakeUserName();
        String snowflakePassword = options.getSnowflakePassword();
        String snowflakeDatabase = options.getSnowflakeDatabase();
        String snowflakeSchema = options.getSnowflakeSchema();
        String snowflakeWarehouse = options.getSnowflakeWarehouse();
        String snowflakeUserRole = options.getSnowflakeUserRole();
        String query = options.getSourceQueries();
        String[] queries = (query != null) ? query.split(";") : new String[]{};


        // DB params
        String host = options.getDbHost();
        String port = options.getDbPort();
        String user = options.getDbUser();
        String password = options.getDbPassword();
        String db = options.getDbName();
        String schemaName = options.getDbSchemaName();
        String dbTable = options.getDbTable();
        String dbTableSchema = options.getDbTableSchema();

        String secretName = options.getDbSecretName();
        boolean replaceTable = "true".equalsIgnoreCase(String.valueOf(options.getReplaceTable())) ? true : false;
        String pullType = options.getPullType();
        String incrementalColumn = options.getIncrementalColumn();
        String incrementalColumnValue = options.getIncrementalColumnValue();
        Integer batchSize = options.getBatchSize(); // Uses 1000 if not provided

        if(snowflakeSecretName!=null) {
            log.info("Fetching the snowflake credentials from secert manager");
            String secret = SecretManager.getSecret(project, snowflakeSecretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);
            for(String key : config.keySet()) {
                if (key.contains("account")) snowflakeServer = String.format("%s.snowflakecomputing.com", config.get(key).getAsString());
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
        SnowflakeIO.DataSourceConfiguration snowflakeConfig =
                SnowflakeIO.DataSourceConfiguration.create()
                        .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                        .withServerName(snowflakeServer)
                        .withWarehouse(snowflakeWarehouse)
                        .withDatabase(snowflakeDatabase)
                        .withSchema(snowflakeSchema)
                        .withRole(snowflakeUserRole);

        if(secretName!=null) {
            log.info("Fetching the credentials from secert manager");
            log.info(String.format("Secret name %s", secretName));
            String secret = SecretManager.getSecret(project, secretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);

            for(String key : config.keySet()) {
                if(key.contains("host") || key.contains("server")) host = config.get(key).getAsString();
                else if(key.contains("port")) port = config.get(key).getAsString();
                else if(key.contains("user")) user = config.get(key).getAsString();
                else if(key.contains("password")) password = config.get(key).getAsString();
                else if(key.contains("database") || key.contains("db")) db = config.get(key).getAsString();
                else if(key.contains("schema") && schemaName==null) schemaName = config.get(key).getAsString();
            }
            log.info(String.format("Host: %s;Port: %s;Database: %s", host, port, db));
        }
        else {
            log.info("secretName is null. Hence using the values passed to host,port,user,password by default");
        }
        // -------------------------  Validate DB Type and create full table name -------------------------
        String dbType = null;
        String tableId = null;
        if("postgresql".equals(options.getDbType().toLowerCase()) || "postgres".equals(options.getDbType().toLowerCase())
                ||"pg".equals(options.getDbType().toLowerCase() )) {
            dbType = "postgresql";
            tableId = String.format("%s.%s", schemaName, dbTable);
            port = port!=null ? port : "5432";
        }
        else if("mysql".equals(options.getDbType().toLowerCase())) {
            dbType = "mysql";
            tableId = String.format("%s.%s", schemaName, dbTable);
            port = port!=null ? port : "3306";
        }
        else if("mssql".equals(options.getDbType().toLowerCase()) || "sqlserver".equals(options.getDbType().toLowerCase())) {
            dbType = "mssql";
            tableId = String.format("%s.%s.%s", db, schemaName, dbTable);
            port = port!=null ? port : "1433";
        }
        else if("oracle".equals(options.getDbType().toLowerCase())) {
            dbType = "oracle";
            tableId = String.format("%s.%s", schemaName, dbTable);
            port = port != null ? port : "1521";
        }
        else {
            log.error(String.format("dbType %s is not supported", options.getDbType()));
            throw new Exception(String.format("dbType %s is not supported", options.getDbType()));
        }
        // Sf Schema
        SnowflakeClient sfClient = SnowflakeClient.builder().withServer(snowflakeServer)
                .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                .withDatabase(snowflakeDatabase)
                .withDbSchema(snowflakeSchema)
                .withWarehouse(snowflakeWarehouse)
                .withUserRole(snowflakeUserRole);
        SnowflakeSchema sfSchema = null;
        Map<String, String> sfSchemaMap = new LinkedHashMap<>();
        if(queries.length > 0 && !queries[0].equals("")) {
            query = queries[0];
            log.info(query);
            sfSchema = sfClient.getQueryResultSchema(query);
            sfSchemaMap = sfClient.convertSchemaToMap(sfSchema);
        }
        else if(query!=null) {
            query = pullType.toLowerCase().equals("incremental") ?
                    query + String.format(" WHERE %s > %s", incrementalColumn, incrementalColumnValue) : query;
            log.info(query);
            sfSchema = sfClient.getQueryResultSchema(query);
            sfSchemaMap = sfClient.convertSchemaToMap(sfSchema);
        }
        else if(snowflakeTable!=null) {
            query = String.format("SELECT * FROM %s.%s.%s", snowflakeDatabase, snowflakeSchema, snowflakeTable);
            query = pullType.toLowerCase().equals("incremental") ?
                    query + String.format(" WHERE %s > %s", incrementalColumn, incrementalColumnValue) : query;
            log.info(query);
            sfSchema = sfClient.getQueryResultSchema(query);
            sfSchemaMap = sfClient.convertSchemaToMap(sfSchema);
        }
        for(String field : sfSchemaMap.keySet()) {
            log.info(String.format("key  %s value %s ", field,sfSchemaMap.get(field)));

        }
        // Map<String, Map<String,String>> bqSchema = bqClient.getTableSchema(dataset, bqTable);
        // DB Schema
        Map<String, String> dbSchema = new LinkedHashMap<>();
        log.info(String.format("Fetching dbtype %s", dbType));
        if(dbTableSchema!=null) {
            // DB Table schema is provided. Overriding it with schema derived from snowflake table
            dbSchema = SnowflakeSchemaConversion.sfToDb(sfSchemaMap, dbTableSchema, dbType);
        }
        else {
            dbSchema = SnowflakeSchemaConversion.sfToDb(sfSchemaMap, dbType);
        }


        DbManager dbManager = new DbManager(host, port, user, password, db, dbType);
        //dbManager.createTable(table, "public", dbSchema, false, true);
        //dbManager.createTable(dbTable, schemaName, dbSchema, false, true);
        dbManager.createTablev3(String.format("%s.%s", schemaName, dbTable), dbSchema, replaceTable, true);

        String driverClassName = DbManager.getDbDriver(dbType);
        String connectionUrl = dbManager.getConnectionUrl();
        log.info(String.format("Driver: %s\nConnection Url: %s", driverClassName, connectionUrl));
        //log.info(dbManager.getInsertQuery(table, "public", dbSchema));
        log.info(dbManager.getInsertQuery(dbTable, schemaName, dbSchema));

        Pipeline p = Pipeline.create(options);

        if(queries.length > 0 && !queries[0].equals("")) {
            for(int i=0; i<queries.length; i++) {
                query = queries[i];
                log.info(query);
                PCollection<TableRow> inputRows = p.apply("Read From Snowflake", SnowflakeIO.<TableRow>read()
                        .withDataSourceConfiguration(snowflakeConfig)
                        .fromQuery(query)
                        .withStagingBucketName(stagingBucket)
                        .withStorageIntegrationName(storageIntegration)
                        .withCsvMapper(CsvMapperToTableRow.builder().withSfSchema(sfSchemaMap).withReplaceSpecialChars(replaceSpecialChars))
                        .withCoder(TableRowJsonCoder.of()));
                // If reshuffle is enabled, it will break the fusion with write to db task and parallelize the reads
                // Write to DB will only run only after Read from BQ is complete
                // Which would mean that all the data needs to reside in dataflow worker RAM
                // .apply("Reshuffle to Break Fusion", Reshuffle.viaRandomKey());

                inputRows.apply("Write To DB", JdbcIO.<TableRow>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl)
                                .withUsername(user).withPassword(password))
                        .withStatement(dbManager.getInsertQuery(dbTable, schemaName, dbSchema))
                        .withBatchSize(batchSize)
                        .withPreparedStatementSetter(new SfToDbPreparedStatementSetter(dbSchema, dbType)));

            }
        }
        else {
            PCollection<TableRow> inputRows = p.apply("Read From Snowflake", SnowflakeIO.<TableRow>read()
                    .withDataSourceConfiguration(snowflakeConfig)
                    .fromQuery(query)
                    .withStagingBucketName(stagingBucket)
                    .withStorageIntegrationName(storageIntegration)
                    .withCsvMapper(CsvMapperToTableRow.builder().withSfSchema(sfSchemaMap).withReplaceSpecialChars(replaceSpecialChars))
                    .withCoder(TableRowJsonCoder.of()));

            // If reshuffle is enabled, it will break the fusion with write to db task and parallelize the reads
            // Write to DB will only run only after Read from BQ is complete
            // Which would mean that all the data needs to reside in dataflow worker RAM
            // .apply("Reshuffle to Break Fusion", Reshuffle.viaRandomKey());

            // JDBCIO Uses connection pool here
            inputRows.apply("Write To DB", JdbcIO.<TableRow>write()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl)
                            .withUsername(user).withPassword(password))
                    .withStatement(dbManager.getInsertQuery(dbTable, schemaName, dbSchema))
                    .withBatchSize(batchSize)
                    .withPreparedStatementSetter(new SfToDbPreparedStatementSetter(dbSchema, dbType)));
        }

        p.run();
    }
}
