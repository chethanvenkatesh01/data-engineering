package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.bigquery.BigQueryClient;
import com.impact.utils.bigquery.BigQuerySchemaConversion;
import com.impact.utils.db.DbManager;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.snowflake.SnowflakeClient;
import com.impact.utils.snowflake.SnowflakeSchema;
import com.impact.utils.snowflake.SnowflakeSchemaConversion;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class SfToDbCopyMultiPipeline {

    public static void main(String[] args) throws Exception {

        SfToDbCopyMultiPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SfToDbCopyMultiPipelineOptions.class);
        run(options);

    }

    public static void run(SfToDbCopyMultiPipelineOptions options) throws Exception {
        String project = options.as(GcpOptions.class).getProject();

        // SF params
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

        // DB params
        String host = options.getDbHost();
        String port = options.getDbPort();
        String user = options.getDbUser();
        String password = options.getDbPassword();
        String db = options.getDbName();
        String schemaName = options.getDbSchemaName();
        String dbType = options.getDbType();
        
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

        String sfTableToPgTable = options.getSfTableToPgTableNameMap();
        log.info(String.format("sfTableToPgTable : %s", sfTableToPgTable));
        Map<String, String> sfTableToPgTableMap = new LinkedHashMap<>();
        for(String tableMap : sfTableToPgTable.split(","))  {
            sfTableToPgTableMap.put(tableMap.split(":")[0], tableMap.split(":")[1]);
        }

        SnowflakeClient sfClient = SnowflakeClient.builder().withServer(snowflakeServer)
                .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                .withDatabase(snowflakeDatabase)
                .withDbSchema(snowflakeSchema)
                .withWarehouse(snowflakeWarehouse)
                .withUserRole(snowflakeUserRole);
        
        DbManager dbManager = new DbManager(host, port, user, password, db, dbType);
        
        log.info(String.format("Found %s tables", sfTableToPgTableMap.size()));

        String driverClassName = DbManager.getDbDriver(dbType);
        String connectionUrl = dbManager.getConnectionUrl();
        log.info(String.format("Driver: %s\nConnection Url: %s", driverClassName, connectionUrl));

        SnowflakeSchema sfSchema = null;
        Map<String, String> dbSchema = null;
        String tableName = null;
        String query = null;
        Map<String, String> sfSchemaMap = new LinkedHashMap<>();

        Pipeline p = Pipeline.create(options);

        for(String snowflakeTable : sfTableToPgTableMap.keySet()) {
            log.info(String.format("Adding table %s", snowflakeTable));
            query = String.format("SELECT * FROM %s.%s.%s", snowflakeDatabase, snowflakeSchema, snowflakeTable);
            sfSchema = sfClient.getQueryResultSchema(query);
            sfSchemaMap = sfClient.convertSchemaToMap(sfSchema);
            dbSchema = SnowflakeSchemaConversion.sfToDb(sfSchemaMap, dbType);
            dbManager.createTablev3(String.format("%s.%s", schemaName, sfTableToPgTableMap.get(snowflakeTable)), dbSchema, replaceTable, true);
            log.info(dbManager.getInsertQuery(sfTableToPgTableMap.get(snowflakeTable), schemaName, dbSchema));
            p.apply("Read From Snowflake", SnowflakeIO.<TableRow>read()
                    .withDataSourceConfiguration(snowflakeConfig)
                    .fromQuery(query)
                    .withStagingBucketName(stagingBucket)
                    .withStorageIntegrationName(storageIntegration)
                    .withCsvMapper(CsvMapperToTableRow.builder().withSfSchema(sfSchemaMap).withReplaceSpecialChars(replaceSpecialChars))
                    .withCoder(TableRowJsonCoder.of()))
                    .apply(String.format("Writing %s To DB", sfTableToPgTableMap.get(snowflakeTable)), JdbcIO.<TableRow>write()
                            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl)
                                    .withUsername(user).withPassword(password))
                            .withStatement(dbManager.getInsertQuery(sfTableToPgTableMap.get(snowflakeTable), schemaName, dbSchema))
                            .withPreparedStatementSetter(new SfToDbPreparedStatementSetter(dbSchema, dbType)));
        }
        
//        for(int i=0; i<tableNames.size(); i++) {
//            tableName = tableNames.get(i);
//            log.info(String.format("Adding table %s", tableName));
//            bqSchema = bqClient.getTableSchema(dataset, tableName);
//            dbSchema = BigQuerySchemaConversion.bqToDb(bqSchema, dbType);
//            dbManager.createTable(tableName, schema, dbSchema, false, true);
//            log.info(dbManager.getInsertQuery(tableName, schema, dbSchema));
//            p.apply(String.format("Reading %s From BigQuery", tableName),
//                    BigQueryIO.readTableRows().from(String.format("%s:%s.%s", project, dataset, tableName)))
//                    .apply(String.format("Writing %s To DB", tableName), JdbcIO.<TableRow>write()
//                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(driverClassName, connectionUrl)
//                            .withUsername(user).withPassword(password))
//                    .withStatement(dbManager.getInsertQuery(tableName, schema, dbSchema))
//                    .withPreparedStatementSetter(new BqToDbPreparedStatementSetter(dbSchema, dbType)));
//
//        }

        p.run();

    }
}
