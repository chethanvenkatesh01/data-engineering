package com.impact;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.bigquery.BigQueryClient;
import com.impact.utils.bigquery.BigQuerySchemaConversion;
import com.impact.utils.fs.ParquetIOHandler;
import com.impact.utils.gcp.GcsClient;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.io.ParquetIO;
import com.impact.utils.snowflake.SnowflakeClient;
import com.impact.utils.snowflake.SnowflakeSchema;
import com.impact.utils.snowflake.SnowflakeSchemaField;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

@Slf4j
public class SnowflakeToBigQueryPipeline {

    //private final static Gauge mailRecipientsMetric = Metrics.gauge( "SnowflakeToBigQueryPipeline", "mailRecipients");
    public static void main(String[] args) throws Exception {
        SnowflakeToBigQueryPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SnowflakeToBigQueryPipelineOptions.class);
        run(pipelineOptions);
    }

    private static void run(SnowflakeToBigQueryPipelineOptions pipelineOptions) throws Exception {

        String projectId = pipelineOptions.as(GcpOptions.class).getProject();

        String snowflakeTable = pipelineOptions.getSnowflakeTable();
        //String snowflakeQuery = pipelineOptions.getSnowflakeQuery();
        String snowflakeQuery = pipelineOptions.getSourceQueries();
        String pullType = pipelineOptions.getPullType();
        String incrementalColumn = pipelineOptions.getIncrementalColumn();
        String incrementalColumnValue = pipelineOptions.getIncrementalColumnValue();
        String auditColumnName = pipelineOptions.getAuditColumn();
        String storageIntegration = pipelineOptions.getStorageIntegration();
        String stagingBucket = pipelineOptions.getStagingBucket();
        boolean replaceSpecialChars = pipelineOptions.getReplaceSpecialChars();
        String snowflakeSecretName = pipelineOptions.getSnowflakeSecretName();
        String snowflakeServer = pipelineOptions.getSnowflakeServerName();
        String snowflakeUserName = pipelineOptions.getSnowflakeUserName();
        String snowflakePassword = pipelineOptions.getSnowflakePassword();
        String snowflakeDatabase = pipelineOptions.getSnowflakeDatabase();
        String snowflakeSchema = pipelineOptions.getSnowflakeSchema();
        String snowflakeWarehouse = pipelineOptions.getSnowflakeWarehouse();
        String snowflakeUserRole = pipelineOptions.getSnowflakeUserRole();

        String bigqueryProject = pipelineOptions.getBigqueryProject();
        String bqBillingProject = pipelineOptions.getBigqueryBillingProject();
        String bigqueryDataset = pipelineOptions.getBigqueryDataset();
        String bigqueryTable = pipelineOptions.getBigqueryTable();
        boolean replaceTable = pipelineOptions.getReplaceTable();
        String bqPartitionColumn = pipelineOptions.getBqPartitionColumn();
        String bqClusteringColumns = pipelineOptions.getBqClusteringColumns();
        String bqRegion = pipelineOptions.getBigqueryRegion();
        if(bqRegion==null) {
            bqRegion = pipelineOptions.as(GcpOptions.class).getWorkerRegion();
        }

        String mailRecipients = pipelineOptions.getMailRecipients();

        bqBillingProject = bqBillingProject!=null ? bqBillingProject : bigqueryProject;
        BigQueryClient bqClient = new BigQueryClient(bqBillingProject, bqRegion);
        //TableSchema bqTableSchema = null;
        SnowflakeSchema sfSchema = null;
        //Map<String, String> bqSchema = new LinkedHashMap<String, String>();
        String query = null;
        String finalQuery = null;

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

        if(snowflakeQuery!=null) {
            log.info(String.format("Source Query: %s", snowflakeQuery));
            query = snowflakeQuery;
            SnowflakeClient sfClient = SnowflakeClient.builder().withServer(snowflakeServer)
                    .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                    .withDatabase(snowflakeDatabase)
                    .withDbSchema(snowflakeSchema)
                    .withWarehouse(snowflakeWarehouse)
                    .withUserRole(snowflakeUserRole);

            sfSchema = sfClient.getQueryResultSchema(query);
//            bqTableSchema = SnowflakeToBigQuerySchemaConverter.convert(sfSchema);
//            bqClient.createTable(bigqueryProject, bigqueryDataset, bigqueryTable, bqTableSchema, bqPartitionColumn,
//                    bqClusteringColumns, replaceTable);
//            for(TableFieldSchema field : bqTableSchema.getFields()) {
//                bqSchema.put(field.getName(), field.getType());
//            }
            List<String> columnProjections = new LinkedList<String>();
            for(SnowflakeSchemaField field : sfSchema.getFields()) {
                if(field.getType().equals("VARCHAR") || field.getType().equals("CHAR") || field.getType().equals("CHARACTER")
                || field.getType().equals("STRING") || field.getType().equals("TEXT")) {
                    columnProjections.add(String.format("COALESCE(%s, 'IA_NULL') AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("NUMERIC") || field.getType().equals("DECIMAL") || field.getType().equals("NUMBER")) {
                    columnProjections.add(String.format("CAST(%s AS NUMERIC) AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("INT") || field.getType().equals("INTEGER") || field.getType().equals("BIGINT")
                        || field.getType().equals("SMALLINT") || field.getType().equals("TINYINT") || field.getType().equals("BYTEINT")) {
                    columnProjections.add(String.format("CAST(%s AS BIGINT) AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("FLOAT") || field.getType().equals("FLOAT4") || field.getType().equals("FLOAT8")
                        || field.getType().equals("DOUBLE") || field.getType().equals("DOUBLE PRECISION") || field.getType().equals("REAL")) {
                    columnProjections.add(String.format("CAST(%s AS DOUBLE PRECISION) AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("TIMESTAMPLTZ") || field.getType().equals("TIMESTAMPTZ") ||
                        field.getType().equals("TIMESTAMP_LTZ") || field.getType().equals("TIMESTAMP_TZ")) {
                    columnProjections.add(String.format("CAST(%s AS TIMESTAMP_NTZ) AS %s", field.getName(), field.getName()));
                }
                else {
                    columnProjections.add(field.getName());
                }
            }
            finalQuery = String.format("SELECT %s FROM (%s)", StringUtils.join(columnProjections, ",\n"), query);
            log.info(String.format("Final Query: %s", finalQuery));

        }
        else {
            assert snowflakeTable!=null : "snowflakeTable is null. Either snowflakeQuery or snowflakeTable should be passed.";
            SnowflakeClient sfClient = SnowflakeClient.builder().withServer(snowflakeServer)
                    .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                    .withDatabase(snowflakeDatabase)
                    .withDbSchema(snowflakeSchema)
                    .withWarehouse(snowflakeWarehouse)
                    .withUserRole(snowflakeUserRole);
            query = String.format("SELECT * FROM %s.%s", snowflakeSchema, snowflakeTable);
            sfSchema = sfClient.getQueryResultSchema(query);
            List<String> fieldNames = sfSchema.getFields().stream().map(x -> x.getName().toUpperCase()).collect(Collectors.toList());
            if(auditColumnName!=null && !fieldNames.contains(auditColumnName.toUpperCase())) {
                query = String.format("SELECT *, SYSDATE() AS %s  FROM %s.%s", auditColumnName, snowflakeSchema, snowflakeTable);
            }
            else {
                query = String.format("SELECT * FROM %s.%s", snowflakeSchema, snowflakeTable);
            }
            if(pullType.equalsIgnoreCase("incremental")) {
                assert incrementalColumn!=null && incrementalColumnValue!=null : "When pullType is incremental, both" +
                        "incrementalColumn and incrementalColumnValue should be passed.";
                query = query + String.format(" WHERE %s > '%s'", incrementalColumn, incrementalColumnValue);
            }
            log.info(String.format("Source Query: %s", query));

            sfSchema = sfClient.getQueryResultSchema(query);
            log.info(String.valueOf(sfSchema.getFields().size()));
//            bqTableSchema = SnowflakeToBigQuerySchemaConverter.convert(sfSchema);
//            log.info(String.valueOf(bqTableSchema.getFields().size()));
//            bqClient.createTable(bigqueryProject, bigqueryDataset, bigqueryTable, bqTableSchema, bqPartitionColumn,
//                    bqClusteringColumns, replaceTable);
//            for(TableFieldSchema field : bqTableSchema.getFields()) {
//                bqSchema.put(field.getName(), field.getType());
//            }
            List<String> columnProjections = new LinkedList<String>();
            for(SnowflakeSchemaField field : sfSchema.getFields()) {
                log.info(String.format("Name: %s Type: %s", field.getName(), field.getType()));
                if(field.getType().equals("VARCHAR") || field.getType().equals("CHAR") || field.getType().equals("CHARACTER")
                        || field.getType().equals("STRING") || field.getType().equals("TEXT")) {
                    columnProjections.add(String.format("COALESCE(%s, 'IA_NULL') AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("NUMERIC") || field.getType().equals("DECIMAL") || field.getType().equals("NUMBER")) {
                    columnProjections.add(String.format("CAST(%s AS NUMERIC) AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("INT") || field.getType().equals("INTEGER") || field.getType().equals("BIGINT")
                        || field.getType().equals("SMALLINT") || field.getType().equals("TINYINT") || field.getType().equals("BYTEINT")) {
                    columnProjections.add(String.format("CAST(%s AS BIGINT) AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("FLOAT") || field.getType().equals("FLOAT4") || field.getType().equals("FLOAT8")
                        || field.getType().equals("DOUBLE") || field.getType().equals("DOUBLE PRECISION") || field.getType().equals("REAL")) {
                    columnProjections.add(String.format("CAST(%s AS DOUBLE PRECISION) AS %s", field.getName(), field.getName()));
                }
                else if(field.getType().equals("TIMESTAMPLTZ") || field.getType().equals("TIMESTAMPTZ") ||
                        field.getType().equals("TIMESTAMP_LTZ") || field.getType().equals("TIMESTAMP_TZ")) {
                    columnProjections.add(String.format("CAST(%s AS TIMESTAMP_NTZ) AS %s", field.getName(), field.getName()));
                }
                else {
                    columnProjections.add(field.getName());
                }
            }
            finalQuery = String.format("SELECT %s FROM (%s)", StringUtils.join(columnProjections, ",\n"), query);
            log.info(String.format("Final Query: %s", finalQuery));
        }

        SnowflakeIO.DataSourceConfiguration snowflakeConfig =
                SnowflakeIO.DataSourceConfiguration.create()
                        .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                        .withServerName(snowflakeServer)
                        .withWarehouse(snowflakeWarehouse)
                        .withDatabase(snowflakeDatabase)
                        .withSchema(snowflakeSchema)
                        .withRole(snowflakeUserRole);

        SerializableFunction<Void, DataSource> dataSourceProviderFn = SnowflakeIO.DataSourceProviderFromDataSourceConfiguration.of(snowflakeConfig);
        String tmpDirName = String.format(
                "sf_copy_csv_%s_%s",
                new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()),
                UUID.randomUUID().toString().subSequence(0, 8) // first 8 chars of UUID should be enough
        );
        String stagingBucketRunDir = String.format(
                "%s/%s/run_%s/",
                StringUtils.stripEnd(stagingBucket,"/"), tmpDirName, UUID.randomUUID().toString().subSequence(0, 8));
        String stagingBucketRunDirWithSnowflakeGcsPrefix = stagingBucketRunDir.replace("gs://", "gcs://");
        log.info(String.format("Staging bucket directory %s,  Staging bucket directory with snowflake gcs prefix",
                stagingBucketRunDir, stagingBucketRunDirWithSnowflakeGcsPrefix));
        String copyQuery =
                String.format(
                        "COPY INTO '%s' FROM (%s) STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=PARQUET COMPRESSION=SNAPPY ) HEADER=true;",
                        stagingBucketRunDirWithSnowflakeGcsPrefix,
                        finalQuery,
                        storageIntegration);
        log.info(String.format("Executing data unloading query %s", copyQuery));
        DataSource dataSource = dataSourceProviderFn.apply(null);
        try(Connection connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(copyQuery)){
            statement.execute();
            log.info("Data is unloaded successfully");
        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw e;
        }
        GcsClient gcsClient = new GcsClient(projectId);
        String sampleFilePattern = String.format("%s*.parquet", stagingBucketRunDir);
        List<String> sampleBlobs = gcsClient.listBlobsWithPattern(sampleFilePattern);
        if(sampleBlobs.size()<=0) {
            throw new Exception(String.format("Unable to find blobs with file pattern to sample for schema inference %s",
                    sampleFilePattern));
        }
        Map<String, String> bqSchema = createBigQuerySchemaFromGcsFile(sampleBlobs.get(0), null,
                "DATETIME", null, null);
        TableSchema bqTableSchema = BigQuerySchemaConversion.convertMapToTableSchema(bqSchema);
        log.info("Running DDL on BigQuery");
        bqClient.createTable(bigqueryProject, bigqueryDataset, bigqueryTable, bqTableSchema, bqPartitionColumn, bqClusteringColumns, replaceTable);
        MessageType parquetSchema = ParquetIOHandler.getParquetSchema(sampleBlobs.get(0));
        Configuration config = new Configuration();
        config.setBoolean(READ_INT96_AS_FIXED, true);
        Schema originalSchema = new AvroSchemaConverter(config).convert(parquetSchema);
        Schema schemaWithAuditColumn = null;
        schemaWithAuditColumn = originalSchema;
        List<String> filePatterns = new ArrayList<>();
        filePatterns.add(String.format("%s*.parquet", stagingBucketRunDir));

        Pipeline pipeline = Pipeline.create(pipelineOptions);

//        PCollection<TableRow> rows = pipeline.apply("Read From Snowflake", SnowflakeIO.<TableRow>read()
//                .withDataSourceConfiguration(snowflakeConfig)
//                .fromQuery(finalQuery)
//                .withStagingBucketName(stagingBucket)
//                .withStorageIntegrationName(storageIntegration)
//                .withCsvMapper(CsvMapperToTableRow.builder().withBqSchema(bqSchema).withReplaceSpecialChars(replaceSpecialChars))
//                .withCoder(TableRowJsonCoder.of()));
//
//        rows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
//                .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        PCollection<String> directoriesPCol = pipeline.apply("Get Directories", Create.of(filePatterns))
                .apply("Reshuffle Directories", Reshuffle.viaRandomKey());
        PCollection<FileIO.ReadableFile> files =directoriesPCol.apply("Find Files", FileIO.matchAll()).apply("Get Readable Files", FileIO.readMatches())
                .apply("Reshuffle Files", Reshuffle.viaRandomKey());
        PCollection<GenericRecord> genericRecords = files.apply(String.format("Read Parquet Files with pattern"), ParquetIO.readFiles(originalSchema)
                .withConfiguration(Map.of("parquet.avro.readInt96AsFixed", "true"))
                .withAuditColumnName(null)
                .withUpdatedSchema(schemaWithAuditColumn));

        PCollection<TableRow> rows = genericRecords.apply("Map GenericRecord To TableRow", ParDo.of(MapGenericRecordToTableRow.builder()
                .withSchema(bqSchema)
                .withReplaceSpecialCharacters(replaceSpecialChars)));

        PCollection<TableDestination> successfulTableLoads = rows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
                .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)).getSuccessfulTableLoads();

        directoriesPCol.apply("Waiting for BigQuery Write Success", Wait.on(successfulTableLoads)).apply("Clean Temp Files from Staging",
                ParDo.of(new CleanTmpFilesFromGcsFn(StringUtils.stripEnd(stagingBucket,"/"), tmpDirName)));

        pipeline.run();


    }

    public static Map<String, String> createBigQuerySchemaFromGcsFile(String filePath, String auditColumnName,
                                                                      String auditColumnDataType, String fieldDelimiter,
                                                                      String fileEncoding) throws Exception {
        Map<String, String> bqSchema = new LinkedHashMap<>();
        if(Pattern.matches(".*.parquet$", Paths.get(filePath).getFileName().toString())) {
            log.info("Infering schema for parquet file");
            MessageType parquetSchema = ParquetIOHandler.getParquetSchema(filePath);
            Configuration config = new Configuration();
            config.setBoolean(READ_INT96_AS_FIXED, true);
            Schema originalSchema = new AvroSchemaConverter(config).convert(parquetSchema);
            Schema schemaWithAuditColumn = null;
            if(auditColumnName!=null) {
//                List<Schema.Field> fields = new ArrayList<Schema.Field>();
//                for(Schema.Field field : originalSchema.getFields()){
//                    fields.add(new Schema.Field(field.name(), field.schema(), null, null));
//                }
//                fields.add(new Schema.Field(auditColumnName, Schema.create(Schema.Type.STRING), null, null));
//                schemaWithAuditColumn = Schema.createRecord(
//                        "SchemaWithAuditColumn",
//                        "docstring",
//                        "namespace1",
//                        false // Not a union
//                );
//                //LOG.info(String.format("Created a new Schema"));
//                schemaWithAuditColumn.setFields(fields);
//                bqSchema = BigQuerySchemaConversion.avroSchemaToBqSchema(schemaWithAuditColumn);
                bqSchema = BigQuerySchemaConversion.avroSchemaToBqSchema(originalSchema);
                bqSchema.put(auditColumnName, auditColumnDataType);
            }
            else {
                bqSchema = BigQuerySchemaConversion.avroSchemaToBqSchema(originalSchema);
            }
        }
        else if(Pattern.matches(".*.(csv|txt|dat|gz|zip)$", Paths.get(filePath).getFileName().toString())) {
            log.info("Infering scheam from flat files (csv|txt|dat|gz|zip)");
            GcsClient gcsClient = new GcsClient();
            String header = gcsClient.getFileHeader(filePath, fileEncoding);
            log.info(String.format("Field Delimiter: %s", fieldDelimiter));
            header = com.impact.utils.text.StringUtils.truncateQuotesEnclosingFields(header, fieldDelimiter);
            List<String> fields = Arrays.stream(header.split(Pattern.quote(fieldDelimiter))).collect(Collectors.toList());
            for(String field : fields) {
                bqSchema.put(field.replaceAll("[^A-Za-z0-9_]",""), "STRING");
            }
            if(auditColumnName!=null) bqSchema.put(auditColumnName, auditColumnDataType);
        }
        return bqSchema;
    }
}
