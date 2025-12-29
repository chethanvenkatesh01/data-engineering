package com.impact;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.date.DateHelper;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.sftp.SftpClient;
import com.impact.utils.snowflake.SnowflakeSchema;
import com.impact.utils.snowflake.SnowflakeClient;
import com.impact.utils.snowflake.SnowflakeSchemaField;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.opencsv.CSVWriter;
import java.io.StringWriter;
import java.util.ArrayList;

@Slf4j
public class SftpToSnowflakePipeline {
    public static void main(String[] args) throws Exception {
        SftpToSnowflakePipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SftpToSnowflakePipelineOptions.class);
        run(pipelineOptions);
    }

    public static SnowflakeIO.UserDataMapper<TableRow> getCsvMapper(SnowflakeSchema schema) {
        return (SnowflakeIO.UserDataMapper<TableRow> ) recordLine -> {
            ArrayList<String> stringList = new ArrayList<>();

            // Iterate over the keys in the TableRow
            List<SnowflakeSchemaField> fields = schema.getFields();
            String valueStr =null;
            for (SnowflakeSchemaField key : fields) {
                String fieldName = key.getName();
                if (recordLine.containsKey(fieldName)){
                    valueStr = recordLine.get(fieldName) != null ? recordLine.get(fieldName).toString() : null;
                }
                else {
                    valueStr = null;
                }
                stringList.add(valueStr);

            }
            return stringList.toArray(new String[0]);
        };
    }



    private static void run(SftpToSnowflakePipelineOptions pipelineOptions) throws Exception {
        // SFTP options
        String host = pipelineOptions.getSftpHost();
        String userName = pipelineOptions.getSftpUsername();
        String password = pipelineOptions.getSftpPassword();
        String rootDirectory = pipelineOptions.getRootDirectory();
        String fileName = pipelineOptions.getFileName();
        String absoluteFilePath = pipelineOptions.getAbsoluteFilePath();
        String filePrefix = pipelineOptions.getFilePrefix();
        String fileSuffix = pipelineOptions.getFileSuffix();
        String directories = pipelineOptions.getDirectories();
        String lastProcessedDate = pipelineOptions.getLastProcessedDate();
        String pullType = pipelineOptions.getPullType();
        boolean header = pipelineOptions.getHeader();
        String fieldDelimiter = pipelineOptions.getFieldDelimiter();
        String auditColumn = pipelineOptions.getAuditColumn();
        boolean replaceTable = pipelineOptions.getReplaceTable();
        boolean replaceSpecialChars = pipelineOptions.getReplaceSpecialChars();
        String folderDatePattern = pipelineOptions.getFolderDatePattern();
        String defaultFileEncoding = pipelineOptions.getFileEncoding();
        defaultFileEncoding = defaultFileEncoding!=null && defaultFileEncoding.equals("null") ? null : defaultFileEncoding;

        // Snowflake Options
        String snowflakeTable = pipelineOptions.getSnowflakeTable();
        String snowflakeClusteringColumns = pipelineOptions.getSnowflakeClusteringColumns();
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


        // GCP options
        String project = pipelineOptions.as(GcpOptions.class).getProject();
        String secretName = pipelineOptions.getSftpSecretName();

        if(secretName!=null) {
            log.info("Fetching the credentials from secert manager");
            log.info(String.format("Secret name %s", secretName));
            String secret = SecretManager.getSecret(project, secretName, "latest");
            JsonObject config = new Gson().fromJson(secret, JsonObject.class);
            for(String key : config.keySet()) {
                if(key.contains("host") || key.contains("server")) host = config.get(key).getAsString();
                    //else if(key.contains("port")) port = config.get(key).getAsString();
                else if(key.contains("user")) userName = config.get(key).getAsString();
                else if(key.contains("password")) password = config.get(key).getAsString();
                else if(key.contains("path")) rootDirectory = config.get(key).getAsString();
            }
            log.info(String.format("Host: %s;rootDirectory: %s", host, rootDirectory));
        }

        if(snowflakeSecretName!=null) {
            log.info("Fetching the snowflake credentials from secert manager");
            String secret = SecretManager.getSecret(project, snowflakeSecretName, "latest");
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

//        BigQueryClient bqClient = new BigQueryClient(bqProject, bqRegion);
        SnowflakeSchema tableSchema = null;
        SnowflakeClient sfClient = SnowflakeClient.builder().withServer(snowflakeServer)
                .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                .withDatabase(snowflakeDatabase)
                .withDbSchema(snowflakeSchema)
                .withWarehouse(snowflakeWarehouse)
                .withUserRole(snowflakeUserRole);

        List<String> directoriesList = new ArrayList<>();

        // Create a PCollection of directories to read
        if(directories!=null && directories.length()>0) {
            // List of directories are provided as input
            log.info("Processing files from the list of directories provided");
            directoriesList = Arrays.asList(directories.split(";"));
        }
        else if(lastProcessedDate != null && lastProcessedDate.length()>0) {
            // Generate directory names with date > lastProcessedDate
            log.info("Processing files from the directories (named with date) greater than lastProcessedDate provided");
            assert rootDirectory!=null : "rootDirectory is null. When using lastProcessedDate, the rootDirectory cannot be null";
            List<LocalDate> dateRange = DateHelper.generateDateRange(lastProcessedDate, null, false, true);
            String finalRootDirectory = rootDirectory;
            directoriesList = dateRange.stream().map(localDate -> finalRootDirectory.replaceAll("/$", "")
                    .concat("/"+localDate.format(DateTimeFormatter.ofPattern(folderDatePattern)))).collect(Collectors.toList());
        }
        else if(rootDirectory != null) {
            log.info("Processing files from the root directory provided");
            directoriesList = new ArrayList<>();
            directoriesList.add(rootDirectory.replaceAll("/$", ""));
        }
        else {
            log.error("Either one of the [directories, lastProcessedDate, rootDirectory] options should be provided");
        }

        // ------------ RUN A DDL IN BIGQUERY AFTER FETCHING THE SCHEMA FROM SAMPLE FILE -------------------

        if(directoriesList.size()>0) {
            String sampleDirectory = directoriesList.get(0);
            log.info(String.format("Sample Directory: %s", sampleDirectory));
            SftpClient sftpClient = new SftpClient(host, userName, password);
            List<String> files = sftpClient.getFilePaths(sampleDirectory, filePrefix+".*"+fileSuffix+"$", 1);
            if(files.size()>0) {
                String file = files.get(0);
                log.info(String.format("Sample file: %s", file));
                String fileHeader = sftpClient.getFileHeader(file, defaultFileEncoding);
                log.info(String.format("Header: %s", fileHeader));
                if(fileHeader==null) {
                    throw new Exception(String.format("The file %s might be empty", file));
                }
//                if(auditColumn!=null) {
//                    fileHeader = fileHeader.concat(auditColumn);
//                }
                //List<String> fieldNames = Arrays.asList(fileHeader.split(fieldDelimiter));
                log.info("Delimiter " + fieldDelimiter);
                List<String> fieldNames = Arrays.stream(fileHeader.split(Pattern.quote(fieldDelimiter))).collect(Collectors.toList());
                if(auditColumn!=null) {
                    fieldNames.add(auditColumn);
                }
                List<SnowflakeSchemaField> fields = new LinkedList<>();
                for(String fieldName : fieldNames) {
                    if(fieldName.equals(auditColumn)) {
                        fields.add(new SnowflakeSchemaField(fieldName.replaceAll("[^A-Za-z0-9_]","_"),"DATETIME"));
                    }
                    else {
                        fields.add(new SnowflakeSchemaField(fieldName.replaceAll("[^A-Za-z0-9_]","_"),"VARCHAR"));
                    }
                }
                tableSchema = new SnowflakeSchema(fields);
                sfClient.createTable(snowflakeDatabase, snowflakeSchema ,snowflakeTable, tableSchema, snowflakeClusteringColumns, replaceTable);

            }
            else {
                log.info(String.format("Couldn't find any files which match the pattern %s", filePrefix+".*"+fileSuffix+"$"));
            }
        }
        else {
            throw new Exception(String.format("The directories list is empty"));
        }

        SnowflakeIO.DataSourceConfiguration snowflakeConfig =
                SnowflakeIO.DataSourceConfiguration.create()
                        .withUsernamePasswordAuth(snowflakeUserName, snowflakePassword)
                        .withServerName(snowflakeServer)
                        .withWarehouse(snowflakeWarehouse)
                        .withDatabase(snowflakeDatabase)
                        .withSchema(snowflakeSchema)
                        .withRole(snowflakeUserRole);

        SnowflakeSchema sfTableSchema = sfClient.getQueryResultSchema(String.format("SELECT * FROM %s.%s.%s", snowflakeDatabase, snowflakeSchema, snowflakeTable));
        LinkedHashMap<String, String> sfTableSchemaAsMap = new LinkedHashMap<>();
        for(SnowflakeSchemaField field : sfTableSchema.getFields()) {
            sfTableSchemaAsMap.put(field.getName(), field.getType());
        }

        if(pullType.toLowerCase().equals("full")) {
            Collections.reverse(directoriesList);
            // If pull type is full, it'll only consider the recent directory
            directoriesList = Collections.singletonList(directoriesList.get(0));
        }


        Pipeline p = Pipeline.create(pipelineOptions);

        PCollection<String> directoriesPCol = null;

        directoriesPCol = p.apply("Get directories", GetDirectoriesTransform.builder().withDirectories(directoriesList));

        PCollection<KV<String, String>> filePaths = directoriesPCol.apply("Get Files", GetFilesTransform.builder().withSftpCredentials(host, userName, password));

        PCollection<String> lines = filePaths.apply("Read Files",  ReadFilesTransform.builder().withSftpCredentials(host, userName, password)
                .withHeader(header).withDelimiter(fieldDelimiter).withFileEncoding(defaultFileEncoding));

        PCollection<TableRow> tableRows = lines.apply("Convert lines to TableRow",
                ParDo.of(MapStringToTableRow.builder().withDelimiter(fieldDelimiter).withSchema(sfTableSchemaAsMap)
                        .withReplaceSpecialChars(replaceSpecialChars)
                        .withUseStandardCsvParser(pipelineOptions.getUseStandardCsvParser())));

        tableRows.apply("Write to Snowflake", SnowflakeIO.<TableRow>write()
                .withDataSourceConfiguration(snowflakeConfig)
                .to(snowflakeTable)
                .withTableSchema(sfTableSchema)
                .withUserDataMapper(getCsvMapper(tableSchema))
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withStagingBucketName(stagingBucket)
                .withStorageIntegrationName(storageIntegration));

        p.run();

    }

}
