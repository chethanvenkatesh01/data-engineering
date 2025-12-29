package com.impact;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.bigquery.BigQueryClient;
import com.impact.utils.bigquery.BigQuerySchemaConversion;
import com.impact.utils.date.DateHelper;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.sftp.SftpClient;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class SftpToBigQueryPipeline {
    public static void main(String[] args) throws Exception {
        SftpToBigQueryPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SftpToBigQueryPipelineOptions.class);
        run(pipelineOptions);
    }

    private static void run(SftpToBigQueryPipelineOptions pipelineOptions) throws Exception {
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
        String partitionColumn = pipelineOptions.getBqPartitionColumn();
        String clusteringColumns = pipelineOptions.getBqClusteringColumns();
        String folderDatePattern = pipelineOptions.getFolderDatePattern();
        String defaultFileEncoding = pipelineOptions.getFileEncoding();
        defaultFileEncoding = defaultFileEncoding!=null && defaultFileEncoding.equals("null") ? null : defaultFileEncoding;

        // BigQuery Options
        String bqProject = pipelineOptions.getBigqueryProject();
        String bqBillingProject = pipelineOptions.getBigqueryBillingProject();
        String bqDataset = pipelineOptions.getBigqueryDataset();
        String bqTable = pipelineOptions.getBigqueryTable();
        String bqRegion = pipelineOptions.getBigqueryRegion();
        if(bqRegion==null) {
            bqRegion = pipelineOptions.as(GcpOptions.class).getWorkerRegion();
        }

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
        
        bqBillingProject = bqBillingProject!=null ? bqBillingProject : project;
        BigQueryClient bqClient = new BigQueryClient(bqBillingProject, bqRegion);

        List<String> directoriesList = new ArrayList<>();

        // Create a PCollection of directories to read
        if(directories!=null && !directories.equalsIgnoreCase("null") && directories.length()>0) {
            // List of directories are provided as input
            log.info("Processing files from the list of directories provided");
            directoriesList = Arrays.asList(directories.split(";"));
        }
        else if(lastProcessedDate != null && !lastProcessedDate.equalsIgnoreCase("null") && lastProcessedDate.length()>0) {
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
                TableSchema tableSchema = new TableSchema();
                List<TableFieldSchema> fields = new LinkedList<>();
                for(String fieldName : fieldNames) {
                    if(fieldName.equals(auditColumn)) {
                        fields.add(new TableFieldSchema().setName(fieldName.replaceAll("[^A-Za-z0-9_]","_")).setType("DATETIME"));
                    }
                    else {
                        fields.add(new TableFieldSchema().setName(fieldName.replaceAll("[^A-Za-z0-9_]","_")).setType("STRING"));
                    }
                }
                tableSchema.setFields(fields);
                bqClient.createTable(bqProject, bqDataset, bqTable, tableSchema,partitionColumn,clusteringColumns, replaceTable);

            }
            else {
                log.info(String.format("Couldn't find any files which match the pattern %s", filePrefix+".*"+fileSuffix+"$"));
            }
        }
        else {
            throw new Exception(String.format("The directories list is empty"));
        }

        TableSchema bqTableSchema = bqClient.getSchemaAsTableSchema(bqProject, bqDataset, bqTable);
        LinkedHashMap<String, String> bqTableSchemaAsMap = new LinkedHashMap<>();
        for(TableFieldSchema field : bqTableSchema.getFields()) {
            bqTableSchemaAsMap.put(field.getName(), field.getType());
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
                ParDo.of(MapStringToTableRow.builder().withDelimiter(fieldDelimiter).withSchema(bqTableSchemaAsMap)
                        .withReplaceSpecialChars(replaceSpecialChars)
                        .withUseStandardCsvParser(pipelineOptions.getUseStandardCsvParser())));

        tableRows.apply("Write to BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", project, bqDataset, bqTable))
                .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();

    }

}
