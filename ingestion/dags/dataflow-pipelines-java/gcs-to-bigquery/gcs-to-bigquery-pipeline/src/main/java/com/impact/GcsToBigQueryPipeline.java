package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.impact.utils.bigquery.BigQueryClient;
import com.impact.utils.bigquery.BigQuerySchemaConversion;
import com.impact.utils.date.DateHelper;
import com.impact.utils.fs.ParquetIOHandler;
import com.impact.utils.gcp.GcsClient;
import com.impact.utils.io.TextIO;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.io.parquet.ParquetIO;
import com.impact.utils.io.ParquetIO;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.avro.data.Json.SCHEMA;
import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

@Slf4j
public class GcsToBigQueryPipeline {

    public static void main(String[] args) throws Exception {
        GcsToBigQueryPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(GcsToBigQueryPipelineOptions.class);

        run(pipelineOptions);

    }

    private static void run(GcsToBigQueryPipelineOptions pipelineOptions) throws Exception {
        // BigQuery params
        String bigqueryProject = pipelineOptions.getBigqueryProject();
        String bqBillingProject = pipelineOptions.getBigqueryBillingProject();
        String bigqueryDataset = pipelineOptions.getBigqueryDataset();
        String bigqueryTable = pipelineOptions.getBigqueryTable();
        String partitionColumn = pipelineOptions.getBqPartitionColumn();
        String clusteringColumns = pipelineOptions.getBqClusteringColumns();
        String bqRegion = pipelineOptions.getBigqueryRegion();
        if(bqRegion==null) {
            bqRegion = pipelineOptions.as(GcpOptions.class).getWorkerRegion();
        }

        // GCS Params
        String gcsBucketName = pipelineOptions.getGcsBucketName();
        String blobPathPrefix = pipelineOptions.getGcsBlobPathPrefix();
        String blobNamePrefix = pipelineOptions.getBlobNamePrefix();
        String blobNameSuffix = pipelineOptions.getBlobNameSuffix();
        String pullType = pipelineOptions.getPullType();

        String directories = pipelineOptions.getDirectories();
        String fieldDelimiter = pipelineOptions.getFieldDelimiter();
        String lastProcessedDate = pipelineOptions.getLastProcessedDate();
        String folderDatePattern = pipelineOptions.getFolderDatePattern();

        String projectId = pipelineOptions.as(GcpOptions.class).getProject();
        bigqueryProject = bigqueryProject!=null ? bigqueryProject : projectId;
        boolean replaceTable = pipelineOptions.getReplaceTable();
        String auditColumnName = pipelineOptions.getAuditColumn();
        //String auditColumnDataType = pipelineOptions.getAuditColumnDataType();
        boolean replaceSpecialChars = pipelineOptions.getReplaceSpecialChars();
        String defaultFileEncoding = pipelineOptions.getFileEncoding();
        defaultFileEncoding = defaultFileEncoding!=null && defaultFileEncoding.equals("null") ? null : defaultFileEncoding;

        bqBillingProject = bqBillingProject!=null ? bqBillingProject : bigqueryProject;
        BigQueryClient bqClient = new BigQueryClient(bqBillingProject, bqRegion);
        GcsClient gcsClient = new GcsClient(projectId);
        //Map<String, String> bqSchema = null;
        //TableSchema bqTableSchema = null;
        List<String> filePatterns = new ArrayList<String>();

        //String gcsBucketName = null;
        String blobPrefix = null;

        if(directories!=null && !directories.equalsIgnoreCase("null") && directories.length()>0) {
            filePatterns = Arrays.asList(directories.split(";"));
            gcsBucketName = filePatterns.get(0).replace("gs://","").split("/")[0];
            blobPrefix = filePatterns.get(0).replace("gs://"+gcsBucketName+"/","");
            log.info(String.format("Bucket: %s, Blob prefix: %s", gcsBucketName, blobPrefix));
        }
        else if(lastProcessedDate != null && !lastProcessedDate.equalsIgnoreCase("null") && lastProcessedDate.length()>0) {
            if(gcsBucketName==null) throw new IllegalArgumentException("gcsBucket cannot be null");
            List<LocalDate> dateRange = DateHelper.generateDateRange(lastProcessedDate, null, false, true);
            for(LocalDate localDate : dateRange) {
                String filePattern = String.format("gs://%s/", gcsBucketName);
                if(blobPathPrefix!=null && blobPathPrefix.strip().length()>0) filePattern = filePattern + StringUtils.stripStart(blobPathPrefix,"/") + "/";
                filePattern = filePattern + localDate.format(DateTimeFormatter.ofPattern(folderDatePattern)) + "/";
                filePatterns.add(filePattern);
            }
            gcsBucketName = filePatterns.get(0).replace("gs://","").split("/")[0];
            blobPrefix = filePatterns.get(0).replace("gs://"+gcsBucketName+"/","");
            log.info(String.format("Bucket: %s, Blob prefix: %s", gcsBucketName, blobPrefix));
        }
        else if(gcsBucketName!=null) {
            log.info("Both directories and lastProcessedDate arguments are not provided. Hence trying to read files" +
                    "from bucket root");
            assert gcsBucketName!=null : "gcsBucket is null";
            String filePattern = String.format("gs://%s/", gcsBucketName);
            if(blobPathPrefix!=null && blobPathPrefix.strip().length()>0) filePattern = filePattern + StringUtils.stripStart(blobPathPrefix, "/") + "/";
            filePatterns.add(filePattern);
            gcsBucketName = filePatterns.get(0).replace("gs://","").split("/")[0];
            blobPrefix = filePatterns.get(0).replace("gs://"+gcsBucketName+"/","");
            log.info(String.format("Bucket: %s, Blob prefix: %s", gcsBucketName, blobPrefix));
        }
        else {
            log.info("Pipeline cannot proceed as all directories, lastProcessedDate and gcsBucketName arguments are null");
            return;
        }

        if(filePatterns.size()<=0) {
            log.warn("filePatterns is empty. Hence the pipeline will not start");
            return;
        }

        // Fetch the file schema and create the BQ table if required
        if(pullType.equalsIgnoreCase("full")) {
            Collections.sort(filePatterns, Collections.reverseOrder());
            filePatterns = filePatterns.subList(0,1);
        }
        String sampleFilePattern = filePatterns.get(0);
        if(!sampleFilePattern.endsWith("/")) sampleFilePattern = sampleFilePattern.concat("/");
        if(blobNamePrefix!=null) {
            sampleFilePattern = sampleFilePattern + blobNamePrefix + ".*";
        }
        else {
            sampleFilePattern = sampleFilePattern + "*";
        }
        sampleFilePattern = sampleFilePattern + "." + blobNameSuffix;
        log.info(String.format("Using the file pattern %s to extract sample blobs for schema inference", sampleFilePattern));
        List<String> sampleBlobs = gcsClient.listBlobsWithPattern(sampleFilePattern);
        if(sampleBlobs.size()<=0) {
            log.warn("Unable to find blobs to sample for schema inference");
            return;
        }

        //Map<String, String> bqSchema = createBigQuerySchemaFromGcsFile(sampleBlobs.get(0), auditColumnName, auditColumnDataType, fieldDelimiter);
        Map<String, String> bqSchema = createBigQuerySchemaFromGcsFile(sampleBlobs.get(0), auditColumnName,
                "DATETIME", fieldDelimiter, defaultFileEncoding);
        TableSchema bqTableSchema = BigQuerySchemaConversion.convertMapToTableSchema(bqSchema);
        log.info("Running DDL on BigQuery");
        bqClient.createTable(bigqueryProject, bigqueryDataset, bigqueryTable, bqTableSchema, partitionColumn, clusteringColumns, replaceTable);

        if(Pattern.matches(".*.parquet$", Paths.get(sampleBlobs.get(0)).getFileName().toString())) {
            log.info("Pipeline interpreted to process parquet files based on provided arguments");
            Pipeline pipeline = Pipeline.create(pipelineOptions);
            MessageType parquetSchema = ParquetIOHandler.getParquetSchema(sampleBlobs.get(0));
            Configuration config = new Configuration();
            config.setBoolean(READ_INT96_AS_FIXED, true);
            Schema originalSchema = new AvroSchemaConverter(config).convert(parquetSchema);
            Schema schemaWithAuditColumn = null;
            if(auditColumnName!=null) {
                List<Schema.Field> fields = new ArrayList<Schema.Field>();
                for(Schema.Field field : originalSchema.getFields()){
                    fields.add(new Schema.Field(field.name(), field.schema(), null, null));
                }
                fields.add(new Schema.Field(auditColumnName, Schema.create(Schema.Type.STRING), null, null));
                schemaWithAuditColumn = Schema.createRecord(
                        "SchemaWithAuditColumn",
                        "docstring",
                        "namespace1",
                        false // Not a union
                );
                //LOG.info(String.format("Created a new Schema"));
                schemaWithAuditColumn.setFields(fields);
            }
            else {
                schemaWithAuditColumn = originalSchema;
            }
            int i=0;
            List<String> completeFilePattern = new ArrayList<>();
            for(String filePattern : filePatterns) {

                if (!filePattern.endsWith("/")) filePattern = filePattern.concat("/");
                filePattern = filePattern + (blobNamePrefix != null ? blobNamePrefix : "") + "*";
                filePattern = filePattern + "." + "parquet";

                log.info(String.format("File Pattern %s: %s", ++i, filePattern));
                completeFilePattern.add(filePattern);

//                PCollection<FileIO.ReadableFile> files = pipeline.apply(String.format("Find Files With Pattern %s",i), FileIO.match().filepattern(filePattern))
//                        .apply(String.format("Generate ReadableFile for files With Pattern %s",i), FileIO.readMatches());
//
//                PCollection<GenericRecord> genericRecords = files.apply(String.format("Read Parquet Files with pattern %s", i), ParquetIO.readFiles(originalSchema)
//                        .withConfiguration(Map.of("parquet.avro.readInt96AsFixed", "true"))
//                        .withAuditColumnName(auditColumnName)
//                        .withUpdatedSchema(schemaWithAuditColumn));
//
//                PCollection<TableRow> rows = genericRecords.apply("Map GenericRecord To TableRow", ParDo.of(MapGenericRecordToTableRow.builder()
//                        .withSchema(bqSchema)
//                        .withReplaceSpecialCharacters(replaceSpecialChars)));
//
//                rows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
//                        .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
            }

            PCollection<String> directoriesPCol = pipeline.apply("Get Directories", Create.of(completeFilePattern))
                    .apply("Reshuffle Directories", Reshuffle.viaRandomKey());

            PCollection<FileIO.ReadableFile> files =directoriesPCol.apply("Find Files", FileIO.matchAll()).apply("Get Readable Files", FileIO.readMatches())
                    .apply("Reshuffle Files", Reshuffle.viaRandomKey());

            PCollection<GenericRecord> genericRecords = files.apply(String.format("Read Parquet Files with pattern %s", i), ParquetIO.readFiles(originalSchema)
                    .withConfiguration(Map.of("parquet.avro.readInt96AsFixed", "true"))
                    .withAuditColumnName(auditColumnName)
                    .withUpdatedSchema(schemaWithAuditColumn));
            PCollection<TableRow> rows = genericRecords.apply("Map GenericRecord To TableRow", ParDo.of(MapGenericRecordToTableRow.builder()
                    .withSchema(bqSchema)
                    .withReplaceSpecialCharacters(replaceSpecialChars)));
            rows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
                    .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            pipeline.run();

        }

        else if(Pattern.matches(".*.(csv|txt|dat|gz|zip)$", Paths.get(sampleBlobs.get(0)).getFileName().toString())) {
            log.info("Pipeline interpreted to process flat files (csv|txt|dat|gz|zip) based on provided arguments");
            Pipeline pipeline = Pipeline.create(pipelineOptions);
            int i=0;
            List<String> completeFilePattern = new ArrayList<>();
            for(String filePattern : filePatterns) {

                if(!filePattern.endsWith("/")) filePattern = filePattern.concat("/");
                filePattern = filePattern + (blobNamePrefix!=null ? blobNamePrefix : "") + "*";
                if(sampleBlobs.get(0).endsWith("csv")) filePattern = filePattern + ".csv";
                else if(sampleBlobs.get(0).endsWith("txt")) filePattern = filePattern + ".txt";
                else if(sampleBlobs.get(0).endsWith("dat")) filePattern = filePattern + ".dat";
                else if(sampleBlobs.get(0).endsWith("csv.gz")) filePattern = filePattern + ".csv.gz";
                else if(sampleBlobs.get(0).endsWith("txt.gz")) filePattern = filePattern + ".txt.gz";
                else if(sampleBlobs.get(0).endsWith("dat.gz")) filePattern = filePattern + ".dat.gz";
                else if(sampleBlobs.get(0).endsWith("csv.zip")) filePattern = filePattern + ".csv.zip";
                else if(sampleBlobs.get(0).endsWith("txt.zip")) filePattern = filePattern + ".txt.zip";
                else if(sampleBlobs.get(0).endsWith("dat.zip")) filePattern = filePattern + ".dat.zip";
                else {
                    log.info("Blobs suffix didn't match with any pre-configured file formats. Hence using defalt value as csv");
                    filePattern = filePattern + ".csv";
                }

                log.info(String.format("File Pattern %s: %s", ++i, filePattern));

                completeFilePattern.add(filePattern);

//                PCollection<FileIO.ReadableFile> files = pipeline.apply(String.format("Find Files With Pattern %s",i), FileIO.match().filepattern(filePattern))
//                        .apply(String.format("Generate ReadableFile for files With Pattern %s",i), FileIO.readMatches());
//
//                PCollection<KV<String, String>> rows = files.apply("Reshuffle", Reshuffle.viaRandomKey()).apply(
//                        "Read File", ParDo.of(new GcsFileReader().withHeader(pipelineOptions.getHeader()))
//                );
//
//                PCollection<TableRow> tableRows = rows.apply("Convert To TableRow", ParDo.of(
//                        MapKVToTableRow.builder().withBqSchema(bqSchema).withDelimiter(fieldDelimiter)
//                                .withAuditColumnName(auditColumnName).withReplaceSpecialChars(replaceSpecialChars)
//                ));
//
//                tableRows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
//                        .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

//                PCollection<KV<String, String>> rows = pipeline.apply("Read Files", TextIO.readWithMetadata().
//                        from(filePattern)
//                        .withHintMatchesManyFiles());
//
//                PCollection<TableRow> tableRows = rows.apply("Convert To TableRow", ParDo.of(
//                        MapKVToTableRow.builder().withBqSchema(bqSchema).withDelimiter(fieldDelimiter)
//                                .withAuditColumnName(auditColumnName).withReplaceSpecialChars(replaceSpecialChars)
//                ));
//
//                tableRows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
//                        .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            }

            PCollection<String> directoriesPCol = pipeline.apply("Get Directories", Create.of(completeFilePattern))
                    .apply("Reshuffle Directories", Reshuffle.viaRandomKey());

            PCollection<FileIO.ReadableFile> files =directoriesPCol.apply("Find Files", FileIO.matchAll()).apply("Get Readable Files", FileIO.readMatches());
            PCollection<KV<String, String>> rows = files.apply("Reshuffle", Reshuffle.viaRandomKey()).apply(
                    "Read File", ParDo.of(new GcsFileReader().withHeader(pipelineOptions.getHeader()).withFileEncoding(defaultFileEncoding))
            );

            PCollection<TableRow> tableRows = rows.apply("Convert To TableRow", ParDo.of(
                    MapKVToTableRow.builder().withBqSchema(bqSchema).withDelimiter(fieldDelimiter)
                            .withAuditColumnName(auditColumnName).withReplaceSpecialChars(replaceSpecialChars)
                            .withUseStandardCsvParser(pipelineOptions.getUseStandardCsvParser())
            ));

            tableRows.apply("Write To BigQuery", BigQueryIO.writeTableRows().to(String.format("%s:%s.%s", bigqueryProject, bigqueryDataset, bigqueryTable))
                    .withSchema(bqTableSchema).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            pipeline.run();

        }
        else {
            log.warn("The file pattern is not a valid pre-configured type (parquet, csv, txt, dat, csv.gz, txt.gz, csv.zip, txt.zip");
            return;
        }
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
