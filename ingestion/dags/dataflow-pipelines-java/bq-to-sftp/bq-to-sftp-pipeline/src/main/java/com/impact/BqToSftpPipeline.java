package com.impact;

import com.google.cloud.bigquery.Schema;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.impact.utils.bigquery.BigQueryClient;
import com.impact.utils.gcp.SecretManager;
import com.impact.utils.sftp.SftpSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Slf4j
public class BqToSftpPipeline {
    public static void main(String[] args) throws Exception {
        BqToSftpPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BqToSftpPipelineOptions.class);
        run(pipelineOptions);
    }

    private static void run(BqToSftpPipelineOptions pipelineOptions) throws Exception {

        String projectId = pipelineOptions.as(GcpOptions.class).getProject();

        // BigQuery Params
        String bigQueryDataset = pipelineOptions.getBigqueryDataset();
        String bigQueryTable = pipelineOptions.getBigqueryTable();
        String sourceQuery = pipelineOptions.getSourceQuery();
        String bqProject = pipelineOptions.getBigqueryProject();
        String bqBillingProject = pipelineOptions.getBigqueryBillingProject();
        bqProject = bqProject!=null ? bqProject : projectId;
        bqBillingProject = bqBillingProject!=null ? bqBillingProject : projectId;
        String bqRegion = pipelineOptions.getBigqueryRegion();
        if(bqRegion==null) {
            bqRegion = pipelineOptions.as(GcpOptions.class).getWorkerRegion();
        }
        // SFTP params
        String sftpHost = pipelineOptions.getSftpHost();
        String sftpUsername = pipelineOptions.getSftpUsername();
        String sftpPassword = pipelineOptions.getSftpPassword();
        String sftpSecretName = pipelineOptions.getSftpSecretName();
        String fieldDelimiter = pipelineOptions.getFieldDelimiter();
        String outputDirectory = pipelineOptions.getOutputDirectory();
        String outputFilePrefix = pipelineOptions.getOutputFilePrefix();

        bqBillingProject = bqBillingProject!=null ? bqBillingProject : projectId;
        BigQueryClient bqClient = new BigQueryClient(bqBillingProject, bqRegion);
        Schema bqSchema;
        String header =null;
        Map<String, Map<String,String>> bqSchemaMap = new LinkedHashMap<>();
        if(sourceQuery!=null) {
            log.info(sourceQuery);
            bqSchema = bqClient.getSchemaFromQuery(sourceQuery);
            bqSchemaMap = bqClient.convertSchemaToMap(bqSchema);
            StringJoiner joiner = new StringJoiner(fieldDelimiter);
            for (String key : bqSchemaMap.keySet()) {
                joiner.add(key);
            }
            header = joiner.toString();
        }
        else if(bigQueryTable!=null) {
            String query = String.format("SELECT * FROM `%s.%s.%s`", bqProject, bigQueryDataset, bigQueryTable);
            log.info(query);
            bqSchema = bqClient.getSchemaFromQuery(query);
            bqSchemaMap = bqClient.convertSchemaToMap(bqSchema);
            StringJoiner joiner = new StringJoiner(fieldDelimiter);
            for (String key : bqSchemaMap.keySet()) {
                joiner.add(key);
            }
            header = joiner.toString();
        }

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

        String finalQuery = null;

        if (sourceQuery != null) {
            finalQuery = sourceQuery;
        } else if (bigQueryTable != null) {
            finalQuery = String.format("SELECT * FROM `%s.%s.%s`", bqProject, bigQueryDataset, bigQueryTable);
        } else {
            throw new Exception("Either sourceQuery or bigQueryTable argument should be provided");
        }

        Pipeline p = Pipeline.create(pipelineOptions);

        PCollection<String> rows = p.apply("Read from BigQuery", BigQueryIO.readTableRows().fromQuery(finalQuery).usingStandardSql()
                .withQueryLocation(bqRegion)).apply("Format Rows", ParDo.of(new TableRowToString(fieldDelimiter)));

        rows.apply("Write To File", ParDo.of(SftpSink.builder().withConfiguration(sftpHost, sftpUsername, sftpPassword)
                .withDirectory(outputDirectory).withFilePrefix(outputFilePrefix).withHeader(header).withDelimiter(fieldDelimiter)));

        p.run();
    }
}
