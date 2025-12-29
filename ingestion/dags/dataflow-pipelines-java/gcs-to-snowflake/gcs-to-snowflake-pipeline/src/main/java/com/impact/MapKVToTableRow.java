package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import com.impact.utils.date.DateHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.HashMap;


@Slf4j
public class MapKVToTableRow extends DoFn<KV<String, String>, TableRow> {

    protected Map<String, String> schema;
    protected String delimiter;
    protected LinkedList<String> fieldNames;
    protected boolean replaceSpecialChars;
    protected boolean useStandardCsvParser;

    protected String auditColumnName;
    private Counter totalOutputRowsCounter = Metrics.counter(MapKVToTableRow.class, "totalOutputRows");
    private Counter totalOutputBytesCounter = Metrics.counter(MapKVToTableRow.class, "totalOutputBytes");

    private static final Map<Character, String> replacements = new HashMap<>();

    static {
        replacements.put('\'', "__ia_char_01");
        replacements.put('\"', "__ia_char_02");
        replacements.put('/', "__ia_char_03");
        replacements.put('\\', "__ia_char_04");
        replacements.put('`', "__ia_char_05");
        replacements.put('~', "__ia_char_06");
        replacements.put('!', "__ia_char_07");
        replacements.put('@', "__ia_char_08");
        replacements.put('#', "__ia_char_09");
        replacements.put('$', "__ia_char_10");
        replacements.put('%', "__ia_char_11");
        replacements.put('^', "__ia_char_12");
        replacements.put('&', "__ia_char_13");
        replacements.put('*', "__ia_char_14");
        replacements.put('(', "__ia_char_15");
        replacements.put(')', "__ia_char_16");
        replacements.put('=', "__ia_char_19");
        replacements.put('+', "__ia_char_20");
        replacements.put('{', "__ia_char_21");
        replacements.put('}', "__ia_char_22");
        replacements.put('[', "__ia_char_23");
        replacements.put(']', "__ia_char_24");
        replacements.put('|', "__ia_char_25");
        replacements.put(';', "__ia_char_27");
        replacements.put('<', "__ia_char_28");
        replacements.put('>', "__ia_char_29");
        replacements.put(',', "__ia_char_30");
        replacements.put('?', "__ia_char_32");
        replacements.put('\t', "__ia_char_33");
    }

    public static MapKVToTableRow builder() {
        return new MapKVToTableRow();
    }

    public MapKVToTableRow withSfSchema(Map<String, String> schema) {
        this.schema = schema;
        return this;
    }

    public MapKVToTableRow withReplaceSpecialChars(boolean replaceSpecialChars) {
        this.replaceSpecialChars = replaceSpecialChars;
        return this;
    }

    public MapKVToTableRow withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public MapKVToTableRow withAuditColumnName(String auditColumnName) {
        this.auditColumnName = auditColumnName;
        return this;
    }

    public MapKVToTableRow withUseStandardCsvParser(boolean useStandardCsvParser) {
        this.useStandardCsvParser = useStandardCsvParser;
        return this;
    }

    @Setup
    public void setup() {
        assert schema!=null && schema.size()>0 : "schema is either null or empty";
        LinkedList<String> columns = new LinkedList<>();
        if(auditColumnName!=null) {
            schema.remove(auditColumnName);
        }
        for(String column : schema.keySet()) {
            columns.add(column);
        }
        this.fieldNames = columns;
        log.info(String.format("Using Standard CSV Parser %s", useStandardCsvParser));
    }

    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "MapKVToTableRow DoFn Bundle Started");
    }

    @ProcessElement
    public void processElement(@Element KV<String, String> element, ProcessContext context) throws IOException {
        String fileName = element.getKey();
        String line = element.getValue();
        String fileDate = DateHelper.getFileDateFromPath(fileName);
        totalOutputBytesCounter.inc(line.getBytes().length+1);
        totalOutputRowsCounter.inc();
        List<String> fields = null;
        if(line.length()>0) {
            if(useStandardCsvParser) {
                CSVParser parser = CSVParser.parse(line, CSVFormat.EXCEL.withDelimiter(delimiter.charAt(0)).withEscape('\\'));
                fields = parser.getRecords().get(0).toList();
            }
            else {
                line = com.impact.utils.text.StringUtils.truncateQuotesEnclosingFields(line, delimiter);
                fields = Arrays.stream(line.split(Pattern.quote(delimiter))).collect(Collectors.toList());
            }
        }
        //line = com.impact.utils.text.StringUtils.truncateQuotesEnclosingFields(line, delimiter);
        //CSVParser parser = CSVParser.parse(line, CSVFormat.EXCEL.withDelimiter(delimiter.charAt(0)).withEscape('\\'));
        //List<String> fields = parser.getRecords().get(0).toList();
        //List<String> fields = Arrays.stream(line.split(Pattern.quote(delimiter))).collect(Collectors.toList());
        if(fields!=null && fields.size()>0) {
            TableRow tr = new TableRow();
            for(int i=0; i<fieldNames.size(); i++) {
                try {
                    if(fields.get(i).length()>0) {
                        if (schema.get(fieldNames.get(i)).equalsIgnoreCase("STRING") && replaceSpecialChars) {
                            tr.set(fieldNames.get(i), preprocessStringField(fields.get(i)));
                        } else {
                            tr.set(fieldNames.get(i), fields.get(i));
                        }
                    }
                }
                catch (IndexOutOfBoundsException ignored) {}
                catch (Exception e) { throw e; }
            }
            if(auditColumnName!=null) tr.set(auditColumnName, fileDate);
            context.output(tr);
        }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext finishBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "MapKVToTableRow DoFn Bundle Finished");
    }

    @Teardown
    public void teardown() {
    }

    public String preprocessStringField(String colValue) {
        StringBuilder result = new StringBuilder();
        for (char c : colValue.toCharArray()) {
            if (replacements.containsKey(c)) {
                result.append(replacements.get(c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    // java -cp gcs-to-bigquery-pipeline-1.7.1.jar com.impact.GcsToBigQueryPipeline --runner=DataflowRunner --project=victorias-secret-393308 --serviceAccount=vs-sourcing-dev@victorias-secret-393308.iam.gserviceaccount.com --region=us-central1 --bigqueryRegion=US --tempLocation=gs://vs-sourcing-dev/dataflow/temp/ --stagingLocation=gs://vs-sourcing-dev/dataflow/staging/ --numWorkers=1 --maxNumWorkers=5 --workerMachineType=n2-highmem-4 --subnetwork=regions/us-central1/subnetworks/vs-dev-vm-0 --usePublicIps=False --bigqueryProject=victorias-secret-393308 --bigqueryDataset=vs_sourcing_dev --bigqueryTable=Product_Master_tab_sch_test --replaceTable=False '--directories=gs://vs_data_ingestion/Periodic_data_transfer/2024-07-29/;gs://vs_data_ingestion/Periodic_data_transfer/2024-07-30/;gs://vs_data_ingestion/Periodic_data_transfer/2024-07-31/;gs://vs_data_ingestion/Periodic_data_transfer/2024-08-01/;gs://vs_data_ingestion/Periodic_data_transfer/2024-08-02/;gs://vs_data_ingestion/Periodic_data_transfer/2024-08-03/;gs://vs_data_ingestion/Periodic_data_transfer/2024-08-04/' --lastProcessedDate= --gcsBucketName=vs_data_ingestion --gcsBlobPathPrefix=Periodic_data_transfer --blobNamePrefix=Product_Master '--blobNameSuffix=(csv|dat|txt|csv.gz|dat.gz|txt.gz|parquet)' --folderDatePattern=yyyy-MM-dd --auditColumn=SYNCSTARTDATETIME '--fieldDelimiter=|' --pullType=incremental --bqPartitionColumn=null --bqClusteringColumns=null --replaceSpecialChars --fileEncoding=null --jobName=gcs-to-bq-product-master-vs-dev
    // java -cp gcs-to-bigquery-pipeline-1.7.2.jar com.impact.GcsToBigQueryPipeline --runner=DataflowRunner --project=data-ingestion-342714 --serviceAccount=ralphlauren-sourcing-dev@data-ingestion-342714.iam.gserviceaccount.com --region=us-central1 --bigqueryRegion=US --tempLocation=gs://rl-na-sourcing-dev/dataflow/temp/ --stagingLocation=gs://rl-na-sourcing-dev/dataflow/staging/ --numWorkers=5 --maxNumWorkers=5 --workerMachineType=n2-standard-8 --subnetwork=regions/us-central1/subnetworks/ralphlauren-dev-vm-0 --usePublicIps=False --bigqueryProject=data-ingestion-342714 --bigqueryDataset=rl_na_sourcing_dev --bigqueryTable=Regional_Product_Extnd_sch_test_arun --replaceTable=true --directories=gs://ralphlauren_northamerica_ingestion/BI_feed_integration/20240819/ --lastProcessedDate= --gcsBucketName=ralphlauren_northamerica_ingestion --gcsBlobPathPrefix=BI_feed_integration --blobNamePrefix=Regional_Product_Extnd/ '--blobNameSuffix=(csv|dat|txt|csv.gz|dat.gz|txt.gz|gz|parquet)' --folderDatePattern=yyyyMMdd --auditColumn=SYNCSTARTDATETIME --fieldDelimiter=None --pullType=incremental --bqPartitionColumn=SYNCSTARTDATETIME --bqClusteringColumns=null --replaceSpecialChars=true --fileEncoding=null --jobName=gcs-to-bq-regional-product-extnd-rl-na-dev
}