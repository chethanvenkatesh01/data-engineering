package com.impact.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryOptions extends PipelineOptions {

    @Description("BigQuery Project")
    String getBigqueryProject();
    void setBigqueryProject(String value);

    @Description("BigQuery Billing Project")
    String getBigqueryBillingProject();
    void setBigqueryBillingProject(String value);

    @Description("BigQuery Dataset")
    String getBigqueryDataset();
    void setBigqueryDataset(String value);

    @Description("BigQuery Table")
    String getBigqueryTable();
    void setBigqueryTable(String value);

//    @Description("Replace Table")
//    @Default.Boolean(false)
//    boolean getReplaceTable();
//    void setReplaceTable(boolean value);

    @Description("BQ Partition Column")
    String getBqPartitionColumn();
    void setBqPartitionColumn(String value);

    @Description("BQ Clustering Column (comma separated values)")
    String getBqClusteringColumns();
    void setBqClusteringColumns(String value);

    @Description("BQ Region")
    String getBigqueryRegion();
    void setBigqueryRegion(String value);

}
