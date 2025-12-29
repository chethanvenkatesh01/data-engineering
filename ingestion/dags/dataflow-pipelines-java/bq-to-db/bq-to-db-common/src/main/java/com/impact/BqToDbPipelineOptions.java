package com.impact;

import com.impact.options.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BqToDbPipelineOptions extends JdbcOptions, BigQueryOptions, ReadOptions,
        WriteOptions, NotificationOptions {

    @Description("DB Table Schema As JSON")
    String getDbTableSchema();
    void setDbTableSchema(String value);

    @Description("Batch Size")
    @Default.Integer(1000)
    Integer getBatchSize();
    void setBatchSize(Integer value);

}
