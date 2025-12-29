package com.impact.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface GcsOptions extends PipelineOptions {
    @Description("GCS Bucket Name")
    String getGcsBucketName();
    void setGcsBucketName(String value);

}
