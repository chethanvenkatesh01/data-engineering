package com.impact.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface WriteOptions extends PipelineOptions {
    @Description("Replace Special Characters")
    @Default.Boolean(false)
    boolean getReplaceSpecialChars();
    void setReplaceSpecialChars(boolean value);

    @Description("Audit Column Name")
    String getAuditColumn();
    void setAuditColumn(String value);

    @Description("Replace Table")
    @Default.Boolean(false)
    boolean getReplaceTable();
    void setReplaceTable(boolean value);

    @Description("Output Directory")
    String getOutputDirectory();
    void setOutputDirectory(String value);

    @Description("Output File Prefix")
    String getOutputFilePrefix();
    void setOutputFilePrefix(String value);

    @Description("Output File Type")
    @Default.String("csv")
    String getOutputFileType();
    void setOutputFileType(String value);


}
