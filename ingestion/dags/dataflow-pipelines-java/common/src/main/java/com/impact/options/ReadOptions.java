package com.impact.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ReadOptions extends PipelineOptions {
    @Description("Pull Type (Incremental or Full)")
    @Default.String("full")
    String getPullType();
    void setPullType(String value);

    @Description("Incremental Column")
    String getIncrementalColumn();
    void setIncrementalColumn(String value);

    @Description("Incremental Column Value")
    String getIncrementalColumnValue();
    void setIncrementalColumnValue(String value);

    @Description("Field Delimiter")
    @Default.String(",")
    String getFieldDelimiter();
    void setFieldDelimiter(String value);

    @Description("Header")
    @Default.Boolean(true)
    boolean getHeader();
    void setHeader(boolean value);

    @Description("Source Query")
    String getSourceQuery();
    void setSourceQuery(String value);

    @Description("Folder Date Pattern")
    @Default.String("yyyy-MM-dd")
    String getFolderDatePattern();
    void setFolderDatePattern(String value);

    @Description("Encoding to use when reading files")
    String getFileEncoding();
    void setFileEncoding(String value);

    @Description("Use Standard CSV Parser")
    @Default.Boolean(true)
    boolean getUseStandardCsvParser();
    void setUseStandardCsvParser(boolean value);

    @Description("Source Queries")
    String getSourceQueries();
    void setSourceQueries(String value);

}
