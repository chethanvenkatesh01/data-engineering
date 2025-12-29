package com.impact;

import com.impact.options.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface GcsToBigQueryPipelineOptions extends BigQueryOptions, GcsOptions, ReadOptions,
        WriteOptions, NotificationOptions {

//    @Description("GCS Bucket Name")
//    String getGcsBucketName();
//    void setGcsBucketName(String value);

    @Description("GCS Blob Path Prefix")
    String getGcsBlobPathPrefix();
    void setGcsBlobPathPrefix(String value);

    @Description("GCS Blob Format")
    String getGcsBlobType();
    void setGcsBlobType(String value);

    @Description("Directories")
    String getDirectories();
    void setDirectories(String value);

//    @Description("Audit Column")
//    String getAuditColumn();
//    void setAuditColumn(String value);

//    @Description("Audit Column Data Type. Must be a valid BQ data type")
//    @Default.String("DATETIME")
//    String getAuditColumnDataType();
//    void setAuditColumnDataType(String value);

    @Description("Blob name prefix")
    String getBlobNamePrefix();
    void setBlobNamePrefix(String value);

    @Description("Blob name suffix")
    @Default.String("csv")
    String getBlobNameSuffix();
    void setBlobNameSuffix(String value);

//    @Description("Field Delimiter")
//    String getFieldDelimiter();
//    void setFieldDelimiter(String value);

    @Description("Last Processed Date")
    String getLastProcessedDate();
    void setLastProcessedDate(String value);

//    @Description("Folder Date Pattern")
//    @Default.String("yyyyMMdd")
//    String getFolderDatePattern();
//    void setFolderDatePattern(String value);

//    @Description("partitioningColumn")
//    String getBqPartitioningColumn();
//    void setPartitioningColumn(String value);
//
//    @Description("Clustering Columns(Comma separated values)")
//    String getBqClusteringColumns();
//    void setClusteringColumns(String value);

//    @Description("Replace Special Characters in String fields")
//    @Default.Boolean(false)
//    boolean getReplaceSpecialCharacters();
//    void setReplaceSpecialCharacters(boolean value);
//
//    @Description("Pull Type")
//    @Default.String("full")
//    String getPullType();
//    void setPullType(String value);


}
