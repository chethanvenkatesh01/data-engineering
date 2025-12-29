package com.impact;


import com.impact.options.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface SftpToBigQueryPipelineOptions extends BigQueryOptions, SftpOptions, ReadOptions,
        WriteOptions, NotificationOptions {
    @Description("Root Directory from which files should be fetched")
    String getRootDirectory();
    void setRootDirectory(String value);

    @Description("Name of the file which should be processed. Should be specified along with extension")
    String getFileName();
    void setFileName(String value);

    @Description("Absolute File path which should be processed")
    String getAbsoluteFilePath();
    void setAbsoluteFilePath(String value);

    @Description("Prefix of the files which have to be processed")
    String getFilePrefix();
    void setFilePrefix(String value);

    @Description("Suffix of the files which have to be processed")
    String getFileSuffix();
    void setFileSuffix(String value);

    // Customized options
    @Description("List of directories from which files have to be processed. Give ';' separated directory paths")
    String getDirectories();
    void setDirectories(String value);

    @Description("Last processed date. This date will be used to find directories with name in date format" +
            "Datetime can also be specified. Recommended formats are YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    String getLastProcessedDate();
    void setLastProcessedDate(String value);

//    @Description("Pull Type")
//    @Default.String("incremental")
//    String getPullType();
//    void setPullType(String value);

//    @Description("Field Delimiter")
//    @Default.String(",")
//    String getFieldDelimiter();
//    void setFieldDelimiter(String value);

//    @Description("Header")
//    @Default.Boolean(true)
//    boolean getHeader();
//    void setHeader(boolean value);

//    @Description("Replace Table")
//    @Default.Boolean(false)
//    boolean getReplaceTable();
//    void setReplaceTable(boolean value);

//    @Description("Name of the audit column to be added to BQ table")
//    String getAuditColumn();
//    void setAuditColumn(String value);

//    @Description("Replace Special Characters")
//    @Default.Boolean(false)
//    boolean getReplaceSpecialChars();
//    void setReplaceSpecialChars(boolean value);

//    @Description("partitioningColumn")
//    String getPartitioningColumn();
//    void setPartitioningColumn(String value);
//
//    @Description("Clustering Columns(Comma separated values)")
//    String getClusteringColumns();
//    void setClusteringColumns(String value);

//    @Description("Secret Manager Secret Name")
//    String getSftpSecretName();
//    void setSftpSecretName(String value);


}
