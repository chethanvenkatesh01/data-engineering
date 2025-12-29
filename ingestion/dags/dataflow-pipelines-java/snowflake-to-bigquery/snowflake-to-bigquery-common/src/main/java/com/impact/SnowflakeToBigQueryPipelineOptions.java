package com.impact;

import com.impact.options.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;


public interface SnowflakeToBigQueryPipelineOptions extends SnowflakeOptions, BigQueryOptions, ReadOptions,
        WriteOptions, NotificationOptions {

    @Description("Snowflake Table To Read")
    String getSnowflakeTable();
    void setSnowflakeTable(String value);

    @Description("Snowflake Query")
    String getSnowflakeQuery();
    void setSnowflakeQuery(String value);

//    @Description("Pull Type (Incremental or Full)")
//    @Default.String("full")
//    String getPullType();
//    void setPullType(String value);

//    @Description("Incremental Column")
//    String getIncrementalColumn();
//    void setIncrementalColumn(String value);

//    @Description("Incremental Column Value")
//    String getIncrementalColumnValue();
//    void setIncrementalColumnValue(String value);

    @Description("Storage Integration Name")
    String getSnowflakeStorageIntegration();
    void setSnowflakeStorageIntegration(String value);

    @Description("Snowflake Staging Bucket Name")
    String getSnowflakeStagingBucket();
    void setSnowflakeStagingBucket(String value);

//    @Description("Audit Column Name")
//    String getAuditColumnName();
//    void setAuditColumnName(String value);

//    @Description("Replace Special Characters")
//    @Default.Boolean(false)
//    boolean getReplaceSpecialChars();
//    void setReplaceSpecialChars(boolean value);

    @Description("Snowflake Secret Name")
    String getSnowflakeSecretName();
    void setSnowflakeSecretName(String value);



}
