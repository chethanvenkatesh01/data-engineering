package com.impact;

import com.impact.options.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface SfToDbPipelineOptions extends JdbcOptions, SnowflakeOptions, ReadOptions,
        WriteOptions, NotificationOptions {

    @Description("DB Table Schema As JSON")
    String getDbTableSchema();
    void setDbTableSchema(String value);

    @Description("Batch Size")
    @Default.Integer(1000)
    Integer getBatchSize();
    void setBatchSize(Integer value);

    //    @Description("DB Host")
//    String getDbHost();
//    void setDbHost(String value);
//
//    @Description("DB Port")
//    String getDbPort();
//    void setDbPort(String value);
//
//    @Description("DB User")
//    String getDbUser();
//    void setDbUser(String value);
//
//    @Description("DB Password")
//    String getDbPassword();
//    void setDbPassword(String value);
//
//    @Description("DB Name")
//    String getDbName();
//    void setDbName(String value);
//
//    @Description("DB Schema")
//    String getDbSchemaName();
//    void setDbSchemaName(String value);

//    @Description("BQ Dataset")
//    String getBqDataset();
//    void setBqDataset(String value);
//
//    @Description("BQ Table")
//    String getBqTable();
//    void setBqTable(String value);

//    @Description("DB Table Pull Type")
//    @Default.String("full")
//    String getPullType();
//    void setPullType(String value);

    @Description("DB Queries as ';' separated string")
    @Default.String("")
    String getQueries();
    void setQueries(String value);

//    @Description("DB Table")
//    String getDbTable();
//    void setDbTable(String value);
//
//    @Description("DB Type")
//    @Validation.Required
//    String getDbType();
//    void setDbType(String value);

    @Description("DB Table Partition Column. Must be numeric, date or timestamp or string")
    String getDbPartitionColumn();
    void setDbPartitionColumn(String value);

    @Description("Number of Partitions or The number of parallel queries")
    Integer getNumPartitions();
    void setNumPartitions(Integer value);

//    @Description("DB Incremental column")
//    String getDbIncrementalColumn();
//    void setDbIncrementalColumn(String value);
//
//    @Description("DB Incremental Column value")
//    String getDbIncrementalColumnValue();
//    void setDbIncrementalColumnValue(String value);

    @Description("Threshold for query partitioning")
    Integer getQueryPartitioningThreshold();
    void setQueryPartitioningThreshold(Integer value);

    @Description("Maximum number of elements in a partition")
    @Default.Integer(1000000)
    Integer getMaxPartitionSize();
    void setMaxPartitionSize(Integer value);


    @Description("Snowflake Table To Write")
    String getSnowflakeTable();
    void setSnowflakeTable(String value);

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
