package com.impact;

import java.util.List;

import com.impact.options.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface DbToBqPipelineOptions extends JdbcOptions, BigQueryOptions, ReadOptions,
        WriteOptions, NotificationOptions {

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

//    @Description("DB secret name")
//    String getDbSecretName();
//    void setDbSecretName(String value);

//    @Description("Replace Table in BQ")
//    boolean getReplaceTable();
//    void setReplaceTable(boolean value);

//    @Description("BQ Table Partition Column")
//    String getBqPartitionColumn();
//    void setBqPartitionColumn(String value);
//
//    @Description("BQ Table Clustering Columns")
//    String getBqClusteringColumns();
//    void setBqClusteringColumns(String value);

//    @Description("Allow special characters in data")
//    @Default.Boolean(true)
//    boolean getAllowSpecialChars();
//    void setAllowSpecialChars(boolean value);

//    @Description("Secret Managet Secret Name")
//    String getSecretName();
//    void setSecretName(String value);
}
