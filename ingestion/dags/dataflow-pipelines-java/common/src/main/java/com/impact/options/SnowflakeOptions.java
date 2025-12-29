package com.impact.options;

import org.apache.beam.sdk.options.*;

public interface SnowflakeOptions extends PipelineOptions {
    @Description("Snowflake Server Name")
    String getSnowflakeServerName();
    void setSnowflakeServerName(String value);

    @Description("Snowflake Username")
    String getSnowflakeUserName();
    void setSnowflakeUserName(String value);

    @Description("Snowflake Password")
    @Hidden
    String getSnowflakePassword();
    void setSnowflakePassword(String value);

    @Description("Snowflake Database Name")
    String getSnowflakeDatabase();
    void setSnowflakeDatabase(String value);

    @Description("Snowflake Schema")
    String getSnowflakeSchema();
    void setSnowflakeSchema(String value);

    @Description("Snowflake User Role")
    String getSnowflakeUserRole();
    void setSnowflakeUserRole(String value);

    @Description("Snowflake Warehouse")
    String getSnowflakeWarehouse();
    void setSnowflakeWarehouse(String value);

    @Description("Storage Integration")
    String getStorageIntegration();
    void setStorageIntegration(String value);

    @Description("Staging Bucket")
    String getStagingBucket();
    void setStagingBucket(String value);

    @Description("BQ Clustering Column (comma separated values)")
    String getSnowflakeClusteringColumns();
    void setSnowflakeClusteringColumns(String value);
}
