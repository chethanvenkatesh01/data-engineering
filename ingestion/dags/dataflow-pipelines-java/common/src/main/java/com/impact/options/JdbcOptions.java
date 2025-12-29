package com.impact.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

public interface JdbcOptions extends PipelineOptions {
    @Description("DB Host")
    String getDbHost();
    void setDbHost(String value);

    @Description("DB Port")
    String getDbPort();
    void setDbPort(String value);

    @Description("DB User")
    @Hidden
    String getDbUser();
    void setDbUser(String value);

    @Description("DB Password")
    @Hidden
    String getDbPassword();
    void setDbPassword(String value);

    @Description("DB name")
    String getDbName();
    void setDbName(String value);

    @Description("DB schema")
    String getDbSchemaName();
    void setDbSchemaName(String value);

    @Description("DB Table")
    String getDbTable();
    void setDbTable(String value);

    @Description("DB Type")
    String getDbType();
    void setDbType(String value);

    @Description("DB secret name")
    String getDbSecretName();
    void setDbSecretName(String value);
}
