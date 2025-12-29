package com.impact;

import org.apache.beam.sdk.options.Description;

public interface SfToDbCopyMultiPipelineOptions extends SfToDbPipelineOptions {
    @Description("Dervied tables query")
    String getDerviedTablesQuery();
    void setDerviedTablesQuery(String value);

    @Description("Sf Table Name to Pg Table Name Map")
    String getSfTableToPgTableNameMap();
    void setSfTableToPgTableNameMap(String value);
}
