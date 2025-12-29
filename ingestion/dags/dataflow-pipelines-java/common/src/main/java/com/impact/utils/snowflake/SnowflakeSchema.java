package com.impact.utils.snowflake;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SnowflakeSchema extends SnowflakeTableSchema implements Serializable {
    private List<SnowflakeSchemaField> fields;

    public SnowflakeSchema() {
        fields = null;
    }

    public SnowflakeSchema(List<SnowflakeSchemaField> fields) {
        this.fields = fields;
    }

    public SnowflakeSchema(Map<Integer, SnowflakeSchemaField> fieldsMap) {
        List<SnowflakeSchemaField> fieldList = new LinkedList<>();
        for(Integer idx : fieldsMap.keySet()) {
            fieldList.add(fieldsMap.get(idx));
        }
        this.fields = fieldList;
    }

    public List<SnowflakeSchemaField> getFields() {
        return fields;
    }

    public int getSize() {
        return (fields != null) ? fields.size() : 0;
    }

}
