package com.impact;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.impact.utils.snowflake.SnowflakeSchema;
import com.impact.utils.snowflake.SnowflakeSchemaField;

import java.util.ArrayList;
import java.util.List;

public class SnowflakeToBigQuerySchemaConverter {

    public static TableSchema convert(SnowflakeSchema sfSchema) {
        TableSchema bqTableSchema = new TableSchema();
        List<TableFieldSchema> fieldList = new ArrayList<>();
        List<SnowflakeSchemaField> sfFields = sfSchema.getFields();
        for(SnowflakeSchemaField sfField : sfFields) {
            fieldList.add(
                    new TableFieldSchema().setName(sfField.getName())
                            .setType(mapSnowflakeToBigQueryType(sfField.getType()))
            );
        }
        bqTableSchema.setFields(fieldList);
        return bqTableSchema;
    }

    public static String mapSnowflakeToBigQueryType(String snowflakeType) {
        String bqType = null;
        switch (snowflakeType.toUpperCase()) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                bqType = "NUMERIC";
                break;
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
            case "BYTEINT":
                //bqType = "BIGNUMERIC";
                bqType = "INTEGER";
                break;
            case "FLOAT":
            case "FLOAT4":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "REAL":
                bqType = "FLOAT64";
                break;
            case "VARCHAR":
            case "CHAR":
            case "CHARACTER":
            case "STRING":
            case "TEXT":
                bqType = "STRING";
                break;
            case "BINARY":
            case "VARBINARY":
                bqType = "BYTES";
                break;
            case "BOOLEAN":
                bqType = "BOOL";
                break;
            case "DATE":
                bqType = "DATE";
                break;
            case "TIME":
                bqType = "TIME";
                break;
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
            case "DATETIME":
            case "TIMESTAMPNTZ":
            case "TIMESTAMP WITHOUT TIME ZONE":
                bqType = "DATETIME";
                break;
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
            case "TIMESTAMPLTZ":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
            case "TIMESTAMPTZ":
            case "TIMESTAMP WITH TIME ZONE":
                bqType = "TIMESTAMP";
                break;
            default:
                bqType = "STRING";
                break;
            // Add support for complex data types likes ARRAY, OBJECT and VARIANT
        }
        return bqType;
    }

}
