package com.impact.utils.snowflake;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

@Slf4j
public class SnowflakeSchemaConversion {

    // Fetch Snowflake type mappings
    public static JSONObject getSchemaMappingForDb(String dbType) throws Exception {
        String jsonStr = readFromInputStream(SnowflakeSchemaConversion.class
                .getResourceAsStream("/json/sf_to_db_dtypes_map.json"));

        JSONObject schemaMapping = new JSONObject(jsonStr);
        return schemaMapping.getJSONObject(dbType);
    }

    public static JSONObject getSchemaMappingForSf(String dbType) throws Exception {
        String jsonStr = readFromInputStream(SnowflakeSchemaConversion.class
                .getResourceAsStream("/json/db_to_sf_dtypes_map.json"));
        JSONObject schemaMapping = new JSONObject(jsonStr);
        return schemaMapping.getJSONObject(dbType);
    }
    public static Map<String, String> sfToDb(Map<String, String> sfSchemaMap, String userProvidedDbSchema,
                                                           String dbType) throws Exception {
        // Get the schema mapping for the target database
        JSONObject schemaMap = getSchemaMappingForDb(dbType);
        log.info(String.format("Schema Map %s", schemaMap.toString()));

        // Initialize the database schema map
        Map<String, String> dbSchema = new LinkedHashMap<>();

        // Parse user-provided schema into a map
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> userProvidedSchema = mapper.readValue(userProvidedDbSchema, Map.class);

        // Iterate through Snowflake schema map
        for (String field : sfSchemaMap.keySet()) {
            // If the field exists in the user-provided schema, use that mapping
            if (userProvidedSchema.containsKey(field)) {
                dbSchema.put(field, userProvidedSchema.get(field));
            } else {
                // Get the Snowflake data type for the field
                String sfType = sfSchemaMap.get(field);
                // Map the Snowflake type to the database type
                dbSchema.put(field.toLowerCase(), schemaMap.getString(sfType));

            }
        }

        return dbSchema;
    }

    public static Map<String, String> sfToDb(Map<String, String> sfSchemaMap, String dbType) throws Exception {
        // Get the schema mapping for the target database
        JSONObject schemaMap = getSchemaMappingForDb(dbType);
        log.info(String.format("Schema Map %s", schemaMap.toString()));

        // Initialize the database schema map
        Map<String, String> dbSchema = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : sfSchemaMap.entrySet()) {
            String field = entry.getKey(); // Field name
            String sfType = entry.getValue(); // Snowflake data type
            // Map the Snowflake type to the target database type
            dbSchema.put(field.toLowerCase(), schemaMap.getString(sfType));

        }
        return dbSchema;
    }


    public static Map<String, String> dbToSf(Map<String, String> dbSchema, String dbType) throws Exception {
        JSONObject schemaMap = getSchemaMappingForSf(dbType);
        log.info(String.format("Schema Map %s", schemaMap.toString()));
        Map<String, String> sfSchema = new LinkedHashMap<>();
        for (String field : dbSchema.keySet()) {
            String dbTypeValue = dbSchema.get(field).toUpperCase();
            sfSchema.put(field, schemaMap.getString(dbTypeValue));
        }
        return sfSchema;
    }

    public static SnowflakeSchema convertMapToSnowflakeSchema(Map<String, String> schema) {
        List<SnowflakeSchemaField> fields = new ArrayList<>();
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            fields.add(new SnowflakeSchemaField(entry.getKey(), entry.getValue()));
        }
        return new SnowflakeSchema(fields);
    }

    private static String readFromInputStream(InputStream inputStream) throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    // Convert Parquet schema to Snowflake schema
    public static Map<String, String> parquetSchemaToSfSchema(List<Map<String, String>> parquetSchema) {
        Map<String, String> sfSchema = new LinkedHashMap<>();
        for (Map<String, String> field : parquetSchema) {
            switch (field.get("physicalType")) {
                case "BOOLEAN":
                case "BOOL":
                    sfSchema.put(field.get("name"), "BOOLEAN");
                    break;
                case "INT32":
                    if (field.get("logicalType") != null) {
                        if (field.get("logicalType").equals("DATE")) {
                            sfSchema.put(field.get("name"), "DATE");
                        } else if (field.get("logicalType").equals("DECIMAL")) {
                            sfSchema.put(field.get("name"), "NUMBER");
                        } else {
                            sfSchema.put(field.get("name"), "INTEGER");
                        }
                    } else {
                        sfSchema.put(field.get("name"), "INTEGER");
                    }
                    break;
                case "INT64":
                    if (field.get("logicalType") != null) {
                        if (field.get("logicalType").equals("TIMESTAMP")) {
                            sfSchema.put(field.get("name"), "TIMESTAMP_NTZ");
                        } else if (field.get("logicalType").equals("DECIMAL")) {
                            sfSchema.put(field.get("name"), "NUMBER");
                        } else {
                            sfSchema.put(field.get("name"), "BIGINT");
                        }
                    } else {
                        sfSchema.put(field.get("name"), "BIGINT");
                    }
                    break;
                case "INT96":
                case "FIXED_LEN_BYTE_ARRAY(12)":
                    sfSchema.put(field.get("name"), "TIMESTAMP_NTZ");
                    break;
                case "FLOAT":
                    sfSchema.put(field.get("name"), "FLOAT");
                    break;
                case "DOUBLE":
                    sfSchema.put(field.get("name"), "DOUBLE");
                    break;
                case "BYTE_ARRAY":
                    if (field.get("logicalType") != null && field.get("logicalType").contains("STRING")) {
                        sfSchema.put(field.get("name"), "VARCHAR");
                    } else {
                        sfSchema.put(field.get("name"), "BINARY");
                    }
                    break;
                case "FIXED_LEN_BYTE_ARRAY":
                    if (field.get("logicalType") != null && field.get("logicalType").equals("DECIMAL")) {
                        sfSchema.put(field.get("name"), "NUMBER");
                    } else {
                        sfSchema.put(field.get("name"), "BINARY");
                    }
                    break;
                default:
                    sfSchema.put(field.get("name"), "VARCHAR");
                    break;
            }
        }
        return sfSchema;
    }

    public static Map<String, String> parquetSchemaToSfSchema(MessageType parquetSchema) {
        List<Type> fields = parquetSchema.getFields();
        List<Map<String, String>> schema = new ArrayList<>();
        for (Type type : fields) {
            Map<String, String> fieldMetadata = new LinkedHashMap<>();
            fieldMetadata.put("name", type.getName());
            fieldMetadata.put("physicalType", type.asPrimitiveType().toString().split(" ")[1].toUpperCase());
            fieldMetadata.put("logicalType", String.valueOf(type.getLogicalTypeAnnotation()).toUpperCase());
            schema.add(fieldMetadata);
        }
        return parquetSchemaToSfSchema(schema);
    }

    public static Map<String, String> avroSchemaToSfSchema(org.apache.avro.Schema avroSchema) {
        Configuration config = new Configuration();
        config.setBoolean(READ_INT96_AS_FIXED, true);
        MessageType parquetSchema = new AvroSchemaConverter(config).convert(avroSchema);
        return parquetSchemaToSfSchema(parquetSchema);
    }
}
