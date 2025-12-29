package com.impact.utils.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;

@Slf4j
public class BigQuerySchemaConversion {

    public static JSONObject getSchemaMappingForDb(String dbType) throws Exception {
        String jsonStr = readFromInputStream(BigQuerySchemaConversion.class
                .getResourceAsStream("/json/bq_to_db_dtypes_map.json"));
        JSONObject schemaMapping = new JSONObject(jsonStr);
        return schemaMapping.getJSONObject(dbType);
    }

    public static JSONObject getSchemaMappingForBq(String dbType) throws Exception {
        String jsonStr = readFromInputStream(BigQuerySchemaConversion.class
                .getResourceAsStream("/json/db_to_bq_dtypes_map.json"));
        JSONObject schemaMapping = new JSONObject(jsonStr);
        return schemaMapping.getJSONObject(dbType);
    }

    public static Map<String, String> bqToDb(Map<String, Map<String,String>> bqSchema, String dbType) throws Exception {
        JSONObject schemaMap = getSchemaMappingForDb(dbType);
        log.info(String.format("Schema Map %s", schemaMap.toString()));
        Map<String, String> dbSchema = new LinkedHashMap<>();
        String bqType = null;
        String mode = null;
        for(String field : bqSchema.keySet()) {
            bqType = bqSchema.get(field).get("dtype");
            mode = bqSchema.get(field).get("mode");
            if(bqType.startsWith("ARRAY")) {
                Pattern pattern = Pattern.compile("ARRAY<([^>]+)>");
                Matcher matcher = pattern.matcher(bqType);
                if(matcher.find()) {
                    dbSchema.put(field, schemaMap.getString(matcher.group(1))+"[]");
                }
                else {
                    throw new Exception(String.format("The %s has datatype as %s. Unable to extract the primitive" +
                            " datatype from this. For example, in ARRAY<INT64>, the primitive type is INT64", field, bqType));
                }
            }
            else {
                dbSchema.put(field, schemaMap.getString(bqType));
            }
        }
        return dbSchema;
    }

    public static Map<String, String> bqToDb(Map<String, Map<String,String>> bqSchema, String userProvidedDbSchema,
                                             String dbType) throws Exception {
        JSONObject schemaMap = getSchemaMappingForDb(dbType);
        log.info(String.format("Schema Map %s", schemaMap.toString()));
        Map<String, String> dbSchema = new LinkedHashMap<>();
        String bqType = null;
        String mode = null;
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> userProvidedSchema = mapper.readValue(userProvidedDbSchema, Map.class);
        for(String field : bqSchema.keySet()) {
            if(userProvidedSchema.containsKey(field)) {
                dbSchema.put(field, userProvidedSchema.get(field));
            }
            else {
                bqType = bqSchema.get(field).get("dtype");
                mode = bqSchema.get(field).get("mode");
                if(bqType.startsWith("ARRAY")) {
                    Pattern pattern = Pattern.compile("ARRAY<([^>]+)>");
                    Matcher matcher = pattern.matcher(bqType);
                    if(matcher.find()) {
                        dbSchema.put(field, schemaMap.getString(matcher.group(1))+"[]");
                    }
                    else {
                        throw new Exception(String.format("The %s has datatype as %s. Unable to extract the primitive" +
                                " datatype from this. For example, in ARRAY<INT64>, the primitive type is INT64", field, bqType));
                    }
                }
                else {
                    dbSchema.put(field, schemaMap.getString(bqType));
                }
            }
        }
        return dbSchema;
    }

    public static Map<String, String> dbToBq(Map<String, String> dbSchema, String dbType) throws Exception {
        JSONObject schemaMap = getSchemaMappingForBq(dbType);
        log.info(String.format("Schema Map %s", schemaMap.toString()));
        Map<String, String> bqSchema = new LinkedHashMap<>();
        for(String field : dbSchema.keySet()) {
            dbType = dbSchema.get(field);
            bqSchema.put(field.replaceAll("[^0-9A-Za-z_]",""), schemaMap.getString(dbType.toUpperCase()));
        }
        return bqSchema;
    }

    public static TableSchema convertMapToTableSchema(Map<String, String> schema) {
        TableSchema tableSchema = new TableSchema();
        List<TableFieldSchema> fieldList = new ArrayList<>();
        for(String col : schema.keySet()) {
            fieldList.add(new TableFieldSchema().setName(col).setType(schema.get(col)));
        }
        tableSchema.setFields(fieldList);
        return tableSchema;
    }

    private static String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    public static Map<String, String> parquetSchemaToBqSchema(List<Map<String, String>> parquetSchema) {
        Map<String, String> bqSchema = new LinkedHashMap<String, String>();
        List<Map<String, String>> fields = parquetSchema;
        Pattern decimalWithoutFractionPattern = Pattern.compile("DECIMAL\\(\\d+,0\\)");
        Pattern decimalWithFractionPattern = Pattern.compile("DECIMAL\\(\\d+,\\d+\\)");
        for(Map<String, String> field : fields) {
            switch (field.get("physicalType")) {
                case "BOOLEAN":
                case "BOOL":
                    bqSchema.put(field.get("name"), "BOOLEAN");
                    break;
                case "INT32":
                    if(field.get("logicalType").startsWith("DATE")) {
                        bqSchema.put(field.get("name"), "DATE");
                        break;
                    }
                    else if(field.get("logicalType").startsWith("DECIMAL")) {
                        bqSchema.put(field.get("name"), "NUMERIC");
                        break;
                    }
                    else {
                        bqSchema.put(field.get("name"), "INTEGER");
                        break;
                    }
                case "INT64":
                    if(field.get("logicalType").startsWith("TIMESTAMP")) {
                        bqSchema.put(field.get("name"), "TIMESTAMP");
                        break;
                    }
                    else if(field.get("logicalType").startsWith("DECIMAL")) {
                        bqSchema.put(field.get("name"), "NUMERIC");
                        break;
                    }
                    else {
                        bqSchema.put(field.get("name"), "INTEGER");
                        break;
                    }
                case "INT96":
                case "FIXED_LEN_BYTE_ARRAY(12)":
                    bqSchema.put(field.get("name"), "TIMESTAMP");
                    break;
                case "FLOAT":
                    bqSchema.put(field.get("name"), "FLOAT64");
                    break;
                case "DOUBLE":
                    bqSchema.put(field.get("name"), "FLOAT64");
                    break;
                case "BYTE_ARRAY":
                    if(field.get("logicalType").startsWith("STRING")) {
                        bqSchema.put(field.get("name"), "STRING");
                        break;
                    }
                    else {
                        bqSchema.put(field.get("name"), "BYTES");
                        break;
                    }
                case "FIXED_LEN_BYTE_ARRAY":
                    if(field.get("logicalType").startsWith("DECIMAL")) {
                        bqSchema.put(field.get("name"), "NUMERIC");
                        break;
                    }
                    else {
                        bqSchema.put(field.get("name"), "BYTES");
                        break;
                    }
                default:
                    if(field.get("physicalType").contains("FIXED_LEN_BYTE_ARRAY")) {
                        if(decimalWithoutFractionPattern.matcher(field.get("logicalType")).matches()) {
                            bqSchema.put(field.get("name"), "INTEGER");
                        }
                        else if(decimalWithFractionPattern.matcher(field.get("logicalType")).matches()) {
                            bqSchema.put(field.get("name"), "NUMERIC");
                        }
                        else {
                            bqSchema.put(field.get("name"), "STRING");
                        }
                    }
                    else {
                        bqSchema.put(field.get("name"), "STRING");
                    }
                    //bqSchema.put(field.get("name"), "STRING");
                    break;
            }
        }
//        String physicalType = null;
//        for(Map<String, String> field : fields) {
//            physicalType = field.get("physicalType");
//            if(physicalType.equals(""))
//        }
        return bqSchema;
    }

    public static Map<String, String> parquetSchemaToBqSchema(MessageType parquetSchema) {
        List<Type> fields = parquetSchema.getFields();
        List<Map<String, String>> schema = new ArrayList<Map<String, String>>();
        log.info("Parquet Schema\nNAME\tPhysical Type\tLogical Type");
        for(Type type : fields) {
            Map<String, String> fieldMetadata = new LinkedHashMap<String, String>();
            fieldMetadata.put("name", type.getName());
            fieldMetadata.put("physicalType", type.asPrimitiveType().toString().split(" ")[1].toUpperCase());
            fieldMetadata.put("logicalType", String.valueOf(type.getLogicalTypeAnnotation()).toUpperCase());
            //log.info(t.getName() + "// " + t.asPrimitiveType() + "//" + t.getLogicalTypeAnnotation());
            log.info(type.getName() + "<<<<//>>>>" + type.asPrimitiveType().toString().split(" ")[1].toUpperCase()
                    + "<<<<//>>>>>" + String.valueOf(type.getLogicalTypeAnnotation()).toUpperCase());
            schema.add(fieldMetadata);
        }
        return parquetSchemaToBqSchema(schema);
    }

    public static Map<String, String> avroSchemaToBqSchema(Schema avroSchema) {
        Map<String, String> bqSchema = new LinkedHashMap<String, String>();
        Configuration config = new Configuration();
        config.setBoolean(READ_INT96_AS_FIXED, true);
        MessageType parquetSchema = new AvroSchemaConverter(config).convert(avroSchema);
        bqSchema = parquetSchemaToBqSchema(parquetSchema);
        return bqSchema;
    }

}
