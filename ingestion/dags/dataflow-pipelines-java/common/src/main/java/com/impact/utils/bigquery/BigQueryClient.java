package com.impact.utils.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.*;

@Slf4j
public class BigQueryClient {
    protected BigQuery bqClient;
    protected String projectId;

    public BigQueryClient(String projectId) {
        this.projectId = projectId;
        this.bqClient = BigQueryOptions.newBuilder().setProjectId(this.projectId).build().getService();
        log.info(String.format("BigQuery Client Initialized for %s", projectId));
    }

    public BigQueryClient(String projectId, String region) {
        this.projectId = projectId;
        this.bqClient = BigQueryOptions.newBuilder().setProjectId(this.projectId).setLocation(region).build().getService();
        log.info(String.format("BigQuery Client Initialized for %s", projectId));
    }

    public Map<String, Map<String, String>> getTableSchemaV1(String dataset, String table)  {
        try {
            TableId tableId = TableId.of(this.projectId, dataset, table);
            Table tableObj = this.bqClient.getTable(tableId);
            //Schema tableSchema = tableObj.getDefinition().getSchema();
            //FieldList fields = tableSchema.getFields();
            Map<String, Map<String, String>> schema = new LinkedHashMap<>();
            for (Field field : tableObj.getDefinition().getSchema().getFields()) {
                Map<String, String> fieldMetaData = new LinkedHashMap<>();
                fieldMetaData.put("dtype", this.legacySqlTypeToStr(field.getType()));
                fieldMetaData.put("mode", field.getMode()!=null ? field.getMode().name() : null);
                schema.put(field.getName(), fieldMetaData);
            }
            return schema;
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in BigQueryClient.getTableSchema %s", ExceptionUtils.getStackTrace(e)));
            return new HashMap<>();
        }

    }

    public Map<String, Map<String, String>> getTableSchema(String dataset, String table) {
        try {
            Map<String, Map<String, String>> schema = new LinkedHashMap<>();
            String query = String.format("SELECT column_name, data_type, is_nullable FROM %s.INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE table_name='%s' ORDER BY ordinal_position", dataset, table);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            TableResult result = this.bqClient.query(queryConfig);
            Iterable<FieldValueList> rows = result.getValues();
            for (FieldValueList row : rows) {
                Map<String, String> fieldMetaData = new LinkedHashMap<>();
                fieldMetaData.put("dtype", row.get("data_type").getStringValue());
                fieldMetaData.put("mode", row.get("is_nullable").getStringValue().equals("YES") ? "NULLABLE" : "REQUIRED");
                schema.put(row.get("column_name").getStringValue(), fieldMetaData);
            }
            return schema;
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in BigQueryClient.getTableSchema %s", ExceptionUtils.getStackTrace(e)));
            return new HashMap<>();
        }
    }

    public TableSchema getSchemaAsTableSchema(String project, String dataset, String table) {
        List<TableFieldSchema> fieldList = new ArrayList<>();
        TableSchema tableSchema = new TableSchema();
        try {
            String query = String.format("SELECT column_name, data_type, is_nullable FROM %s.%s.INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE table_name='%s' ORDER BY ordinal_position", project, dataset, table);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            TableResult result = this.bqClient.query(queryConfig);
            Iterable<FieldValueList> rows = result.getValues();
            for (FieldValueList row : rows) {
                fieldList.add(new TableFieldSchema().setName(row.get("column_name").getStringValue())
                        .setType(row.get("data_type").getStringValue()));
            }
            tableSchema.setFields(fieldList);
            return tableSchema;
        }
        catch (Exception e) {
            log.error(String.format("Exception occured in BigQueryClient.getTableSchema %s", ExceptionUtils.getStackTrace(e)));
            return tableSchema.setFields(fieldList);
        }
    }

    public void createTable(String projectId, String datasetName, String tableName, TableSchema tableSchema, String partitionCol, String clusteringColumns, boolean replace) throws Exception {
        try {
            StringBuilder query = new StringBuilder("");
            if(replace) {
                if(partitionCol!=null && !"null".equals(partitionCol.toLowerCase())) {
                    query.append(String.format("DROP TABLE IF EXISTS %s.%s.%s;\n", projectId, datasetName, tableName));
                    query.append(String.format("CREATE OR REPLACE TABLE %s.%s.%s\n(\n", projectId, datasetName, tableName));
                }
                else {
                    query.append(String.format("CREATE OR REPLACE TABLE %s.%s.%s\n(\n", projectId, datasetName, tableName));
                }
            }
            else {
                query.append(String.format("CREATE TABLE IF NOT EXISTS %s.%s.%s\n(\n", projectId, datasetName, tableName));
            }
//            StringBuilder query = new StringBuilder("CREATE " + (replace ? " OR REPLACE " : " ") + " TABLE "
//                    + String.format("%s.%s.%s", projectId, datasetName, tableName) + "\n(\n");
            TableId tableId = TableId.of(projectId, datasetName, tableName);
            List<TableFieldSchema> tableFieldSchemaList = tableSchema.getFields();
            for (int i = 0; i < tableFieldSchemaList.size(); i++) {
                if (i != tableFieldSchemaList.size() - 1) {
                    query.append("`"+tableFieldSchemaList.get(i).getName()+"`" + " " + tableFieldSchemaList.get(i).getType() + ",\n");
                } else {
                    query.append("`"+tableFieldSchemaList.get(i).getName()+"`" + " " + tableFieldSchemaList.get(i).getType() + "\n");
                }
            }
            query.append("\n)\n");
            if (partitionCol != null && !"null".equals(partitionCol.toLowerCase())) {
                query.append(String.format("PARTITION BY DATE(%s)\n", partitionCol));
            }
            if (clusteringColumns != null && !"null".equals(clusteringColumns.toLowerCase())) {
                query.append(String.format("CLUSTER BY %s\n", clusteringColumns));
            }
            log.info(String.format("Executing DDL:\n%s", query));
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(String.valueOf(query)).build();
            TableResult result = this.bqClient.query(queryConfig);

        }
        catch (Exception e) {
            throw e;
        }
    }

    public TableResult executeQuery(String query) {
        TableResult result = null;
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            log.info(String.format("Executing Query %s", query));
            result = this.bqClient.query(queryConfig);
            return result;

        } catch (Exception e) {
            log.error(String.format("Exception occured in BigQueryClient.executeQuery %s", ExceptionUtils.getStackTrace(e)));
            throw new RuntimeException(e);
        }
    }

    public String legacySqlTypeToStr(LegacySQLTypeName type){
        if (type.equals(LegacySQLTypeName.BOOLEAN)) {
            return "BOOLEAN";
        }
        else if(type.equals(LegacySQLTypeName.STRING)) {
            return "STRING";
        }
        else if(type.equals(LegacySQLTypeName.FLOAT)) {
            return "FLOAT";
        }
        else if(type.equals(LegacySQLTypeName.INTEGER)) {
            return "INTEGER";
        }
        else if(type.equals(LegacySQLTypeName.NUMERIC)) {
            return "NUMERIC";
        }
        else if(type.equals(LegacySQLTypeName.BIGNUMERIC)) {
            return "BIGNUMERIC";
        }
        else if(type.equals(LegacySQLTypeName.DATE)) {
            return "DATE";
        }
        else if(type.equals(LegacySQLTypeName.DATETIME)) {
            return "DATETIME";
        }
        else if(type.equals(LegacySQLTypeName.TIMESTAMP)) {
            return "TIMESTAMP";
        }
        else if(type.equals(LegacySQLTypeName.TIME)) {
            return "TIME";
        }
        else {
            log.warn(String.format("LegacySQLTypeName type %s did not match with pre-configured types. Hence " +
                    "default STRING type is considered", type));
            return "STRING";
        }
    }

    public Schema getSchemaFromQuery(String query) throws Exception {
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).setDryRun(true).setUseQueryCache(false).build();
        Job job = this.bqClient.create(JobInfo.of(queryConfig));
        JobStatistics.QueryStatistics queryStats =  job.getStatistics();
        return queryStats.getSchema();
    }

    public Map<String, Map<String, String>> convertSchemaToMap(Schema schema) {
        Map<String, Map<String, String>> schemaMap = new LinkedHashMap<>();
        FieldList fieldList = schema.getFields();
        for(Field f : fieldList) {
            Map<String, String> fieldMap = new LinkedHashMap<>();
            fieldMap.put("dtype", this.legacySqlTypeToStr(f.getType()));
            fieldMap.put("mode", f.getMode()!=null ? f.getMode().name() : null);
            schemaMap.put(f.getName(), fieldMap);
        }
        return schemaMap;
    }

}
