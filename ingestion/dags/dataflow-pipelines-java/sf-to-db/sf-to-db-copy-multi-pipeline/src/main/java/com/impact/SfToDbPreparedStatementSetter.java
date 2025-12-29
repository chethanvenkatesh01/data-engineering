package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
public class SfToDbPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<TableRow> {

    private static final Logger log = LoggerFactory.getLogger(SfToDbPreparedStatementSetter.class);
    private final Map<String, String> dbSchema;
    private final String dbType;

    private final Counter totalOutputRowsCounter = Metrics.counter(SfToDbPreparedStatementSetter.class, "totalOutputRows");

    public SfToDbPreparedStatementSetter(Map<String, String> dbSchema, String dbType) {
        this.dbSchema = dbSchema;
        this.dbType = dbType;
    }

    @Override
    public void setParameters(TableRow element, PreparedStatement preparedStatement) throws Exception {
        String fieldType = null;
        Set<String> keys = dbSchema.keySet();
        String[] colNames = keys.toArray(new String[0]);
        for(int i=0; i<colNames.length; i++) {
            fieldType = dbSchema.get(colNames[i]).toUpperCase();
            log.info(String.valueOf(element));
            switch (fieldType) {
                case "BOOLEAN":
                case "BOOL":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.BOOLEAN);
                    break;
                case "BOOLEAN[]":
                case "BOOL[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("BOOLEAN",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "BIGINT":
                case "INT8":
                case "SERIAL8":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.BIGINT);
                    break;
                case "BIGINT[]":
                case "INT8[]":
                case "SERIAL8[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("BIGINT",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "TEXT":
                case "VARCHAR":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.VARCHAR);
                    break;
                case "TEXT[]":
                case "VARCHAR[]":
//                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("VARCHAR",
//                            element.get(colNames[i])!=null ?  ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    String[] cleanedInput = String.valueOf(element.get(colNames[i])).replaceAll("\\[|\\]|'\"", "").split("\\s*,\\s*");;
                    preparedStatement.setObject(i+1, preparedStatement.getConnection().createArrayOf("VARCHAR",cleanedInput));
                    break;
                case "DOUBLE PRECISION":
                case "FLOAT8":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.DOUBLE);
                    break;
                case "DOUBLE PRECISION[]":
                case "FLOAT8[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("DOUBLE",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "NUMERIC":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.NUMERIC);
                    break;
                case "NUMERIC[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("NUMERIC",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "DATE":
                    preparedStatement.setObject(i+1, element.get(colNames[i])!=null ?
                            String.valueOf(element.get(colNames[i])).replaceAll("T"," ") : element.get(colNames[i]), Types.DATE);
                    break;
                case "DATE[]":
                    if(element.get(colNames[i]) != null) {
                        Object[] values = ((ArrayList<Object>) element.get(colNames[i])).toArray();
                        for(int x=0; x<values.length; x++) {
                            values[x] = String.valueOf(values[x]).replaceAll("T"," ");
                        }
                        preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("DATE", values));
                    }
                    else {
                        preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("DATE", null));
                    }
                    break;
                case "TIMESTAMP":
                case "TIMESTAMP WITHOUT TIME ZONE":
                    preparedStatement.setObject(i+1, element.get(colNames[i])!=null ?
                            String.valueOf(element.get(colNames[i])).replaceAll("T"," ") : element.get(colNames[i]), Types.TIMESTAMP);
                    break;
                case "TIMESTAMP[]":
                case "TIMESTAMP WITHOUT TIME ZONE[]":
                    if(element.get(colNames[i]) != null) {
                        Object[] values = ((ArrayList<Object>) element.get(colNames[i])).toArray();
                        for(int x=0; x<values.length; x++) {
                            values[x] = String.valueOf(values[x]).replaceAll("T"," ");
                        }
                        preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("TIMESTAMP", values));
                    }
                    else {
                        preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("TIMESTAMP", null));
                    }
                    break;
                case "TIMETZ":
                case "TIMESTAMP WITH TIME ZONE":
//                    log.info((String) element.get(colNames[i]));
//                    String trimmed_value=StringUtils.trim(String.valueOf(element.get(colNames[i])));
//                    preparedStatement.setObject(i+1, element.get(colNames[i])!=null ?
//                            trimmed_value.replace("T"," ").replace("Z"," ") : element.get(colNames[i]), Types.TIMESTAMP_WITH_TIMEZONE);
//                    log.info(trimmed_value.replace("T"," ").replace("Z"," "));
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS X");
                    OffsetDateTime offsetDateTime = OffsetDateTime.parse((String) element.get(colNames[i]), formatter);
                    preparedStatement.setObject(i+1, element.get(colNames[i])!=null ?
                            offsetDateTime : element.get(colNames[i]));


                    break;
                case "TIMETZ[]":
                case "TIMESTAMP WITH TIME ZONE[]":
                    if(element.get(colNames[i]) != null) {
                        Object[] values = ((ArrayList<Object>) element.get(colNames[i])).toArray();
                        for(int x=0; x<values.length; x++) {
                            values[x] = String.valueOf(values[x]).replaceAll("T"," ");
                        }
                        preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("TIMESTAMP WITH TIMEZONE", values));
                    }
                    else {
                        preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("TIMESTAMP WITH TIMEZONE", null));
                    }
                    break;
                case "TIMESTAMPTZ":
                case "TIME WITHOUT TIME ZONE":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.TIME);
                    break;
                case "TIMESTAMPTZ[]":
                case "TIME WITHOUT TIME ZONE[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("TIME WITH TIMEZONE",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "FLOAT":
                case "FLOAT4":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.FLOAT);
                    break;
                case "FLOAT[]":
                case "FLOAT4[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("FLOAT",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "INT":
                case "INT2":
                case "INT4":
                case "INTEGER":
                case "SERIAL2":
                case "SERIAL4":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.INTEGER);
                    break;
                case "INT[]":
                case "INT2[]":
                case "INT4[]":
                case "INTEGER[]":
                case "SERIAL2[]":
                case "SERIAL4[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("INTEGER",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "CHAR":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.CHAR);
                    break;
                case "CHAR[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("CHAR",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                case "DECIMAL":
                    preparedStatement.setObject(i+1, element.get(colNames[i]), Types.DECIMAL);
                    break;
                case "DECIMAL[]":
                    preparedStatement.setArray(i+1, preparedStatement.getConnection().createArrayOf("DECIMAL",
                            element.get(colNames[i])!=null ? ((ArrayList<Object>) element.get(colNames[i])).toArray() : null));
                    break;
                default:
                    log.error(String.format("fieldType %s did not match with any given types", fieldType));
            }

        }
        totalOutputRowsCounter.inc();

    }


    public Map<String, Integer> getSqlTypesForSnowflake() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("BOOLEAN", Types.BOOLEAN);
        map.put("BIGINT", Types.BIGINT);
        map.put("TEXT", Types.VARCHAR);
        map.put("DOUBLE", Types.DOUBLE);
        map.put("NUMERIC", Types.NUMERIC);
        map.put("DATE", Types.DATE);
        map.put("TIMESTAMP", Types.TIMESTAMP);
        return map;
    }
}
