package com.impact;
import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;
import com.google.api.services.bigquery.model.TableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
// import com.impact.utils.bigquery.SpecialCharReplacer;

import java.sql.ResultSet;
import java.util.Map;
import java.util.HashMap;

@Slf4j
public class MapToTableRow implements JdbcIO.RowMapper<TableRow> {

    public Map<String, String> dbSchema;
    public Map<String, String> bqSchema;
    public String dbType;
    //public boolean allowSpecialChars;
    public boolean replaceSpecialChars;
    private Counter totalOutputRowsCounter = Metrics.counter(MapToTableRow.class, "totalOutputRows");

    private static final Map<Character, String> replacements = new HashMap<>();

    static {
        replacements.put('\'', "__ia_char_01");
        replacements.put('\"', "__ia_char_02");
        replacements.put('/', "__ia_char_03");
        replacements.put('\\', "__ia_char_04");
        replacements.put('`', "__ia_char_05");
        replacements.put('~', "__ia_char_06");
        replacements.put('!', "__ia_char_07");
        replacements.put('@', "__ia_char_08");
        replacements.put('#', "__ia_char_09");
        replacements.put('$', "__ia_char_10");
        replacements.put('%', "__ia_char_11");
        replacements.put('^', "__ia_char_12");
        replacements.put('&', "__ia_char_13");
        replacements.put('*', "__ia_char_14");
        replacements.put('(', "__ia_char_15");
        replacements.put(')', "__ia_char_16");
        replacements.put('=', "__ia_char_19");
        replacements.put('+', "__ia_char_20");
        replacements.put('{', "__ia_char_21");
        replacements.put('}', "__ia_char_22");
        replacements.put('[', "__ia_char_23");
        replacements.put(']', "__ia_char_24");
        replacements.put('|', "__ia_char_25");
        replacements.put(';', "__ia_char_27");
        replacements.put('<', "__ia_char_28");
        replacements.put('>', "__ia_char_29");
        replacements.put(',', "__ia_char_30");
        replacements.put('?', "__ia_char_32");
        replacements.put('\t', "__ia_char_33");
    }

    public MapToTableRow(Map<String, String> dbSchema, Map<String, String> bqSchema, String dbType, boolean replaceSpecialChars) {
        this.dbSchema = dbSchema;
        this.bqSchema = bqSchema;
        this.dbType = dbType;
        this.replaceSpecialChars = replaceSpecialChars;
    }

    public String preprocessString(String colValue) {
        StringBuilder result = new StringBuilder();
        for (char c : colValue.toCharArray()) {
            if (replacements.containsKey(c)) {
                result.append(replacements.get(c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {
        TableRow tr = new TableRow();
//        String[] bqColNames = bqSchema.keySet().toArray(new String[0]);
//        for (String col : bqColNames) {
//            String colValue = resultSet.getString(col);
//            if (colValue != null) {
//                if("time".equals(bqSchema.get(col).toLowerCase()) && colValue.length()>15) {
//                    colValue = colValue.substring(0,15);
//                }
//                tr.set(col, colValue);
//            }
//        }
        String[] dbColNames = dbSchema.keySet().toArray(new String[0]);
        for (String col : dbColNames) {
            String colValue = resultSet.getString(col);
            String colName = col.replaceAll("[^0-9A-Za-z_]", "");
            String colSchemaType = bqSchema.get(colName).toLowerCase();
            if (colValue != null) {
                switch (colSchemaType) {
                    case "time":
                        if (colValue.length() > 15) {
                            colValue = colValue.substring(0, 15);
                        }
                        tr.set(colName, colValue);
                        break;

                    case "array<string>":
                        if (colValue.startsWith("{") && colValue.endsWith("}")) {
                            colValue = colValue.substring(1, colValue.length() - 1); // Remove outer braces
                            List<String> formattedArray = Arrays.stream(colValue.split(","))
                                                                .map(String::trim)
                                                                .collect(Collectors.toList());
                            tr.set(colName, formattedArray); // Set as List<String> for BigQuery
                        }
                        break;

                    case "string":
                        if (this.replaceSpecialChars) {
                            colValue = preprocessString(colValue);
                        }
                        tr.set(colName, colValue);
                        break;

                    default:
                        tr.set(colName, colValue); // For any other types, set the value directly
                        break;
                }
            }
        }
        totalOutputRowsCounter.inc();
        return tr;
    }
}
