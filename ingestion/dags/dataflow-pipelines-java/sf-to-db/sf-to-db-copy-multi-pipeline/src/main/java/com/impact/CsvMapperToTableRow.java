package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CsvMapperToTableRow implements SnowflakeIO.CsvMapper<TableRow> {

    // Snowflake schema mapping: column name -> data type
    private Map<String, String> sfSchema;
    private boolean replaceSpecialChars = false;

    public static CsvMapperToTableRow builder() {
        return new CsvMapperToTableRow();
    }

    private Counter totalOutputRowsCounter = Metrics.counter(CsvMapperToTableRow.class, "totalOutputRows");

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

    public CsvMapperToTableRow withSfSchema(Map<String, String> sfSchema) {
        this.sfSchema = sfSchema;
        return this;
    }

    public CsvMapperToTableRow withReplaceSpecialChars(boolean replaceSpecialChars) {
        this.replaceSpecialChars = replaceSpecialChars;
        return this;
    }

    private String preprocessStringField(String colValue) {
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
    public TableRow mapRow(String[] parts) throws Exception {
        TableRow tr = new TableRow();

        if (sfSchema == null || sfSchema.isEmpty()) {
            throw new Exception("Snowflake schema (sfSchema) is not set or is empty.");
        }

        int i = 0;
        for (String col : sfSchema.keySet()) {
            if (i >= parts.length || parts[i] == null) {
                i++;
                continue;
            }

            String fieldType = sfSchema.get(col);

            switch (fieldType.toUpperCase()) {
                case "TEXT":
                case "STRING":
                case "VARCHAR":
                case "CHAR":
                case "CHARACTER":
                    if (!parts[i].equals("IA_NULL")) {
                        tr.set(col, replaceSpecialChars ? preprocessStringField(parts[i]) : parts[i]);
                    }
                    break;

                case "NUMBER":
                case "DECIMAL":
                case "NUMERIC":
                case "INTEGER":
                case "BIGINT":
                case "INT":
                case "SMALLINT":
                case "TINYINT":
                    if (!parts[i].equals("N")) {
                        tr.set(col, Double.valueOf(parts[i]));
                    }
                    break;

                case "FLOAT":
                case "FLOAT4":
                case "FLOAT8":
                case "DOUBLE":
                case "DOUBLE PRECISION":
                case "REAL":
                    if (!parts[i].equals("N")) {
                        tr.set(col, Double.valueOf(parts[i]));
                    }
                    break;

                case "BOOLEAN":
                    if (!parts[i].equals("N")) {
                        tr.set(col, Boolean.valueOf(parts[i]));
                    }
                    break;

                case "DATE":
                    if (!parts[i].equals("N")) {
                        tr.set(col, parts[i]); // Expected ISO-8601 format
                    }
                    break;

                case "TIME":
                case "TIMESTAMP":
                case "TIMESTAMPTZ":
                case "TIMESTAMP_NTZ":
                case "TIMESTAMP_TZ":
                case "TIMESTAMP_LTZ":
                case "TIMESTAMPNTZ":
                    if (!parts[i].equals("N")) {
                        tr.set(col, parts[i]);
                    }
                    break;

                case "VARIANT":
                case "OBJECT":
                case "ARRAY":
                    if (!parts[i].equals("N")) {
                        tr.set(col, parts[i]);
                    }
                    break;

                case "BINARY":
                    if (!parts[i].equals("N")) {
                        tr.set(col, parts[i]);
                    }
                    break;

                case "GEOGRAPHY":
                    if (!parts[i].equals("N")) {
                        tr.set(col, parts[i]);
                    }
                    break;

                default:
                    throw new Exception(String.format("Unsupported Snowflake data type: %s for column %s", fieldType, col));
            }

            i++;
        }

        totalOutputRowsCounter.inc();
        return tr;
    }

}
