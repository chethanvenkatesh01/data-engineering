package com.impact;
import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;
import com.google.api.services.bigquery.model.TableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import java.sql.ResultSet;
import java.util.Map;
import java.util.HashMap;
import com.fasterxml.jackson.databind.ObjectMapper;


@Slf4j
public class MapToTableRow implements JdbcIO.RowMapper<TableRow> {

    public Map<String, String> dbSchema;
    public Map<String, String> sfSchema;  // Snowflake schema map
    public String dbType;
    public boolean replaceSpecialChars;
    private Counter totalOutputRowsCounter = Metrics.counter(MapToTableRow.class, "totalOutputRows");

    private static final Map<Character, String> replacements = new HashMap<>();

    static {
        // Populate character replacements for special chars
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

    public MapToTableRow(Map<String, String> dbSchema, Map<String, String> sfSchema, String dbType, boolean replaceSpecialChars) {
        this.dbSchema = dbSchema;
        this.sfSchema = sfSchema;  // Snowflake schema passed in the constructor
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
        ObjectMapper objectMapper = new ObjectMapper();

        // Get column names from the Snowflake schema
        String[] dbColNames = dbSchema.keySet().toArray(new String[0]);

        for (String col : dbColNames) {
            String colValue = null;
            try {
                colValue = resultSet.getString(col); // Retrieve the column value
                String colName = col.replaceAll("[^0-9A-Za-z_]", ""); // Clean column name to match schema
                String colSchemaType = sfSchema.getOrDefault(colName, "string").toLowerCase(); // Use Snowflake schema

                // Check for actual null values and set them directly
                if (resultSet.wasNull()) {
                    tr.set(colName, null); // Explicitly set null for empty columns
                    continue;
                }

                // Process non-null values based on Snowflake schema type
                switch (colSchemaType) {
                    case "timestamp":
                    case "datetime":
                        if (colValue.length() > 15) {
                            colValue = colValue.substring(0, 15); // Truncate timestamp if needed
                        }
                        tr.set(colName, colValue);
                        break;

                    case "variant":
                        // Handle Snowflake's variant type as a JSON string or directly as needed
                        tr.set(colName, colValue);
                        break;

                    case "array":
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
                            colValue = preprocessString(colValue); // Replace special characters if needed
                        }
                        tr.set(colName, colValue);
                        break;

                    case "int":
                    case "number":
                    case "decimal":
                        tr.set(colName, Integer.parseInt(colValue)); // Convert numeric values
                        break;

                    default:
                        tr.set(colName, colValue); // For other types, set the value directly
                        break;
                }
            } catch (Exception e) {
                // Log the column name, schema type, and the problematic value
               log.info(String.format(
                        "Error processing column: %s, Value: %s, Schema Type: %s, Error: %s%n",
                        col,
                        colValue,
                        sfSchema.getOrDefault(col, "unknown"),
                        e.getMessage()
                ));
                throw e; // Re-throw the exception after logging
            }
        }

        // Increment the counter for rows processed
        totalOutputRowsCounter.inc();

        return tr;
    }


}
