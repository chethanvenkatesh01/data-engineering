package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.JulianFields;
import java.util.Map;
import java.util.HashMap;

@Slf4j
public class MapGenericRecordToTableRow extends DoFn<GenericRecord, TableRow> {
    protected Map<String, String> sfSchema;
    protected boolean replaceSpecialCharacters = false;

    private Counter totalOutputRowsCounter = Metrics.counter(MapGenericRecordToTableRow.class, "totalOutputRows");

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

    public static MapGenericRecordToTableRow builder() {
        return new MapGenericRecordToTableRow();
    }

    public MapGenericRecordToTableRow withSchema(Map<String, String> schema) {
        this.sfSchema = schema;
        return this;
    }

    public MapGenericRecordToTableRow withReplaceSpecialCharacters(boolean replaceSpecialCharacters) {
        this.replaceSpecialCharacters = replaceSpecialCharacters;
        return this;
    }

    @ProcessElement
    public void processElement(@Element GenericRecord record, ProcessContext context) {
        TableRow tr = new TableRow();

        for (Map.Entry<String, String> entry : sfSchema.entrySet()) {
            String field = entry.getKey();
            Object value = record.get(field);

            if (value != null) {
                switch (entry.getValue().toUpperCase()) {
                    case "TIMESTAMP_NTZ":
                    case "TIMESTAMP_LTZ":
                    case "TIMESTAMP_TZ":
                        if (value instanceof GenericData.Fixed) {
                            value = convertByteArrToDateStr(((GenericData.Fixed) value).bytes());
                        } else {
                            value = value.toString();
                        }
                        break;
                    case "VARCHAR":
                    case "STRING":
                    case "TEXT":
                    case "CHAR":
                        value = value.toString();
                        if (replaceSpecialCharacters) {
                            value = preprocessStringField((String) value);
                        }
                        break;
                    case "DATE":
                        value = convertNumberOfDaysSinceEpochToDate(Long.parseLong(value.toString()));
                        break;
                    case "NUMBER":
                    case "DECIMAL":
                    case "NUMERIC":
                        if (value instanceof Integer || value instanceof Long) {
                            value = value.toString();
                        }
                        else if (value instanceof Double || value instanceof Float) {
                            value = String.format("%.10f", value);
                        }
                        else if (value instanceof GenericData.Fixed) {
                            value = convertByteArrToBigDecimalStr(((GenericData.Fixed) value).bytes());
                        }
                        break;
                    case "FLOAT":
                        value = Double.parseDouble(value.toString());
                        break;
                    case "BOOLEAN":
                        value = Boolean.parseBoolean(value.toString());
                        break;
                    case "INTEGER":
                    case "BIGINT":
                        value = Long.parseLong(value.toString());
                        break;
                    case "BINARY":
                    case "VARBINARY":
                        if (value instanceof ByteBuffer) {
                            ByteBuffer bb = (ByteBuffer) value;
                            byte[] bytes = new byte[bb.remaining()];
                            bb.get(bytes);
                            value = bytes;
                        } else if (value instanceof byte[]) {
                            // Keep as is
                        } else {
                            value = value.toString().getBytes();
                        }
                        break;
                    default:
                        value = value.toString();
                }
            }

            if (value != null) {
                tr.set(field, value);
            }
        }
        totalOutputRowsCounter.inc();
        context.output(tr);
    }

    // Keeping existing utility methods
    public String preprocessStringField(String colValue) {
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

    public String convertByteArrToDateStr(byte[] byteArr) {
        int julianDay = 0;
        long nanos = 0;

        for (int i = byteArr.length - 1; i >= 0; i--) {
            if (i >= 8) {
                julianDay = (julianDay << 8) + (byteArr[i] & 0xFF);
            } else {
                nanos = (nanos << 8) + (byteArr[i] & 0xFF);
            }
        }

        LocalDateTime timestamp = LocalDate.MIN
                .with(JulianFields.JULIAN_DAY, julianDay)
                .atTime(LocalTime.MIDNIGHT)
                .plusNanos(nanos);

        return timestamp.toString();
    }

    public String convertNumberOfDaysSinceEpochToDate(long numDays) {
        return LocalDate.ofEpochDay(numDays).toString();
    }

    public String convertByteArrToBigDecimalStr(byte[] byteArr) {
        // double value = ByteBuffer.wrap(byteArr).getDouble();
        BigInteger bigIntValue = new BigInteger(byteArr);
        BigDecimal bigDecimalValue = new BigDecimal(bigIntValue); // Scale of 38 for BigQuery BIGNUMERIC
        return bigDecimalValue.toPlainString();
    }

    public long convertByteArrayToInt(byte[] byteArr) {
        return new BigInteger(byteArr).longValue();
    }
}