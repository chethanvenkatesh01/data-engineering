package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.postgresql.jdbc.TimestampUtils;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.JulianFields;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.math.BigInteger;
import java.math.BigDecimal;


@Slf4j
public class MapGenericRecordToTableRow extends DoFn<GenericRecord, TableRow> {
    protected Map<String, String> bqSchema;
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
        this.bqSchema = schema;
        return this;
    }

    public MapGenericRecordToTableRow withReplaceSpecialCharacters(boolean replaceSpecialCharacters) {
        this.replaceSpecialCharacters = replaceSpecialCharacters;
        return this;
    }

    @ProcessElement
    public void processElement(@Element GenericRecord record, ProcessContext context) {
        // Set<String> fields = bqSchema.keySet();
        TableRow tr = new TableRow();
        // for(String field : fields) {
        for (Map.Entry<String, String> entry : bqSchema.entrySet()) {
            String field = entry.getKey();
            Object value = record.get(field);
            if (value != null) {
                switch (entry.getValue().toUpperCase()) {
                    case "TIMESTAMP":
                        if (value instanceof GenericData.Fixed) {
                            value = convertByteArrToDateStr(((GenericData.Fixed) value).bytes());
                        }
                        else if(value.getClass().getCanonicalName().equals("java.lang.Long")) {
                            //log.info(value.toString());
                            //log.info(Instant.ofEpochMilli(Long.parseLong(value.toString())).toString());
                            //value = convertNumberOfDaysSinceEpochToDate(Long.parseLong(value.toString()));
                            value = Instant.ofEpochMilli(Long.parseLong(value.toString())).toString();
                        }
                        else {
                            value = value.toString();
                        }
                        break;
                    case "STRING":
                        value = value.toString();
                        if (replaceSpecialCharacters) {
                            value = preprocessStringField((String) value);
                        }
                        break;
                    case "DATE":
                        value = convertNumberOfDaysSinceEpochToDate(Long.parseLong(value.toString()));
                        break;
                    case "DATETIME":
                        value = value.toString();
                        break;
                    case "NUMERIC":
                        if (value instanceof GenericData.Fixed) {
                            value = convertByteArrToBigDecimalStr(((GenericData.Fixed) value).bytes());
                        }
                        break;
                    case "INTEGER":
                        if (value instanceof GenericData.Fixed) {
                            value = convertByteArrayToInt(((GenericData.Fixed) value).bytes());
                        }
                        break;
                    // Other cases as needed
                }
            }

            if (value != null) {
                tr.set(field, value);
            }
        }
        totalOutputRowsCounter.inc();
        context.output(tr);
    }

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

        // Compute Julian day and nanoseconds in a single pass
        for (int i = byteArr.length - 1; i >= 0; i--) {
            if (i >= 8) {
                julianDay = (julianDay << 8) + (byteArr[i] & 0xFF);
            } else {
                nanos = (nanos << 8) + (byteArr[i] & 0xFF);
            }
        }

        // Convert to LocalDateTime
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

    public double convertByteArrayToFloat(byte[] byteArr) {
        //int intValue = new BigInteger(byteArr).intValue();
        //return Float.intBitsToFloat(intValue);
        return ByteBuffer.wrap(byteArr).getDouble();
    }
}
