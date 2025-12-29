package com.impact;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import java.util.*;

@Slf4j
public class CsvMapperToTableRow implements SnowflakeIO.CsvMapper<TableRow> {

    //private TableSchema bqSchema;
    private Map<String, String> bqSchema;
    private boolean replaceSpecialChars = false;

    private Counter totalOutputRowsCounter = Metrics.counter(CsvMapperToTableRow.class, "totalOutputRows");

    public static CsvMapperToTableRow builder() {
        return new CsvMapperToTableRow();
    }

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
    
    public CsvMapperToTableRow withBqSchema(Map<String, String> bqSchema) {
        this.bqSchema = bqSchema;
        return this;
    }

    public CsvMapperToTableRow withReplaceSpecialChars(boolean replaceSpecialChars) {
        this.replaceSpecialChars = replaceSpecialChars;
        return this;
    }

    @Override
    public TableRow mapRow(String[] parts) throws Exception {
        TableRow tr = new TableRow();
        //log.info("$$$" + Arrays.toString(parts));
        //List<TableFieldSchema> columns = bqSchema.getFields();
        Set<String> columns = bqSchema.keySet();
        int i=0;
        for(String col : bqSchema.keySet()) {
            if(parts[i]==null) {
                i++;
                continue;
            }
            if(bqSchema.get(col).equals("STRING")) {
                if(!parts[i].equals("IA_NULL")) {
                    if(replaceSpecialChars) tr.set(col, preprocessStringField(parts[i]));
                    else tr.set(col, parts[i]);
                }
            }
            else if(bqSchema.get(col).equals("NUMERIC")) { if(!parts[i].equals("N")) tr.set(col, Double.valueOf(parts[i])); }
            else if(bqSchema.get(col).equals("BIGNUMERIC")) { if(!parts[i].equals("N")) tr.set(col, Long.valueOf(parts[i])); }
            else if(bqSchema.get(col).equals("FLOAT64")) { if(!parts[i].equals("N")) tr.set(col, Double.valueOf(parts[i])); }
            else if(bqSchema.get(col).equals("BOOL")) { if(!parts[i].equals("N")) tr.set(col, Boolean.valueOf(parts[i])); }
            else if(bqSchema.get(col).equals("DATE")) { if(!parts[i].equals("N")) tr.set(col, parts[i]); }
            else if(bqSchema.get(col).equals("TIME")) { if(!parts[i].equals("N")) tr.set(col, parts[i]); }
            else if(bqSchema.get(col).equals("DATETIME")) { if(!parts[i].equals("N")) tr.set(col, parts[i]); }
            else if(bqSchema.get(col).equals("TIMESTAMP")) { if(!parts[i].equals("N")) tr.set(col, parts[i]); }
            else if(bqSchema.get(col).equals("BYTES")) { if(!parts[i].equals("N")) tr.set(col, parts[i]); }
            else throw new Exception(String.format("Invalid bigquery data type %s", bqSchema.get(col)));
            i++;
        }
//        for(int i=0; i<columns.size(); i++) {
//            //columns.get
//            if(columns.get(i).getType().equals("STRING")) tr.set(columns.get(i).getName(), parts[i]);
//            else if(columns.get(i).getType().equals("NUMERIC")) tr.set(columns.get(i).getName(), Double.valueOf(parts[i]));
//            else if(columns.get(i).getType().equals("BIGNUMERIC")) tr.set(columns.get(i).getName(), Long.valueOf(parts[i]));
//            else if(columns.get(i).getType().equals("FLOAT64")) tr.set(columns.get(i).getName(), Double.valueOf(parts[i]));
//            else if(columns.get(i).getType().equals("BOOL")) tr.set(columns.get(i).getName(), Boolean.valueOf(parts[i]));
//            else if(columns.get(i).getType().equals("DATE")) tr.set(columns.get(i).getName(), parts[i]);
//            else if(columns.get(i).getType().equals("TIME")) tr.set(columns.get(i).getName(), parts[i]);
//            else if(columns.get(i).getType().equals("DATETIME")) tr.set(columns.get(i).getName(), parts[i]);
//            else if(columns.get(i).getType().equals("TIMESTAMP")) tr.set(columns.get(i).getName(), parts[i]);
//            else if(columns.get(i).getType().equals("BYTES")) tr.set(columns.get(i).getName(), parts[i]);
//            else throw new Exception(String.format("Invalid bigquery data type %s", columns.get(i).getType()));
//        }
        totalOutputRowsCounter.inc();
        return tr;
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

}
