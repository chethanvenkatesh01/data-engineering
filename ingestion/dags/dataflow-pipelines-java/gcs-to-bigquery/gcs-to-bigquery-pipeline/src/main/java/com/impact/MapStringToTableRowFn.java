package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import com.impact.utils.sftp.SftpClient;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MapStringToTableRowFn extends DoFn<String, TableRow> {

    protected String delimiter;
    protected LinkedHashMap<String, String> schema;
    protected LinkedList<String> fieldNames;
    protected boolean replaceSpecialChars;
    protected boolean useStandardCsvParser;

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

    public MapStringToTableRowFn() {
        this.delimiter = ",";
        this.replaceSpecialChars = false;
    }

    public static MapStringToTableRowFn builder() {
        return new MapStringToTableRowFn();
    }

    public MapStringToTableRowFn withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public MapStringToTableRowFn withSchema(LinkedHashMap<String, String> schema) {
        this.schema = schema;
        return this;
    }

    public MapStringToTableRowFn withReplaceSpecialChars(boolean replaceSpecialChars) {
        this.replaceSpecialChars = replaceSpecialChars;
        return this;
    }

    public MapStringToTableRowFn withUseStandardCsvParser(boolean useStandardCsvParser) {
        this.useStandardCsvParser = useStandardCsvParser;
        return this;
    }

    @Setup
    public void setup() {
        assert schema!=null && schema.size()>0 : "schema is either null or empty";
        LinkedList<String> columns = new LinkedList<>();
        for(String column : schema.keySet()) {
            columns.add(column);
        }
        this.fieldNames = columns;
        log.info(String.format("Using Standard CSV Parser %s", useStandardCsvParser));
    }

    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "MapStringToTableRow DoFn Bundle Started");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        String line = context.element();
        List<String> fields = null;
        if(useStandardCsvParser) {
            CSVParser parser = CSVParser.parse(line, CSVFormat.EXCEL.withDelimiter(delimiter.charAt(0)).withEscape('\\'));
            fields = parser.getRecords().get(0).toList();
        }
        else {
            line = com.impact.utils.text.StringUtils.truncateQuotesEnclosingFields(line, delimiter);
            fields = Arrays.stream(line.split(Pattern.quote(delimiter))).collect(Collectors.toList());
        }
        //line = com.impact.utils.text.StringUtils.truncateQuotesEnclosingFields(line, delimiter);
        //List<String> fields = Arrays.stream(line.split(Pattern.quote(delimiter))).collect(Collectors.toList());
        //CSVParser parser = CSVParser.parse(line, CSVFormat.EXCEL.withDelimiter(delimiter.charAt(0)).withEscape('\\'));
        //List<String> fields = parser.getRecords().get(0).toList();
        TableRow tr = new TableRow();
        for(int i=0; i<fieldNames.size(); i++) {
            try {
                if(fields.get(i).length()>0) {
                    if (schema.get(fieldNames.get(i)).equalsIgnoreCase("STRING") && replaceSpecialChars) {
                        tr.set(fieldNames.get(i), preprocessStringField(fields.get(i)));
                    } else {
                        tr.set(fieldNames.get(i), fields.get(i));
                    }
                }
            }
            catch (IndexOutOfBoundsException ignored) {}
            catch (Exception e) { throw e; }
        }
        context.output(tr);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext finishBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "MapStringToTableRow DoFn Bundle Finished");
    }

    @Teardown
    public void teardown() {
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
