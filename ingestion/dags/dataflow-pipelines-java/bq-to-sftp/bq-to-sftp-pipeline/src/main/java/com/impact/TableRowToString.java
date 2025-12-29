package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import java.util.Map;

public class TableRowToString extends DoFn<TableRow, String> {
    private final String fieldDelimiter;

    public TableRowToString(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = c.element();
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (sb.length() > 0) {
                sb.append(fieldDelimiter);
            }
            String value = entry.getValue().toString();
            if (",".equals(fieldDelimiter)) {
                value = "\"" + value + "\"";
            }
            sb.append(value);
        }

        c.output(sb.toString());
    }
}
