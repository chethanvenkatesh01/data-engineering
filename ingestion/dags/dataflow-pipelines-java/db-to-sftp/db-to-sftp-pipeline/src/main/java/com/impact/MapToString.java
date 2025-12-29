package com.impact;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.util.LinkedList;

public class MapToString implements JdbcIO.RowMapper<String> {
    public String fieldDelimiter = "|";

    public MapToString(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    @Override
    public String mapRow(ResultSet resultSet) throws Exception {
        int columnCount = resultSet.getMetaData().getColumnCount();
        LinkedList<String> fields = new LinkedList<>();
        for(int i=1; i<=columnCount; i++) {
            fields.add(resultSet.getString(i));
        }
        return StringUtils.join(fields, fieldDelimiter);
    }
}
