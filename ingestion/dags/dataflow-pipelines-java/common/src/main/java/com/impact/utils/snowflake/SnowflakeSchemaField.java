package com.impact.utils.snowflake;

import java.io.Serializable;

public class SnowflakeSchemaField implements Serializable {
    private String name;
    private String type;
    private int precision;

    public SnowflakeSchemaField(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public SnowflakeSchemaField(String name, String type, int precision) {
        this.name = name;
        this.type = type;
        this.precision = precision;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public int getPrecision() {
        return precision;
    }

}
