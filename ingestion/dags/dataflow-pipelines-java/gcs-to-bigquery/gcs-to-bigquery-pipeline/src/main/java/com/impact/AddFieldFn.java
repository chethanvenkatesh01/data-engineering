package com.impact;

import org.apache.beam.sdk.transforms.DoFn;

public class AddFieldFn extends DoFn<String, String> {

    protected String delimiter = ",";

    public static AddFieldFn builder() {
        return new AddFieldFn();
    }

    public AddFieldFn withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    @ProcessElement
    public void processElement(@Element String element, ProcessContext context) {
        String line = element;
        context.output(line+"");
    }
}
