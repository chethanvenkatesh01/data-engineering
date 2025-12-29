package com.impact;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class PreprocessRows extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        PCollection<String> preprocessedRows = input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                String row = context.element();
                row = row.replace("\'", "__ia_char_01").replace("\"", "__ia_char_02")
                        .replace("/", "__ia_char_03").replace("\\", "__ia_char_04")
                        .replace("`", "__ia_char_05").replace("~", "__ia_char_06")
                        .replace("!", "__ia_char_07").replace("@", "__ia_char_08")
                        .replace("#", "__ia_char_09").replace("$", "__ia_char_10")
                        .replace("%", "__ia_char_11").replace("^", "__ia_char_12")
                        .replace("&", "__ia_char_13").replace("*", "__ia_char_14")
                        .replace("(", "__ia_char_15").replace(")", "__ia_char_16")
                        .replace("=", "__ia_char_19").replace("+", "__ia_char_20")
                        .replace("{", "__ia_char_21").replace("}", "__ia_char_22")
                        .replace("[", "__ia_char_23").replace("]", "__ia_char_24")
                        .replace("|", "__ia_char_25").replace(":", "__ia_char_26")
                        .replace(";", "__ia_char_27").replace("<", "__ia_char_28")
                        .replace(">", "__ia_char_29").replace(",", "__ia_char_30")
                        .replace(".", "__ia_char_31").replace("?", "__ia_char_32").replace("\t", "__ia_char_33");
                context.output(row);
            }
        }));
        return preprocessedRows;
    }
}
