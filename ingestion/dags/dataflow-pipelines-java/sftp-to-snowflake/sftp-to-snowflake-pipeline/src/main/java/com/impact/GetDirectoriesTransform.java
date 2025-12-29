package com.impact;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;
import java.util.Random;

public class GetDirectoriesTransform extends PTransform<PBegin, PCollection<String>> {
    protected List<String> directories;

    public static GetDirectoriesTransform builder() {
        return new GetDirectoriesTransform();
    }

    public GetDirectoriesTransform withDirectories(List<String> directories) {
        this.directories = directories;
        return this;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
        //return input.apply(Create.of(directories)).apply(Reshuffle.viaRandomKey());
        return input.apply(Create.of(directories)).apply(ParDo.of(new DoFn<String, KV<String,Integer>>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                Random rand = new Random();
                context.output(KV.of(context.element(), rand.nextInt()));
            }
        })).apply(GroupByKey.create()).apply(Keys.create());
    }
}
