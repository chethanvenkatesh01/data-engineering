package com.impact;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class GetFilesTransform extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    protected String host;
    protected String userName;
    protected String password;

    public static GetFilesTransform builder() {
        return new GetFilesTransform();
    }

    public GetFilesTransform withSftpCredentials(String host, String userName, String password) {
        this.host = host;
        this.userName = userName;
        this.password = password;
        return this;
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
        PCollection<KV<String, Iterable<Map<String, String>>>> directoriesAsKVPairs = input.apply("Generate KV pairs of directories", ParDo.of(new DoFn<String, KV<String, Map<String, String>>>() {

            @StartBundle
            public void startBundle(StartBundleContext startBundleContext) {
                log.info("[" + LocalDateTime.now() + "]" + "Generate KV Pairs of directories DoFn Bundle Started");
            }
            @ProcessElement
            public void processElement(ProcessContext context) {
                SftpToBigQueryPipelineOptions pipelineOptions = context.getPipelineOptions().as(SftpToBigQueryPipelineOptions.class);
                String directory = context.element();
                Map<String, String> fileMatchOptions = new LinkedHashMap<>();
                fileMatchOptions.put("filePrefix", pipelineOptions.getFilePrefix());
                fileMatchOptions.put("fileSuffix", pipelineOptions.getFileSuffix());
                //return KV.of(directory, fileMatchOptions)
                context.output(KV.of(directory, fileMatchOptions));
            }

            @FinishBundle
            public void finishBundle(FinishBundleContext finishBundleContext) {
                log.info("[" + LocalDateTime.now() + "]" + "Generate KV Pairs of directories DoFn Bundle Finished");
            }


        })).apply(GroupByKey.create());

        PCollection<KV<String, String>> filesAsKVPairs = directoriesAsKVPairs.apply("Get File Paths",
                ParDo.of(GetFilePaths.builder().withSftpCredentials(host, userName, password)));
        return filesAsKVPairs;
    }
}
