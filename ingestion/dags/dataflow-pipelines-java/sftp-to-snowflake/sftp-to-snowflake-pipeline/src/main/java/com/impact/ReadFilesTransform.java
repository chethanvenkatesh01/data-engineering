package com.impact;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ReadFilesTransform extends PTransform<PCollection<KV<String, String>>, PCollection<String>> {

    protected String host;
    protected String userName;
    protected String password;
    protected boolean header;
    protected String delimiter;
    protected String fileEncoding;

    public ReadFilesTransform() {
        this.header = true;
        this.delimiter = ",";
    }

    public static ReadFilesTransform builder() {
        return new ReadFilesTransform();
    }

    public ReadFilesTransform withSftpCredentials(String host, String userName, String password) {
        this.host = host;
        this.userName = userName;
        this.password = password;
        return this;
    }

    public ReadFilesTransform withHeader(boolean header) {
        this.header = header;
        return this;
    }

    public ReadFilesTransform withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public ReadFilesTransform withFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
        return this;
    }

    @Override
    public PCollection<String> expand(PCollection<KV<String, String>> input) {
        PCollection<KV<String, Iterable<String>>> filePathsAsKV = input.apply(GroupByKey.create());
        PCollection<String> lines = filePathsAsKV.apply("Read Files", ParDo.of(
                        ReadSftpFile.builder().withSftpCredentials(host, userName, password).withHeader(header)
                                .withDelimiter(delimiter).withFileEncoding(fileEncoding)))
                .apply(Reshuffle.viaRandomKey());
        return lines;
    }
}
