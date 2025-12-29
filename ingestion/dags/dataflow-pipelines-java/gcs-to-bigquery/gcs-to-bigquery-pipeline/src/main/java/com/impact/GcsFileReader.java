package com.impact;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.impact.utils.date.DateHelper;
import com.impact.utils.gcp.GcsClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
public class GcsFileReader extends DoFn<FileIO.ReadableFile, KV<String, String>> {

    public GcsClient gcsClient;
    public boolean header;

    public String fileEncoding;

    public static GcsFileReader builder() {
        return new GcsFileReader();
    }

    public GcsFileReader withHeader(boolean header) {
        this.header = header;
        return this;
    }

    public GcsFileReader withFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
        return this;
    }

    @Setup
    public void setup() {
        gcsClient = new GcsClient();
    }

    @StartBundle
    public void startBundle() {

    }

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile element, ProcessContext context) {
        String filePath = element.getMetadata().resourceId().toString();
        String encoding = fileEncoding != null ? fileEncoding : gcsClient.getFileEncoding(filePath);
        log.info(String.format("Using %s as encoding for %s", encoding, filePath));
        String bucketName = filePath.replace("gs://","").split("/")[0];
        String objectName = filePath.replace(String.format("gs://%s/", bucketName), "");
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String line = null;
        try (ReadChannel reader = gcsClient.storageClient.reader(BlobId.of(bucketName, objectName))) {
            InputStream inputStream = Channels.newInputStream(reader);
            if(Pattern.matches(".*.gz$", objectName)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(gzipInputStream, encoding));
                if(header) {
                    // Skip the header line
                    line = br.readLine();
                    while ((line = br.readLine()) != null) {
                        context.output(KV.of(filePath, line));
                    }
                }
                else {
                    while ((line = br.readLine()) != null) {
                        context.output(KV.of(filePath, line));
                    }
                }
            }
            else if(Pattern.matches(".*.zip$", objectName)) {
                zipInputStream = new ZipInputStream(inputStream);
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    log.info(String.format("Processing %s from %s", zipEntry.getName(), filePath));
                    if (Pattern.matches(".*.(csv|txt|dat)$", zipEntry.getName())) {
                        br = new BufferedReader(new InputStreamReader(zipInputStream, StandardCharsets.UTF_8));
                        if(header) {
                            // Skip the header line
                            line = br.readLine();
                            while ((line = br.readLine()) != null) {
                                context.output(KV.of(filePath, line));
                            }
                        }
                        else {
                            while ((line = br.readLine()) != null) {
                                context.output(KV.of(filePath, line));
                            }
                        }
                    }
                }
                // br = new BufferedReader(new InputStreamReader(zipInputStream, encoding));
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", objectName)) {
                br = new BufferedReader(new InputStreamReader(inputStream, encoding));
                if(header) {
                    // Skip the header line
                    line = br.readLine();
                    while ((line = br.readLine()) != null) {
                        context.output(KV.of(filePath, line));
                    }
                }
                else {
                    while ((line = br.readLine()) != null) {
                        context.output(KV.of(filePath, line));
                    }
                }
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", objectName));
            }
        }
        catch (Exception e) {
            log.error(String.format("Error reading file %s", filePath));
            log.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @FinishBundle
    public void finishBundle() {

    }

    @Teardown
    public void teardown() {

    }
}
