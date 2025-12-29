package com.impact.utils.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;

@Slf4j
public class SftpSink extends DoFn<String, String> {

    SftpClient sftpClient;
    String hostName;
    String userName;
    String password;
    ChannelSftp sftpChannel = null;
    String directory = "/";
    String filePrefix = "beam_sftp_sink_tmp";
    OutputStream outputStream;
    BufferedWriter bw;
    String delimiter = "|";
    String header = null;

    private Counter totalOutputRows = Metrics.counter(SftpSink.class, "totalOutputRows");

    static int bundleNumber = 0;

    public static SftpSink builder() {
        return new SftpSink();
    }

    public SftpSink withConfiguration(String host, String userName, String password) {
        this.hostName = host;
        this.userName = userName;
        this.password = password;
        return this;
    }

    public SftpSink withDirectory(String directory) {
        this.directory = directory;
        return this;
    }

    public SftpSink withFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
        return this;
    }
    public SftpSink withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }
    public SftpSink withHeader(String header) {
        this.header = header;
        return this;
    }


    @Setup
    public void setup() {
        log.info("SftpSink Setup");
        try {
            sftpClient = new SftpClient(hostName, userName, password);
        } catch (JSchException e) {
            log.error("Error occured while creating the SFTP Client \n  " + ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) throws Exception {
        log.info("[" + LocalDateTime.now() + "]" + "SftpSink DoFn Bundle Started");
        sftpChannel = sftpClient.getSftpChannel();
        bundleNumber = bundleNumber + 1;
        String filePath = String.format("%s/%s_%s.%s", directory, filePrefix, bundleNumber, "csv");
        log.info(String.format("File Path: %s", filePath));
        outputStream = sftpChannel.put(filePath);
        bw = new BufferedWriter(new OutputStreamWriter(outputStream));
        bw.write(header);
        bw.newLine();
        log.info("Created Channels and Streams");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        totalOutputRows.inc();
        //context.output(context.element());
        bw.write(context.element());
        bw.newLine();
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext finishBundleContext) throws Exception {
        log.info("[" + LocalDateTime.now() + "]" + "SftpSink DoFn Bundle Finished");
        bw.close();
        outputStream.close();
        sftpChannel.disconnect();
        log.info("Closed Channels and Streams");
    }

    @Teardown
    public void teardown() {
        log.info("SftpSink Teardown");
    }
}
