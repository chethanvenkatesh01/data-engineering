package com.impact;

import com.impact.utils.sftp.SftpClient;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class ReadSftpFile extends DoFn<KV<String, Iterable<String>>, String> {
    SftpClient sftpClient;
    String hostName;
    String userName;
    String password;
    boolean header;
    String delimiter;
    String fileEncoding;

    private Counter totalOutputBytes = Metrics.counter(ReadSftpFile.class, "totalOutputBytes");

    public ReadSftpFile() {
        this.header = true;
        this.delimiter = ",";
    }

    public static ReadSftpFile builder() {
        return new ReadSftpFile();
    }

    public ReadSftpFile withSftpCredentials(String hostName, String userName, String password) {
        this.hostName = hostName;
        this.userName = userName;
        this.password = password;
        return this;
    }

    public ReadSftpFile withHeader(boolean header) {
        this.header = header;
        return this;
    }

    public ReadSftpFile withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public ReadSftpFile withFileEncoding(String fileEncoding) {
        this.fileEncoding = fileEncoding;
        return this;
    }

    @Setup
    public void setup() {
        try {
            sftpClient = new SftpClient(hostName, userName, password);
        } catch (JSchException e) {
            log.error("Error occured while creating the SFTP Client \n  " + ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @StartBundle
    public void startBundle(StartBundleContext startBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "ReadSftpFile DoFn Bundle Started");
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws JSchException {
        String filePath = context.element().getKey();
        ChannelSftp sftpChannel = sftpClient.getSftpChannel();
        InputStream inputStream = null;
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String line;
        String encoding = fileEncoding != null ? fileEncoding : sftpClient.getFileEncoding(filePath);
        //String fileEncoding = sftpClient.getFileEncoding(filePath);
        log.info(String.format("Using %s as encoding for %s", encoding, filePath));
        String fileDate = getFileDateFromPath(filePath);
        log.info(String.format("File date %s", fileDate));
        try {
            inputStream = sftpChannel.get(filePath);
            if(Pattern.matches(".*.gz$", filePath)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(gzipInputStream, encoding));
                if(header) {
                    // Skip the header line
                    line = br.readLine();
                    totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                    while ((line = br.readLine()) != null) {
                        totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                        context.output(line+delimiter+fileDate);
                    }
                }
                else {
                    while ((line = br.readLine()) != null) {
                        totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                        context.output(line+delimiter+fileDate);
                    }
                }
            }
            else if(Pattern.matches(".*.zip$", filePath)) {
                zipInputStream = new ZipInputStream(inputStream);
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    log.info(String.format("Processing %s from %s", zipEntry.getName(), filePath));
                    if (Pattern.matches(".*.(csv|txt|dat)$", zipEntry.getName())) {
                        br = new BufferedReader(new InputStreamReader(zipInputStream, StandardCharsets.UTF_8));
                        if(header) {
                            // Skip the header line
                            line = br.readLine();
                            totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                            while ((line = br.readLine()) != null) {
                                totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                                context.output(line+delimiter+fileDate);
                            }
                        }
                        else {
                            while ((line = br.readLine()) != null) {
                                totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                                context.output(line+delimiter+fileDate);
                            }
                        }
                    }
                }
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", filePath)) {
                br = new BufferedReader(new InputStreamReader(inputStream, encoding));
                if(header) {
                    // Skip the header line
                    line = br.readLine();
                    totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                    while ((line = br.readLine()) != null) {
                        totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                        context.output(line+delimiter+fileDate);
                    }
                }
                else {
                    while ((line = br.readLine()) != null) {
                        totalOutputBytes.inc(line.getBytes().length+1); //Adding 1 to account for "\n"
                        context.output(line+delimiter+fileDate);
                    }
                }
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", filePath));
            }
        }
        catch (Exception e) {
            log.error(String.format("Error reading file %s", filePath));
            log.error(String.format("Exception occured in ReadSftpFile.processElement %s", ExceptionUtils.getStackTrace(e)));
            throw new RuntimeException(e);
        }
        finally {
            sftpChannel.disconnect();
        }

    }

    @FinishBundle
    public void finishBundle(FinishBundleContext finishBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "ReadSftpFile DoFn Bundle Finished");
    }

    @Teardown
    public void teardown() {
        if(sftpClient.getJschSession()!=null && sftpClient.getJschSession().isConnected()) {
            sftpClient.getJschSession().disconnect();
            log.info("SFTP Session closed");
        }
    }

    public String getFileDateFromPath(String filePath) {
        try {
            log.debug(String.format("Fetching file date from file %s", filePath));
            List<String> patterns = new LinkedList<>();
            patterns.add("\\d{14}");
            patterns.add("\\d{12}");
            patterns.add("\\d{8}");
            patterns.add("\\d{4}-\\d{2}-\\d{2}");

            String datetimeStr = null;
            int patternIndex = -1;
            Pattern p = null;
            Matcher matcher = null;

            for (int i = 0; i < patterns.size(); i++) {
                p = Pattern.compile(patterns.get(i));
                matcher = p.matcher(filePath);
                if (matcher.find()) {
                    datetimeStr = matcher.group(0);
                    patternIndex = i;
                    break;
                }
            }

            if (patternIndex == 0) {
                return LocalDateTime.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toString() + (datetimeStr.endsWith("00") ? ":00" : "");
            } else if (patternIndex == 1) {
                return LocalDateTime.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmm")).toString() + ":00";
            } else if (patternIndex == 2) {
                return LocalDate.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyyMMdd")).atStartOfDay().toString() + ":00";
            } else if (patternIndex == 3) {
                return LocalDate.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().toString() + ":00";
            } else {
                String currentDateTime = LocalDateTime.now().toString();
                log.warn(String.format("Couldn't extract datetime from the file %s. Using current datetime %s",
                        filePath, currentDateTime));
                return currentDateTime;
            }
        }
        catch (Exception e) {
            String currentDateTime = LocalDateTime.now().toString();
            log.warn(String.format("Couldn't extract datetime from the file %s. Using current datetime %s",
                    filePath, currentDateTime));
            return currentDateTime;
        }
    }
}
