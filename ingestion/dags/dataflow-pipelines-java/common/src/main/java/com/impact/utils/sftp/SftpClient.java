package com.impact.utils.sftp;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.impact.utils.text.CharsetDetector;
import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Slf4j
public class SftpClient {
    private String host;
    private String userName;
    private String password;
    private JSch jsch;
    private Session jschSession;

    public SftpClient(String host, String userName, String password) throws JSchException {
        this.host = host;
        this.userName = userName;
        this.password = password;
        log.info("Connecting to SFTP");
        jsch = new JSch();
        jschSession = jsch.getSession(this.userName, this.host);
        jschSession.setPassword(this.password);
        jschSession.setConfig("StrictHostKeyChecking","no");
        jschSession.connect();
        log.info("SFTP session created");
    }

    public Session getJschSession() {
        return this.jschSession;
    }

    // Returns the absolute paths for the files which has given prefix and suffix in the given directory
    public List<String> getFilePaths(String sftpDirectory, String prefix, String suffix) {
        List<String> filePaths = new ArrayList<>();
        ChannelSftp channelSftp = null;
        try {
            log.info(String.format("Getting file paths from SFTP server"));
            channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
            channelSftp.connect();
            log.info("SFTP Channel Created");
            Vector<ChannelSftp.LsEntry> files = channelSftp.ls(sftpDirectory);
            for(ChannelSftp.LsEntry file : files) {
                if(file.getFilename().endsWith(suffix) && file.getFilename().startsWith(prefix)) {
                    filePaths.add(sftpDirectory.endsWith("/") ? sftpDirectory+file.getFilename() : sftpDirectory+"/"+file.getFilename());
                }
            }
            return filePaths;
        }
        catch (Exception e) {
            log.error("Exception occured while fetching files from SFTP.\n"+ ExceptionUtils.getStackTrace(e));
            return filePaths;
        }
        finally {
            if(channelSftp!=null && channelSftp.isConnected()) {
                channelSftp.disconnect();
                log.info("SFTP channel removed");
            }
        }
    }

    public List<String> getFilePaths(String sftpDirectory, String filePattern, int limit) {
        // Pass any negative number to limit to fetch all the matched files
        List<String> filePaths = new ArrayList<>();
        ChannelSftp channelSftp = null;
        int numOfFilesFound = 0;
        int maxNumOfFiles = limit>0 ? limit : Integer.MAX_VALUE;
        try {
            log.info(String.format("Getting file paths from SFTP server (%s)", sftpDirectory));
            channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
            channelSftp.connect();
            log.info("SFTP Channel Created");
            Vector<ChannelSftp.LsEntry> files = channelSftp.ls(sftpDirectory);
            String fileName = null;
            for(ChannelSftp.LsEntry file : files) {
                fileName = file.getFilename();
                if(Pattern.matches(filePattern, fileName)) {
                    filePaths.add(sftpDirectory.endsWith("/") ? sftpDirectory+file.getFilename() : sftpDirectory+"/"+file.getFilename());
                    numOfFilesFound += 1;
                }
                if(numOfFilesFound >= maxNumOfFiles) break;
            }
            return filePaths;
        }
        catch (Exception e) {
            log.error("Exception occured while fetching files from SFTP.\n"+ ExceptionUtils.getStackTrace(e));
            return filePaths;
        }
        finally {
            if(channelSftp!=null && channelSftp.isConnected()) {
                channelSftp.disconnect();
                log.info("SFTP channel removed");
            }
        }
    }

    public ChannelSftp getSftpChannel() throws JSchException {
        ChannelSftp channelSftp = null;
        try {
            channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
            channelSftp.connect();
            log.info("SFTP Channel Created");
        }
        catch (Exception e) {
            log.error("Exception occured while creating SFTP channel.\n"+ ExceptionUtils.getStackTrace(e));
        }
        return channelSftp;

    }

    public String getFileHeader(String filePath, String fileEncoding) throws JSchException, IOException {
        log.debug("Reading file header");
        ChannelSftp channelSftp = null;
        InputStream inputStream = null;
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String line = null;
        log.info(String.format("Reading header from file %s", filePath));
        String encoding = fileEncoding!=null ? fileEncoding : getFileEncoding(filePath);
        log.info(String.format("File Encoding: %s", encoding));
        try {
            channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
            channelSftp.connect();
            inputStream = channelSftp.get(filePath);
            if(Pattern.matches(".*.gz$", filePath)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(gzipInputStream, encoding));
            }
            else if(Pattern.matches(".*.zip$", filePath)) {
                //zipInputStream = new ZipInputStream(inputStream);
                //br = new BufferedReader(new InputStreamReader(zipInputStream, encoding));
                zipInputStream = new ZipInputStream(inputStream);
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    log.info(String.format("Processing %s from %s", zipEntry.getName(), filePath));
                    if (Pattern.matches(".*.(csv|txt|dat)$", zipEntry.getName())) {
                        br = new BufferedReader(new InputStreamReader(zipInputStream, StandardCharsets.UTF_8));
                        break;
                    }
                }
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", filePath)) {
                br = new BufferedReader(new InputStreamReader(inputStream, encoding));
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", filePath));
            }
            line = br.readLine();
            log.info(String.format("Header %s", line));
            channelSftp.disconnect();

        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        return line;
    }

    public String getFileEncoding(String filePath) {
        CharsetDetector charsetDetector = new CharsetDetector();
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String fileEncoding = null;
        ChannelSftp channelSftp = null;
        InputStream inputStream = null;
        try {
            channelSftp = (ChannelSftp) jschSession.openChannel("sftp");
            channelSftp.connect();
            inputStream = channelSftp.get(filePath);
            if(Pattern.matches(".*.gz$", filePath)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                charsetDetector.setText(new BufferedInputStream(gzipInputStream));
                fileEncoding = charsetDetector.detect().getName();
                // Reading a line with detected encoding, so that any error while reading the file with this encoding will be caught
                br = new BufferedReader(new InputStreamReader(gzipInputStream, fileEncoding));
                br.readLine();
            }
            else if(Pattern.matches(".*.zip$", filePath)) {
                zipInputStream = new ZipInputStream(inputStream);
                charsetDetector.setText(new BufferedInputStream(zipInputStream));
                fileEncoding = charsetDetector.detect().getName();
                br = new BufferedReader(new InputStreamReader(zipInputStream, fileEncoding));
                br.readLine();
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", filePath)) {
                charsetDetector.setText(new BufferedInputStream(inputStream));
                fileEncoding = charsetDetector.detect().getName();
                br = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
                br.readLine();
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", filePath));
            }
            channelSftp.disconnect();
            return fileEncoding;

        } catch (Exception e) {
            log.warn(String.format("Unsupported encoding %s. Using UTF-8 as default", fileEncoding));
            return "UTF-8";
        }
    }

    public String getHeader() {
        return "SHIPMENT_NO,LINE_NO,SHIPMENT_STATUS,PO_NUMBER,SHIPPED_WEIGHT,SHIPPED_VOLUME,SHIPPED_UNITS,SHIPPED_CASES," +
                "ASN_CREATE_DATE,SHIP_DATE,ESTIMATED_DATE_ARRIVAL,ACTUAL_DATE_ARRIVAL,ESTIMATED_PORT_ARRIVAL_DATE," +
                "ESTIMATED_CHESTER_ARRIVAL_DATE,CONTAINER_DISPOSITION";
    }
}
