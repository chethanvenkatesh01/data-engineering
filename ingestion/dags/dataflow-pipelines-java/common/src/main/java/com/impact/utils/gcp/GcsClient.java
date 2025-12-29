package com.impact.utils.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import com.impact.utils.text.CharsetDetector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;

@Slf4j
public class GcsClient {
    public Storage storageClient;

    public GcsClient() {
        log.debug(String.format("Initializing GCS Client for project (default) %s", StorageOptions.getDefaultProjectId()));
        storageClient = StorageOptions.getDefaultInstance().getService();
    }
    public GcsClient(String projectId) {
        log.debug(String.format("Initializing GCS Client for project %s", projectId));
        storageClient = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    }

    public List<String> listBlobs(String bucketName, String blobPrefix, boolean listRecursively) {
        List<String> blobList = new ArrayList<String>();
        List<Storage.BlobListOption> blobListOptions = new ArrayList<Storage.BlobListOption>();
        if(blobPrefix!=null) blobListOptions.add(Storage.BlobListOption.prefix(blobPrefix));
        if(!listRecursively) blobListOptions.add(Storage.BlobListOption.currentDirectory());
        //if(pageSize>=0) blobListOptions.add(Storage.BlobListOption.pageSize(pageSize));
        //blobListOptions.add(Storage.BlobListOption.pageSize());
        Page<Blob> blobs = storageClient.list(bucketName, blobListOptions.toArray(new Storage.BlobListOption[0]));
        for (Blob blob : blobs.iterateAll()) {
            blobList.add(blob.getName());
        }
        return blobList;
    }

    public List<String> listBlobsWithPattern(String filePattern) {
        // This method is used to list blobs matching the pattern (Ex: gs://<BUCKET>/*.csv)
        // Input should be complete GCS path
        List<String> blobList = new ArrayList<String>();
        List<Storage.BlobListOption> blobListOptions = new ArrayList<Storage.BlobListOption>();
        String bucketName = filePattern.replace("gs://","").split("/")[0];
        String[] blobNameComponents = filePattern.replace(String.format("gs://%s/", bucketName), "").split("/");
        String blobPrefix = StringUtils.join(blobNameComponents, "/", 0, blobNameComponents.length-1);
        blobPrefix = blobPrefix.length()>=1 ? blobPrefix + "/" : blobPrefix;
        blobListOptions.add(Storage.BlobListOption.prefix(blobPrefix));
        blobListOptions.add(Storage.BlobListOption.currentDirectory());
        Page<Blob> blobs = storageClient.list(bucketName, blobListOptions.toArray(new Storage.BlobListOption[0]));
        String fileNamePattern = blobNameComponents[blobNameComponents.length-1];
        if(fileNamePattern.startsWith("*")) {
            fileNamePattern = fileNamePattern.replaceFirst("\\*", ".*");
        }
        Pattern p = Pattern.compile(fileNamePattern);
        Matcher matcher = null;
        for(Blob blob : blobs.iterateAll()) {
            matcher = p.matcher(Paths.get(blob.getName()).getFileName().toString());
            if(matcher.matches()) {
                blobList.add(String.format("gs://%s/%s", bucketName, blob.getName()));
            }
        }
        return blobList;
    }

//    public byte[] readNBytesFromBlob(String blobFullPath, int numBytes) throws IOException {
//        String bucketName = blobFullPath.replace("gs://","").split("/")[0];
//        log.info(String.format("Bucket name %s", bucketName));
//        String objectName = blobFullPath.replace(String.format("gs://%s/", bucketName), "");
//        log.info(String.format("Object name %s", objectName));
//        try (ReadChannel reader = storageClient.reader(BlobId.of(bucketName, objectName))) {
//            InputStream inputStream = Channels.newInputStream(reader);
//            //new GZIPInputStream(ipStream);
//            //new BufferedReader(n)
//            log.info(new BufferedReader(new InputStreamReader(inputStream)).readLine());
//            byte[] byteArr = inputStream.readNBytes(numBytes);
//            return byteArr;
//        }
//    }

    public String getFileHeader(String blobFullPath) throws IOException {
        String bucketName = blobFullPath.replace("gs://","").split("/")[0];
        log.info(String.format("Bucket name %s", bucketName));
        String objectName = blobFullPath.replace(String.format("gs://%s/", bucketName), "");
        log.info(String.format("Object name %s", objectName));
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String header = null;
        String fileEncoding = getFileEncoding(blobFullPath);
        log.info(String.format("Using the file encoding %s", fileEncoding));
        try (ReadChannel reader = storageClient.reader(BlobId.of(bucketName, objectName))) {
            InputStream inputStream = Channels.newInputStream(reader);
            if(Pattern.matches(".*.gz$", objectName)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(gzipInputStream, fileEncoding));
            }
            else if(Pattern.matches(".*.zip$", objectName)) {
                zipInputStream = new ZipInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(zipInputStream, fileEncoding));
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", objectName)) {
                br = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", objectName));
            }
            //log.info(new BufferedReader(new InputStreamReader(inputStream)).readLine());
            //byte[] byteArr = inputStream.readNBytes(numBytes);
            header = br.readLine();
        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        log.info(String.format("Header: %s", header));
        return header;
    }

    public String getFileHeader(String blobFullPath, String fileEncoding) throws IOException {
        String bucketName = blobFullPath.replace("gs://","").split("/")[0];
        log.info(String.format("Bucket name %s", bucketName));
        String objectName = blobFullPath.replace(String.format("gs://%s/", bucketName), "");
        log.info(String.format("Object name %s", objectName));
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String header = null;
        String encoding = fileEncoding!=null ? fileEncoding : getFileEncoding(blobFullPath);
        log.info(String.format("Using the file encoding %s", encoding));
        try (ReadChannel reader = storageClient.reader(BlobId.of(bucketName, objectName))) {
            InputStream inputStream = Channels.newInputStream(reader);
            if(Pattern.matches(".*.gz$", objectName)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                br = new BufferedReader(new InputStreamReader(gzipInputStream, encoding));
            }
            else if(Pattern.matches(".*.zip$", objectName)) {
                //zipInputStream = new ZipInputStream(inputStream);
                //br = new BufferedReader(new InputStreamReader(zipInputStream, encoding));
                zipInputStream = new ZipInputStream(inputStream);
                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    log.info(String.format("Processing %s from %s", zipEntry.getName(), blobFullPath));
                    if (Pattern.matches(".*.(csv|txt|dat)$", zipEntry.getName())) {
                        br = new BufferedReader(new InputStreamReader(zipInputStream, StandardCharsets.UTF_8));
                        break;
                    }
                }
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", objectName)) {
                br = new BufferedReader(new InputStreamReader(inputStream, encoding));
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", objectName));
            }
            //log.info(new BufferedReader(new InputStreamReader(inputStream)).readLine());
            //byte[] byteArr = inputStream.readNBytes(numBytes);
            header = br.readLine();
        }
        catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        log.info(String.format("Header: %s", header));
        return header;
    }

    public String getFileEncoding(String filePath) {
        String bucketName = filePath.replace("gs://","").split("/")[0];
        String objectName = filePath.replace(String.format("gs://%s/", bucketName), "");
        CharsetDetector charsetDetector = new CharsetDetector();
        GZIPInputStream gzipInputStream = null;
        ZipInputStream zipInputStream = null;
        BufferedReader br = null;
        String fileEncoding = null;
        try (ReadChannel reader = storageClient.reader(BlobId.of(bucketName, objectName))) {
            InputStream inputStream = Channels.newInputStream(reader);
            if(Pattern.matches(".*.gz$", objectName)) {
                gzipInputStream = new GZIPInputStream(inputStream);
                charsetDetector.setText(new BufferedInputStream(gzipInputStream));
                fileEncoding = charsetDetector.detect().getName();
                br = new BufferedReader(new InputStreamReader(gzipInputStream, fileEncoding));
                br.readLine();
            }
            else if(Pattern.matches(".*.zip$", objectName)) {
                zipInputStream = new ZipInputStream(inputStream);
                charsetDetector.setText(new BufferedInputStream(zipInputStream));
                fileEncoding = charsetDetector.detect().getName();
                br = new BufferedReader(new InputStreamReader(zipInputStream, fileEncoding));
                br.readLine();
            }
            else if(Pattern.matches(".*.(csv|txt|dat)$", objectName)) {
                charsetDetector.setText(new BufferedInputStream(inputStream));
                fileEncoding = charsetDetector.detect().getName();
                br = new BufferedReader(new InputStreamReader(inputStream, fileEncoding));
                br.readLine();
            }
            else {
                throw new Exception(String.format("The file %s has unsupported format", objectName));
            }
            return fileEncoding;
            //return charsetDetector.detect().getName();
        }
        catch (Exception e) {
            //log.error(ExceptionUtils.getStackTrace(e));
            log.warn(String.format("Unsupported encoding %s. Using UTF-8 as default", fileEncoding));
            return "UTF-8";
        }
    }
}
