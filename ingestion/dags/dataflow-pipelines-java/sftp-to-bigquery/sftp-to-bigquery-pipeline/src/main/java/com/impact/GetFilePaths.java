package com.impact;

import com.impact.utils.sftp.SftpClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import com.jcraft.jsch.JSchException;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class GetFilePaths extends DoFn<KV<String, Iterable<Map<String, String>>>, KV<String, String>> {

    SftpClient sftpClient;
    String hostName;
    String userName;
    String password;

    public static GetFilePaths builder() {
        return new GetFilePaths();
    }

    public GetFilePaths withSftpCredentials(String hostName, String userName, String password) {
        this.hostName = hostName;
        this.userName = userName;
        this.password = password;
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
        log.info("[" + LocalDateTime.now() + "]" + "GetFilePaths DoFn Bundle Started");
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        String directory = context.element().getKey();
        log.info(String.format("Directory %s",directory));
        Map<String, String> fileMatchOptions = context.element().getValue().iterator().next();
        String filePattern = fileMatchOptions.get("filePrefix")+".*"+fileMatchOptions.get("fileSuffix")+"$";
        List<String> files = sftpClient.getFilePaths(directory, filePattern, -1);
        for(String file : files) {
            context.output(KV.of(file, "1"));
        }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext finishBundleContext) {
        log.info("[" + LocalDateTime.now() + "]" + "GetFilePaths DoFn Bundle Finished");
    }

    @Teardown
    public void teardown() {
        if(sftpClient.getJschSession()!=null && sftpClient.getJschSession().isConnected()) {
            sftpClient.getJschSession().disconnect();
            log.info("SFTP Session closed");
        }
    }
}
