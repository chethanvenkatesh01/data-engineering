package com.impact;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CleanTmpFilesFromGcsFn extends DoFn<Object, Object> {
    //private final ValueProvider<String> stagingBucketDir;
    private final String stagingBucketDir;
    private final String tmpDirName;
    /**
     * Created object that will remove temp files from stage.
     *
     * @param stagingBucketDir bucket and directory where temporary files are saved
     * @param tmpDirName temporary directory created on bucket where files were saved
     */
    public CleanTmpFilesFromGcsFn(String stagingBucketDir, String tmpDirName) {
        this.stagingBucketDir = stagingBucketDir;
        this.tmpDirName = tmpDirName;
    }
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        String combinedPath = String.format("%s/%s/**", stagingBucketDir, tmpDirName);
        log.info(String.format("Combined Path %s", combinedPath));
        List<ResourceId> paths =
                FileSystems.match(combinedPath).metadata().stream()
                        .map(metadata -> metadata.resourceId())
                        .collect(Collectors.toList());
        for(ResourceId resId : paths) {
            log.info(String.format("File Name: %s", resId.getFilename()));
        }
        FileSystems.delete(paths, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    }
}
