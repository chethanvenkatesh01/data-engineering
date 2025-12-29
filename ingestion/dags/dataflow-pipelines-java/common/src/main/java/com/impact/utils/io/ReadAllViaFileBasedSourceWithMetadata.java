package com.impact.utils.io;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CompressedSource;
//import org.apache.beam.sdk.io.FileBasedSource;
import com.impact.utils.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ReadAllViaFileBasedSourceWithMetadata<T> extends PTransform<PCollection<ReadableFile>, PCollection<KV<String, T>>> {

    public static final boolean DEFAULT_USES_RESHUFFLE = true;
    private final long desiredBundleSizeBytes;
    private final SerializableFunction<String, ? extends FileBasedSource<T>> createSource;
    //private final Coder<T> coder;
    //private final Coder<KV<String, T>> coder;
    private final KvCoder<String, T> coder;
    //private final Coder<KV<String, T>>
    private final com.impact.utils.io.ReadAllViaFileBasedSourceWithMetadata.ReadFileRangesFnExceptionHandler exceptionHandler;
    private final boolean usesReshuffle;

    public ReadAllViaFileBasedSourceWithMetadata(
            long desiredBundleSizeBytes,
            SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
            KvCoder<String, T> coder,
            boolean usesReshuffle,
            com.impact.utils.io.ReadAllViaFileBasedSourceWithMetadata.ReadFileRangesFnExceptionHandler exceptionHandler) {
        this.desiredBundleSizeBytes = desiredBundleSizeBytes;
        this.createSource = createSource;
        this.coder = coder;
        this.usesReshuffle = usesReshuffle;
        this.exceptionHandler = exceptionHandler;
    }

    public ReadAllViaFileBasedSourceWithMetadata(
            long desiredBundleSizeBytes,
            SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
            KvCoder<String, T> coder) {
        this(
                desiredBundleSizeBytes,
                createSource,
                coder,
                DEFAULT_USES_RESHUFFLE,
                new com.impact.utils.io.ReadAllViaFileBasedSourceWithMetadata.ReadFileRangesFnExceptionHandler());
    }

    @Override
    public PCollection<KV<String, T>> expand(PCollection<ReadableFile> input) {
        PCollection<KV<ReadableFile, OffsetRange>> ranges =
                input.apply("Split into ranges", ParDo.of(new com.impact.utils.io.ReadAllViaFileBasedSourceWithMetadata.SplitIntoRangesFn(desiredBundleSizeBytes)));
        if (usesReshuffle) {
            ranges = ranges.apply("Reshuffle", Reshuffle.viaRandomKey());
        }
//        return ranges
//                .apply("Read ranges", ParDo.of(new com.impact.utils.io.ReadAllViaFileBasedSource.ReadFileRangesFn<T>(createSource, exceptionHandler)))
//                .setCoder(coder);
        return ranges
                .apply("Read ranges", ParDo.of(new ReadAllViaFileBasedSourceWithMetadata.ReadFileRangesFn<T>(createSource, exceptionHandler)))
                .setCoder(coder);
    }

    private static class SplitIntoRangesFn extends DoFn<ReadableFile, KV<ReadableFile, OffsetRange>> {
        private final long desiredBundleSizeBytes;

        private SplitIntoRangesFn(long desiredBundleSizeBytes) {
            this.desiredBundleSizeBytes = desiredBundleSizeBytes;
        }

        @ProcessElement
        public void process(ProcessContext c) {
            Metadata metadata = c.element().getMetadata();
            if (!metadata.isReadSeekEfficient()) {
                c.output(KV.of(c.element(), new OffsetRange(0, metadata.sizeBytes())));
                return;
            }
            for (OffsetRange range :
                    new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSizeBytes, 0)) {
                c.output(KV.of(c.element(), range));
            }
        }
    }

    private static class ReadFileRangesFn<T> extends DoFn<KV<ReadableFile, OffsetRange>, KV<String, T>> {
        private final SerializableFunction<String, ? extends FileBasedSource<T>> createSource;
        private final com.impact.utils.io.ReadAllViaFileBasedSourceWithMetadata.ReadFileRangesFnExceptionHandler exceptionHandler;

        private ReadFileRangesFn(
                SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
                com.impact.utils.io.ReadAllViaFileBasedSourceWithMetadata.ReadFileRangesFnExceptionHandler exceptionHandler) {
            this.createSource = createSource;
            this.exceptionHandler = exceptionHandler;
        }

        @ProcessElement
//        @SuppressFBWarnings(
//                value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
//                justification = "https://github.com/spotbugs/spotbugs/issues/756")
        public void process(ProcessContext c) throws IOException {
            ReadableFile file = c.element().getKey();
            String fileName = file.getMetadata().resourceId().toString();
            OffsetRange range = c.element().getValue();
            com.impact.utils.io.FileBasedSource<T> source =
                    com.impact.utils.io.CompressedSource.from(createSource.apply(file.getMetadata().resourceId().toString()))
                            .withCompression(file.getCompression());
            try (BoundedSource.BoundedReader<T> reader =
                         source
                                 .createForSubrangeOfFile(file.getMetadata(), range.getFrom(), range.getTo())
                                 .createReader(c.getPipelineOptions())) {
                for (boolean more = reader.start(); more; more = reader.advance()) {
                    c.output(KV.of(fileName, reader.getCurrent()));
                }
            } catch (RuntimeException e) {
                if (exceptionHandler.apply(file, range, e)) {
                    throw e;
                }
            }
        }
    }

    public static class ReadFileRangesFnExceptionHandler implements Serializable {

        /*
         * Applies the desired handler logic to the given exception and returns
         * if the exception should be thrown.
         */
        public boolean apply(ReadableFile file, OffsetRange range, Exception e) {
            return true;
        }
    }


}
