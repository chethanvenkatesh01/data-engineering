package com.impact.utils.io;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

import com.google.auto.value.AutoValue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <h2>Reading text files</h2>
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code TextIO.read()} to
 * instantiate a transform and use {@link org.apache.beam.sdk.io.TextIO.Read#from(String)} to specify the path of the
 * file(s) to be read. Alternatively, if the filenames to be read are themselves in a {@link
 * PCollection} you can use {@link FileIO} to match them and {@link org.apache.beam.sdk.io.TextIO#readFiles} to read them.
 *
 * <p>{@link #read} returns a {@link PCollection} of {@link String Strings}, each corresponding to
 * one line of an input UTF-8 text file (split into lines delimited by '\n', '\r', or '\r\n', or
 * specified delimiter see {@link org.apache.beam.sdk.io.TextIO.Read#withDelimiter}).
 *
 * <h3>Filepattern expansion and watching</h3>
 *
 * <p>By default, the filepatterns are expanded only once. {@link org.apache.beam.sdk.io.TextIO.Read#watchForNewFiles} or the
 * combination of {@link FileIO.Match#continuously(Duration, TerminationCondition)} and {@link
 * #readFiles()} allow streaming of new files matching the filepattern(s).
 *
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link #readFiles()}
 * allows them in case the filepattern contains a glob wildcard character. Use {@link
 * org.apache.beam.sdk.io.TextIO.Read#withEmptyMatchTreatment} or {@link
 * FileIO.Match#withEmptyMatchTreatment(EmptyMatchTreatment)} plus {@link #readFiles()} to configure
 * this behavior.
 *
 * <p>Example 1: reading a file or filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(TextIO.read().from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>Example 2: reading a PCollection of filenames.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // E.g. the filenames might be computed from other data in the pipeline, or
 * // read from a data source.
 * PCollection<String> filenames = ...;
 *
 * // Read all files in the collection.
 * PCollection<String> lines =
 *     filenames
 *         .apply(FileIO.matchAll())
 *         .apply(FileIO.readMatches())
 *         .apply(TextIO.readFiles());
 * }</pre>
 *
 * <p>Example 3: streaming new files matching a filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> lines = p.apply(TextIO.read()
 *     .from("/local/path/to/files/*")
 *     .watchForNewFiles(
 *       // Check for new files every minute
 *       Duration.standardMinutes(1),
 *       // Stop watching the filepattern if no new files appear within an hour
 *       afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 *
 * <h3>Reading a very large number of files</h3>
 *
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link org.apache.beam.sdk.io.TextIO.Read#withHintMatchesManyFiles} for better performance and
 * scalability. Note that it may decrease performance if the filepattern matches only a small number
 * of files.
 *
 * <h2>Writing text files</h2>
 *
 * <p>To write a {@link PCollection} to one or more text files, use {@code TextIO.write()}, using
 * {@link org.apache.beam.sdk.io.TextIO.Write#to(String)} to specify the output prefix of the files to write.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"))
 *      .withSuffix(".txt")
 *      .withCompression(Compression.GZIP));
 * }</pre>
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 *
 * <p>If you want better control over how filenames are generated than the default policy allows, a
 * custom {@link FilenamePolicy} can also be set using {@link org.apache.beam.sdk.io.TextIO.Write#to(FilenamePolicy)}.
 *
 * <h3>Advanced features</h3>
 *
 * <p>{@link org.apache.beam.sdk.io.TextIO} supports all features of {@link FileIO#write} and {@link FileIO#writeDynamic},
 * such as writing windowed/unbounded data, writing data to multiple destinations, and so on, by
 * providing a {@link org.apache.beam.sdk.io.TextIO.Sink} via {@link #sink()}.
 *
 * <p>For example, to write events of different type to different filenames:
 *
 * <pre>{@code
 * PCollection<Event> events = ...;
 * events.apply(FileIO.<EventType, Event>writeDynamic()
 *       .by(Event::getTypeName)
 *       .via(TextIO.sink(), Event::toString)
 *       .to(type -> nameFilesUsingWindowPaneAndShard(".../events/" + type + "/data", ".txt")));
 * }</pre>
 *
 * <p>For backwards compatibility, {@link org.apache.beam.sdk.io.TextIO} also supports the legacy {@link
 * DynamicDestinations} interface for advanced features via {@link org.apache.beam.sdk.io.TextIO.Write#to(DynamicDestinations)}.
 */
@SuppressWarnings({
        "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TextIO {
    private static final Logger LOG = LoggerFactory.getLogger(com.impact.utils.io.ParquetIO.class);
    private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;

    /**
     * A {@link PTransform} that reads from one or more text files and returns a bounded {@link
     * PCollection} containing one element for each line of the input files.
     */
    public static com.impact.utils.io.TextIO.Read read() {
        return new AutoValue_TextIO_Read.Builder()
                .setCompression(Compression.AUTO)
                .setHintMatchesManyFiles(false)
                .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
                .build();
    }

    public static com.impact.utils.io.TextIO.ReadWithMetadata readWithMetadata() {
        return new AutoValue_TextIO_ReadWithMetadata.Builder()
                .setCompression(Compression.AUTO)
                .setHintMatchesManyFiles(false)
                .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
                .build();
    }

    /**
     * A {@link PTransform} that works like {@link #read}, but reads each file in a {@link
     * PCollection} of filepatterns.
     *
     * <p>Can be applied to both bounded and unbounded {@link PCollection PCollections}, so this is
     * suitable for reading a {@link PCollection} of filepatterns arriving as a stream. However, every
     * filepattern is expanded once at the moment it is processed, rather than watched for new files
     * matching the filepattern to appear. Likewise, every file is read once, rather than watched for
     * new entries.
     *
     * @deprecated You can achieve The functionality of {@link #readAll()} using {@link FileIO}
     *     matching plus {@link #readFiles()}. This is the preferred method to make composition
     *     explicit. {@link org.apache.beam.sdk.io.TextIO.ReadAll} will not receive upgrades and will be removed in a future version
     *     of Beam.
     */
    @Deprecated
    public static com.impact.utils.io.TextIO.ReadAll readAll() {
        return new AutoValue_TextIO_ReadAll.Builder()
                .setCompression(Compression.AUTO)
                .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
                .build();
    }

    /**
     * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
     * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
     */
    public static com.impact.utils.io.TextIO.ReadFiles readFiles() {
        return new AutoValue_TextIO_ReadFiles.Builder()
                // 64MB is a reasonable value that allows to amortize the cost of opening files,
                // but is not so large as to exhaust a typical runner's maximum amount of output per
                // ProcessElement call.
                .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
                .build();
    }

    public static com.impact.utils.io.TextIO.ReadFilesWithMetadata readFilesWithMetadata() {
        return new AutoValue_TextIO_ReadFilesWithMetadata.Builder()
                // 64MB is a reasonable value that allows to amortize the cost of opening files,
                // but is not so large as to exhaust a typical runner's maximum amount of output per
                // ProcessElement call.
                .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
                .build();
    }

    /**
     * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
     * matching a sharding pattern), with each element of the input collection encoded into its own
     * line.
     */

    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//    public static com.impact.utils.io.TextIO.Write write() {
//        return new com.impact.utils.io.TextIO.Write();
//    }
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    /**
     * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
     * matching a sharding pattern), with each element of the input collection encoded into its own
     * line.
     *
     * <p>This version allows you to apply {@link org.apache.beam.sdk.io.TextIO} writes to a PCollection of a custom type
     * {@link UserT}. A format mechanism that converts the input type {@link UserT} to the String that
     * will be written to the file must be specified. If using a custom {@link DynamicDestinations}
     * object this is done using {@link DynamicDestinations#formatRecord}, otherwise the {@link
     * org.apache.beam.sdk.io.TextIO.TypedWrite#withFormatFunction} can be used to specify a format function.
     *
     * <p>The advantage of using a custom type is that is it allows a user-provided {@link
     * DynamicDestinations} object, set via {@link org.apache.beam.sdk.io.TextIO.Write#to(DynamicDestinations)} to examine the
     * custom type when choosing a destination.
     */

    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//    public static <UserT> com.impact.utils.io.TextIO.TypedWrite<UserT, Void> writeCustomType() {
//        return new AutoValue_TextIO_TypedWrite.Builder<UserT, Void>()
//                .setFilenamePrefix(null)
//                .setTempDirectory(null)
//                .setShardTemplate(null)
//                .setFilenameSuffix(null)
//                .setFilenamePolicy(null)
//                .setDynamicDestinations(null)
//                .setDelimiter(new char[] {'\n'})
//                .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
//                .setWindowedWrites(false)
//                .setNoSpilling(false)
//                .setSkipIfEmpty(false)
//                .build();
//    }
    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    /** Implementation of {@link #read}. */
    @AutoValue
    public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

        abstract @Nullable ValueProvider<String> getFilepattern();

        abstract MatchConfiguration getMatchConfiguration();

        abstract boolean getHintMatchesManyFiles();

        abstract Compression getCompression();

        @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
        abstract byte @Nullable [] getDelimiter();

        abstract com.impact.utils.io.TextIO.Read.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.TextIO.Read.Builder setFilepattern(ValueProvider<String> filepattern);

            abstract com.impact.utils.io.TextIO.Read.Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

            abstract com.impact.utils.io.TextIO.Read.Builder setHintMatchesManyFiles(boolean hintManyFiles);

            abstract com.impact.utils.io.TextIO.Read.Builder setCompression(Compression compression);

            abstract com.impact.utils.io.TextIO.Read.Builder setDelimiter(byte @Nullable [] delimiter);

            abstract com.impact.utils.io.TextIO.Read build();
        }

        /**
         * Reads text files that reads from the file(s) with the given filename or filename pattern.
         *
         * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
         * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
         * remote execution service).
         *
         * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
         * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
         *
         * <p>If it is known that the filepattern will match a very large number of files (at least tens
         * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
         */
        public com.impact.utils.io.TextIO.Read from(String filepattern) {
            checkArgument(filepattern != null, "filepattern can not be null");
            return from(StaticValueProvider.of(filepattern));
        }

        /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
        public com.impact.utils.io.TextIO.Read from(ValueProvider<String> filepattern) {
            checkArgument(filepattern != null, "filepattern can not be null");
            return toBuilder().setFilepattern(filepattern).build();
        }

        /** Sets the {@link MatchConfiguration}. */
        public com.impact.utils.io.TextIO.Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
            return toBuilder().setMatchConfiguration(matchConfiguration).build();
        }

        /** @deprecated Use {@link #withCompression}. */
        @Deprecated
        public com.impact.utils.io.TextIO.Read withCompressionType(com.impact.utils.io.TextIO.CompressionType compressionType) {
            return withCompression(compressionType.canonical);
        }

        /**
         * Reads from input sources using the specified compression type.
         *
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public com.impact.utils.io.TextIO.Read withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//        public com.impact.utils.io.TextIO.Read watchForNewFiles(
//                Duration pollInterval,
//                TerminationCondition<String, ?> terminationCondition,
//                boolean matchUpdatedFiles) {
//            return withMatchConfiguration(
//                    getMatchConfiguration()
//                            .continuously(pollInterval, terminationCondition, matchUpdatedFiles));
//        }


//        public com.impact.utils.io.TextIO.Read watchForNewFiles(
//                Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
//            return watchForNewFiles(pollInterval, terminationCondition, false);
//        }

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        /**
         * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
         * files.
         *
         * <p>This hint may cause a runner to execute the transform differently, in a way that improves
         * performance for this case, but it may worsen performance if the filepattern matches only a
         * small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
         * happen less efficiently within individual files).
         */
        public com.impact.utils.io.TextIO.Read withHintMatchesManyFiles() {
            return toBuilder().setHintMatchesManyFiles(true).build();
        }

        /** See {@link MatchConfiguration#withEmptyMatchTreatment}. */
        public com.impact.utils.io.TextIO.Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }

        /** Set the custom delimiter to be used in place of the default ones ('\r', '\n' or '\r\n'). */
        public com.impact.utils.io.TextIO.Read withDelimiter(byte[] delimiter) {
            checkArgument(delimiter != null, "delimiter can not be null");
            checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
            return toBuilder().setDelimiter(delimiter).build();
        }

        static boolean isSelfOverlapping(byte[] s) {
            // s self-overlaps if v exists such as s = vu = wv with u and w non empty
            for (int i = 1; i < s.length - 1; ++i) {
                if (ByteBuffer.wrap(s, 0, i).equals(ByteBuffer.wrap(s, s.length - i, i))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            checkNotNull(getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
            if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
                LOG.info("$$$$$ Not using hintMatchesManyFiles()");
                return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
                //return input.apply("Read", ReadWithMetadata.from(getSource()));
            }

            // All other cases go through FileIO + ReadFiles
            LOG.info(">>>> Using hintMatchesManyFiles()");
            return input
                    .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                    .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                    .apply(
                            "Read Matches",
                            FileIO.readMatches()
                                    .withCompression(getCompression())
                                    .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
                    .apply("Via ReadFiles", readFiles().withDelimiter(getDelimiter()));
        }

        // Helper to create a source specific to the requested compression type.
        protected FileBasedSource<String> getSource() {
            return CompressedSource.from(
                            new TextSource(
                                    getFilepattern(),
                                    getMatchConfiguration().getEmptyMatchTreatment(),
                                    getDelimiter()))
                    .withCompression(getCompression());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .add(
                            DisplayData.item("compressionType", getCompression().toString())
                                    .withLabel("Compression Type"))
                    .addIfNotNull(DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"))
                    .include("matchConfiguration", getMatchConfiguration())
                    .addIfNotNull(
                            DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                                    .withLabel("Custom delimiter to split records"));
        }
    }

    /////////////////////////////////////////////////////////////////////////////

    @AutoValue
    public abstract static class ReadWithMetadata extends PTransform<PBegin, PCollection<KV<String, String>>> {

        abstract @Nullable ValueProvider<String> getFilepattern();

        abstract MatchConfiguration getMatchConfiguration();

        abstract boolean getHintMatchesManyFiles();

        abstract Compression getCompression();

        @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
        abstract byte @Nullable [] getDelimiter();

        abstract com.impact.utils.io.TextIO.ReadWithMetadata.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.TextIO.ReadWithMetadata.Builder setFilepattern(ValueProvider<String> filepattern);

            abstract com.impact.utils.io.TextIO.ReadWithMetadata.Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

            abstract com.impact.utils.io.TextIO.ReadWithMetadata.Builder setHintMatchesManyFiles(boolean hintManyFiles);

            abstract com.impact.utils.io.TextIO.ReadWithMetadata.Builder setCompression(Compression compression);

            abstract com.impact.utils.io.TextIO.ReadWithMetadata.Builder setDelimiter(byte @Nullable [] delimiter);

            abstract com.impact.utils.io.TextIO.ReadWithMetadata build();
        }

        /**
         * Reads text files that reads from the file(s) with the given filename or filename pattern.
         *
         * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
         * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
         * remote execution service).
         *
         * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
         * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
         *
         * <p>If it is known that the filepattern will match a very large number of files (at least tens
         * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
         */
        public com.impact.utils.io.TextIO.ReadWithMetadata from(String filepattern) {
            checkArgument(filepattern != null, "filepattern can not be null");
            return from(StaticValueProvider.of(filepattern));
        }

        /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
        public com.impact.utils.io.TextIO.ReadWithMetadata from(ValueProvider<String> filepattern) {
            checkArgument(filepattern != null, "filepattern can not be null");
            return toBuilder().setFilepattern(filepattern).build();
        }

        /** Sets the {@link MatchConfiguration}. */
        public com.impact.utils.io.TextIO.ReadWithMetadata withMatchConfiguration(MatchConfiguration matchConfiguration) {
            return toBuilder().setMatchConfiguration(matchConfiguration).build();
        }

        /** @deprecated Use {@link #withCompression}. */
        @Deprecated
        public com.impact.utils.io.TextIO.ReadWithMetadata withCompressionType(com.impact.utils.io.TextIO.CompressionType compressionType) {
            return withCompression(compressionType.canonical);
        }

        /**
         * Reads from input sources using the specified compression type.
         *
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public com.impact.utils.io.TextIO.ReadWithMetadata withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//        public com.impact.utils.io.TextIO.Read watchForNewFiles(
//                Duration pollInterval,
//                TerminationCondition<String, ?> terminationCondition,
//                boolean matchUpdatedFiles) {
//            return withMatchConfiguration(
//                    getMatchConfiguration()
//                            .continuously(pollInterval, terminationCondition, matchUpdatedFiles));
//        }


//        public com.impact.utils.io.TextIO.Read watchForNewFiles(
//                Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
//            return watchForNewFiles(pollInterval, terminationCondition, false);
//        }

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        /**
         * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
         * files.
         *
         * <p>This hint may cause a runner to execute the transform differently, in a way that improves
         * performance for this case, but it may worsen performance if the filepattern matches only a
         * small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
         * happen less efficiently within individual files).
         */
        public com.impact.utils.io.TextIO.ReadWithMetadata withHintMatchesManyFiles() {
            return toBuilder().setHintMatchesManyFiles(true).build();
        }

        /** See {@link MatchConfiguration#withEmptyMatchTreatment}. */
        public com.impact.utils.io.TextIO.ReadWithMetadata withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }

        /** Set the custom delimiter to be used in place of the default ones ('\r', '\n' or '\r\n'). */
        public com.impact.utils.io.TextIO.ReadWithMetadata withDelimiter(byte[] delimiter) {
            checkArgument(delimiter != null, "delimiter can not be null");
            checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
            return toBuilder().setDelimiter(delimiter).build();
        }

        static boolean isSelfOverlapping(byte[] s) {
            // s self-overlaps if v exists such as s = vu = wv with u and w non empty
            for (int i = 1; i < s.length - 1; ++i) {
                if (ByteBuffer.wrap(s, 0, i).equals(ByteBuffer.wrap(s, s.length - i, i))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public PCollection<KV<String, String>> expand(PBegin input) {
            checkNotNull(getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
            if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
                LOG.info("$$$$$ Not using hintMatchesManyFiles()");
                //return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
                //return input.apply("Read", ReadWithMetadata.from(getSource()));
                //TODO: Replace org.apache.beam.sdk.io.Read with com.impact.utils.io.Read and use return similar to above instead of below
                return input
                        .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                        .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                        .apply(
                                "Read Matches",
                                FileIO.readMatches()
                                        .withCompression(getCompression())
                                        .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
                        .apply("Via ReadFiles", readFilesWithMetadata().withDelimiter(getDelimiter()));
            }

            // All other cases go through FileIO + ReadFiles
            LOG.info(">>>> Using hintMatchesManyFiles()");
            return input
                    .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                    .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                    .apply(
                            "Read Matches",
                            FileIO.readMatches()
                                    .withCompression(getCompression())
                                    .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
                    .apply("Via ReadFiles", readFilesWithMetadata().withDelimiter(getDelimiter()));
        }

        // Helper to create a source specific to the requested compression type.
        protected FileBasedSource<String> getSource() {
            return CompressedSource.from(
                            new TextSource(
                                    getFilepattern(),
                                    getMatchConfiguration().getEmptyMatchTreatment(),
                                    getDelimiter()))
                    .withCompression(getCompression());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .add(
                            DisplayData.item("compressionType", getCompression().toString())
                                    .withLabel("Compression Type"))
                    .addIfNotNull(DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"))
                    .include("matchConfiguration", getMatchConfiguration())
                    .addIfNotNull(
                            DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                                    .withLabel("Custom delimiter to split records"));
        }
    }

    /////////////////////////////////////////////////////////////////////////////
    /**
     * Implementation of {@link #readAll}.
     *
     * @deprecated See {@link #readAll()} for details.
     */
    @Deprecated
    @AutoValue
    public abstract static class ReadAll
            extends PTransform<PCollection<String>, PCollection<String>> {
        abstract MatchConfiguration getMatchConfiguration();

        abstract Compression getCompression();

        @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
        abstract byte @Nullable [] getDelimiter();

        abstract com.impact.utils.io.TextIO.ReadAll.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.TextIO.ReadAll.Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

            abstract com.impact.utils.io.TextIO.ReadAll.Builder setCompression(Compression compression);

            abstract com.impact.utils.io.TextIO.ReadAll.Builder setDelimiter(byte @Nullable [] delimiter);

            abstract com.impact.utils.io.TextIO.ReadAll build();
        }

        /** Sets the {@link MatchConfiguration}. */
        public com.impact.utils.io.TextIO.ReadAll withMatchConfiguration(MatchConfiguration configuration) {
            return toBuilder().setMatchConfiguration(configuration).build();
        }

        /** @deprecated Use {@link #withCompression}. */
        @Deprecated
        public com.impact.utils.io.TextIO.ReadAll withCompressionType(com.impact.utils.io.TextIO.CompressionType compressionType) {
            return withCompression(compressionType.canonical);
        }

        /**
         * Reads from input sources using the specified compression type.
         *
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public com.impact.utils.io.TextIO.ReadAll withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        /** Same as {@link org.apache.beam.sdk.io.TextIO.Read#withEmptyMatchTreatment}. */
        public com.impact.utils.io.TextIO.ReadAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }

        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//        public com.impact.utils.io.TextIO.ReadAll watchForNewFiles(
//                Duration pollInterval,
//                TerminationCondition<String, ?> terminationCondition,
//                boolean matchUpdatedFiles) {
//            return withMatchConfiguration(
//                    getMatchConfiguration()
//                            .continuously(pollInterval, terminationCondition, matchUpdatedFiles));
//        }
//
//        /** Same as {@link org.apache.beam.sdk.io.TextIO.Read#watchForNewFiles(Duration, TerminationCondition)}. */
//        public com.impact.utils.io.TextIO.ReadAll watchForNewFiles(
//                Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
//            return watchForNewFiles(pollInterval, terminationCondition, false);
//        }
        // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        com.impact.utils.io.TextIO.ReadAll withDelimiter(byte[] delimiter) {
            return toBuilder().setDelimiter(delimiter).build();
        }

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            return input
                    .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                    .apply(
                            FileIO.readMatches()
                                    .withCompression(getCompression())
                                    .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
                    .apply(readFiles().withDelimiter(getDelimiter()));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .add(
                            DisplayData.item("compressionType", getCompression().toString())
                                    .withLabel("Compression Type"))
                    .addIfNotNull(
                            DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                                    .withLabel("Custom delimiter to split records"))
                    .include("matchConfiguration", getMatchConfiguration());
        }
    }

    /** Implementation of {@link #readFiles}. */
    @AutoValue
    public abstract static class ReadFiles
            extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<String>> {
        abstract long getDesiredBundleSizeBytes();

        @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
        abstract byte @Nullable [] getDelimiter();

        abstract com.impact.utils.io.TextIO.ReadFiles.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.TextIO.ReadFiles.Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

            abstract com.impact.utils.io.TextIO.ReadFiles.Builder setDelimiter(byte @Nullable [] delimiter);

            abstract com.impact.utils.io.TextIO.ReadFiles build();
        }

        @VisibleForTesting
        com.impact.utils.io.TextIO.ReadFiles withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
            return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
        }

        /** Like {@link org.apache.beam.sdk.io.TextIO.Read#withDelimiter}. */
        public com.impact.utils.io.TextIO.ReadFiles withDelimiter(byte[] delimiter) {
            return toBuilder().setDelimiter(delimiter).build();
        }

        @Override
        public PCollection<String> expand(PCollection<FileIO.ReadableFile> input) {
            //            LOG.info("######### Read Files Initialized #########");
//            return input.apply(
//                    "Read all via FileBasedSource",
//                    new ReadAllViaFileBasedSourceWithMetadata<>(
//                            getDesiredBundleSizeBytes(),
//                            new com.impact.utils.io.TextIO.ReadFiles.CreateTextSourceFn(getDelimiter()),
//                            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
//                    .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
//                        @ProcessElement
//                        public void processElement(@Element KV<String, String> element, ProcessContext context) {
//                            context.output(element.getKey());
//                        }
//                    }));
            return input.apply(
                    "Read all via FileBasedSource",
                    new ReadAllViaFileBasedSource<>(
                            getDesiredBundleSizeBytes(),
                            new com.impact.utils.io.TextIO.ReadFiles.CreateTextSourceFn(getDelimiter()),
                            StringUtf8Coder.of()));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.addIfNotNull(
                    DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                            .withLabel("Custom delimiter to split records"));
        }

        private static class CreateTextSourceFn
                implements SerializableFunction<String, FileBasedSource<String>> {
            private byte[] delimiter;

            private CreateTextSourceFn(byte[] delimiter) {
                this.delimiter = delimiter;
            }

            @Override
            public FileBasedSource<String> apply(String input) {
                return new TextSource(
                        StaticValueProvider.of(input), EmptyMatchTreatment.DISALLOW, delimiter);
            }
        }
    }

    // ///////////////////////////////////////////////////////////////////////////

    @AutoValue
    public abstract static class ReadFilesWithMetadata
            extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<KV<String, String>>> {
        abstract long getDesiredBundleSizeBytes();

        @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
        abstract byte @Nullable [] getDelimiter();

        abstract com.impact.utils.io.TextIO.ReadFilesWithMetadata.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.TextIO.ReadFilesWithMetadata.Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

            abstract com.impact.utils.io.TextIO.ReadFilesWithMetadata.Builder setDelimiter(byte @Nullable [] delimiter);

            abstract com.impact.utils.io.TextIO.ReadFilesWithMetadata build();
        }

        @VisibleForTesting
        com.impact.utils.io.TextIO.ReadFilesWithMetadata withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
            return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
        }

        /** Like {@link org.apache.beam.sdk.io.TextIO.Read#withDelimiter}. */
        public com.impact.utils.io.TextIO.ReadFilesWithMetadata withDelimiter(byte[] delimiter) {
            return toBuilder().setDelimiter(delimiter).build();
        }

        @Override
        public PCollection<KV<String, String>> expand(PCollection<FileIO.ReadableFile> input) {
            LOG.info("######### ReadFilesWithMetadata Initialized #########");
            return input.apply(
                            "Read all via FileBasedSource",
                            new ReadAllViaFileBasedSourceWithMetadata<>(
                                    getDesiredBundleSizeBytes(),
                                    new com.impact.utils.io.TextIO.ReadFiles.CreateTextSourceFn(getDelimiter()),
                                    KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.addIfNotNull(
                    DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                            .withLabel("Custom delimiter to split records"));
        }

        private static class CreateTextSourceFn
                implements SerializableFunction<String, FileBasedSource<String>> {
            private byte[] delimiter;

            private CreateTextSourceFn(byte[] delimiter) {
                this.delimiter = delimiter;
            }

            @Override
            public FileBasedSource<String> apply(String input) {
                return new TextSource(
                        StaticValueProvider.of(input), EmptyMatchTreatment.DISALLOW, delimiter);
            }
        }
    }

    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    /** Implementation of {@link #write}. */
//    @AutoValue
//    public abstract static class TypedWrite<UserT, DestinationT>
//            extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
//
//        /** The prefix of each file written, combined with suffix and shardTemplate. */
//        abstract @Nullable ValueProvider<ResourceId> getFilenamePrefix();
//
//        /** The suffix of each file written, combined with prefix and shardTemplate. */
//        abstract @Nullable String getFilenameSuffix();
//
//        /** The base directory used for generating temporary files. */
//        abstract @Nullable ValueProvider<ResourceId> getTempDirectory();
//
//        /** The delimiter between string records. */
//        @SuppressWarnings("mutable") // this returns an array that can be mutated by the caller
//        abstract char[] getDelimiter();
//
//        /** An optional header to add to each file. */
//        abstract @Nullable String getHeader();
//
//        /** An optional footer to add to each file. */
//        abstract @Nullable String getFooter();
//
//        /** Requested number of shards. 0 for automatic. */
//        abstract @Nullable ValueProvider<Integer> getNumShards();
//
//        /** The shard template of each file written, combined with prefix and suffix. */
//        abstract @Nullable String getShardTemplate();
//
//        /** A policy for naming output files. */
//        abstract @Nullable FilenamePolicy getFilenamePolicy();
//
//        /** Allows for value-dependent {@link DynamicDestinations} to be vended. */
//        abstract @Nullable DynamicDestinations<UserT, DestinationT, String> getDynamicDestinations();
//
//        /** A destination function for using {@link DefaultFilenamePolicy}. */
//        abstract @Nullable SerializableFunction<UserT, Params> getDestinationFunction();
//
//        /** A default destination for empty PCollections. */
//        abstract @Nullable Params getEmptyDestination();
//
//        /** A function that converts UserT to a String, for writing to the file. */
//        abstract @Nullable SerializableFunction<UserT, String> getFormatFunction();
//
//        /** Whether to write windowed output files. */
//        abstract boolean getWindowedWrites();
//
//        /** Whether to skip the spilling of data caused by having maxNumWritersPerBundle. */
//        abstract boolean getNoSpilling();
//
//        /** Whether to skip writing any output files if the PCollection is empty. */
//        abstract boolean getSkipIfEmpty();
//
//        /**
//         * The {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
//         * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
//         */
//        abstract WritableByteChannelFactory getWritableByteChannelFactory();
//
//        abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> toBuilder();
//
//        @AutoValue.Builder
//        abstract static class Builder<UserT, DestinationT> {
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setFilenamePrefix(
//                    @Nullable ValueProvider<ResourceId> filenamePrefix);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setTempDirectory(
//                    @Nullable ValueProvider<ResourceId> tempDirectory);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setShardTemplate(@Nullable String shardTemplate);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setFilenameSuffix(@Nullable String filenameSuffix);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setHeader(@Nullable String header);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setFooter(@Nullable String footer);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setDelimiter(char[] delimiter);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setFilenamePolicy(
//                    @Nullable FilenamePolicy filenamePolicy);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setDynamicDestinations(
//                    @Nullable DynamicDestinations<UserT, DestinationT, String> dynamicDestinations);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setDestinationFunction(
//                    @Nullable SerializableFunction<UserT, Params> destinationFunction);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setEmptyDestination(Params emptyDestination);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setFormatFunction(
//                    @Nullable SerializableFunction<UserT, String> formatFunction);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setNumShards(
//                    @Nullable ValueProvider<Integer> numShards);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setWindowedWrites(boolean windowedWrites);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setNoSpilling(boolean noSpilling);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setSkipIfEmpty(boolean noSpilling);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite.Builder<UserT, DestinationT> setWritableByteChannelFactory(
//                    WritableByteChannelFactory writableByteChannelFactory);
//
//            abstract com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> build();
//        }
//
//        /**
//         * Writes to text files with the given prefix. The given {@code prefix} can reference any {@link
//         * FileSystem} on the classpath. This prefix is used by the {@link DefaultFilenamePolicy} to
//         * generate filenames.
//         *
//         * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
//         * to define the base output directory and file prefix, a shard identifier (see {@link
//         * #withNumShards(int)}), and a common suffix (if supplied using {@link #withSuffix(String)}).
//         *
//         * <p>This default policy can be overridden using {@link #to(FilenamePolicy)}, in which case
//         * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
//         * Custom filename policies do not automatically see this prefix - you should explicitly pass
//         * the prefix into your {@link FilenamePolicy} object if you need this.
//         *
//         * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
//         * infer a directory for temporary files.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> to(String filenamePrefix) {
//            return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
//        }
//
//        /** Like {@link #to(String)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> to(ResourceId filenamePrefix) {
//            return toResource(StaticValueProvider.of(filenamePrefix));
//        }
//
//        /** Like {@link #to(String)}. */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> to(ValueProvider<String> outputPrefix) {
//            return toResource(
//                    NestedValueProvider.of(outputPrefix, FileBasedSink::convertToFileResourceIfPossible));
//        }
//
//        /**
//         * Writes to files named according to the given {@link FileBasedSink.FilenamePolicy}. A
//         * directory for temporary files must be specified using {@link #withTempDirectory}.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> to(FilenamePolicy filenamePolicy) {
//            return toBuilder().setFilenamePolicy(filenamePolicy).build();
//        }
//
//        /**
//         * Use a {@link DynamicDestinations} object to vend {@link FilenamePolicy} objects. These
//         * objects can examine the input record when creating a {@link FilenamePolicy}. A directory for
//         * temporary files must be specified using {@link #withTempDirectory}.
//         *
//         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} with {@link #sink()}
//         *     instead.
//         */
//        @Deprecated
//        public <NewDestinationT> com.impact.utils.io.TextIO.TypedWrite<UserT, NewDestinationT> to(
//                DynamicDestinations<UserT, NewDestinationT, String> dynamicDestinations) {
//            return (com.impact.utils.io.TextIO.TypedWrite)
//                    toBuilder().setDynamicDestinations((DynamicDestinations) dynamicDestinations).build();
//        }
//
//        /**
//         * Write to dynamic destinations using the default filename policy. The destinationFunction maps
//         * the input record to a {@link DefaultFilenamePolicy.Params} object that specifies where the
//         * records should be written (base filename, file suffix, and shard template). The
//         * emptyDestination parameter specified where empty files should be written for when the written
//         * {@link PCollection} is empty.
//         *
//         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} with {@link #sink()}
//         *     instead.
//         */
//        @Deprecated
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, Params> to(
//                SerializableFunction<UserT, Params> destinationFunction, Params emptyDestination) {
//            return (com.impact.utils.io.TextIO.TypedWrite)
//                    toBuilder()
//                            .setDestinationFunction(destinationFunction)
//                            .setEmptyDestination(emptyDestination)
//                            .build();
//        }
//
//        /** Like {@link #to(ResourceId)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> toResource(ValueProvider<ResourceId> filenamePrefix) {
//            return toBuilder().setFilenamePrefix(filenamePrefix).build();
//        }
//
//        /**
//         * Specifies a format function to convert {@link UserT} to the output type. If {@link
//         * #to(DynamicDestinations)} is used, {@link DynamicDestinations#formatRecord(Object)} must be
//         * used instead.
//         *
//         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} with {@link #sink()}
//         *     instead.
//         */
//        @Deprecated
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withFormatFunction(
//                @Nullable SerializableFunction<UserT, String> formatFunction) {
//            return toBuilder().setFormatFunction(formatFunction).build();
//        }
//
//        /** Set the base directory used to generate temporary files. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withTempDirectory(
//                ValueProvider<ResourceId> tempDirectory) {
//            return toBuilder().setTempDirectory(tempDirectory).build();
//        }
//
//        /** Set the base directory used to generate temporary files. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withTempDirectory(ResourceId tempDirectory) {
//            return withTempDirectory(StaticValueProvider.of(tempDirectory));
//        }
//
//        /**
//         * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
//         * used when using one of the default filename-prefix to() overrides - i.e. not when using
//         * either {@link #to(FilenamePolicy)} or {@link #to(DynamicDestinations)}.
//         *
//         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
//         * used.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withShardNameTemplate(String shardTemplate) {
//            return toBuilder().setShardTemplate(shardTemplate).build();
//        }
//
//        /**
//         * Configures the filename suffix for written files. This option may only be used when using one
//         * of the default filename-prefix to() overrides - i.e. not when using either {@link
//         * #to(FilenamePolicy)} or {@link #to(DynamicDestinations)}.
//         *
//         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
//         * used.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withSuffix(String filenameSuffix) {
//            return toBuilder().setFilenameSuffix(filenameSuffix).build();
//        }
//
//        /**
//         * Configures the number of output shards produced overall (when using unwindowed writes) or
//         * per-window (when using windowed writes).
//         *
//         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
//         * performance of a pipeline. Setting this value is not recommended unless you require a
//         * specific number of output files.
//         *
//         * @param numShards the number of shards to use, or 0 to let the system decide.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withNumShards(int numShards) {
//            checkArgument(numShards >= 0);
//            if (numShards == 0) {
//                // If 0 shards are passed, then the user wants runner-determined
//                // sharding to kick in, thus we pass a null StaticValueProvider
//                // so that the runner-determined-sharding path will be activated.
//                return withNumShards(null);
//            } else {
//                return withNumShards(StaticValueProvider.of(numShards));
//            }
//        }
//
//        /**
//         * Like {@link #withNumShards(int)}. Specifying {@code null} means runner-determined sharding.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withNumShards(
//                @Nullable ValueProvider<Integer> numShards) {
//            return toBuilder().setNumShards(numShards).build();
//        }
//
//        /**
//         * Forces a single file as output and empty shard name template.
//         *
//         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
//         * performance of a pipeline. Setting this value is not recommended unless you require a
//         * specific number of output files.
//         *
//         * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withoutSharding() {
//            return withNumShards(1).withShardNameTemplate("");
//        }
//
//        /**
//         * Specifies the delimiter after each string written.
//         *
//         * <p>Defaults to '\n'.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withDelimiter(char[] delimiter) {
//            return toBuilder().setDelimiter(delimiter).build();
//        }
//
//        /**
//         * Adds a header string to each file. A newline after the header is added automatically.
//         *
//         * <p>A {@code null} value will clear any previously configured header.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withHeader(@Nullable String header) {
//            return toBuilder().setHeader(header).build();
//        }
//
//        /**
//         * Adds a footer string to each file. A newline after the footer is added automatically.
//         *
//         * <p>A {@code null} value will clear any previously configured footer.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withFooter(@Nullable String footer) {
//            return toBuilder().setFooter(footer).build();
//        }
//
//        /**
//         * Returns a transform for writing to text files like this one but that has the given {@link
//         * WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output. The
//         * default is value is {@link Compression#UNCOMPRESSED}.
//         *
//         * <p>A {@code null} value will reset the value to the default value mentioned above.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withWritableByteChannelFactory(
//                WritableByteChannelFactory writableByteChannelFactory) {
//            return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
//        }
//
//        /**
//         * Returns a transform for writing to text files like this one but that compresses output using
//         * the given {@link Compression}. The default value is {@link Compression#UNCOMPRESSED}.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withCompression(Compression compression) {
//            checkArgument(compression != null, "compression can not be null");
//            return withWritableByteChannelFactory(
//                    FileBasedSink.CompressionType.fromCanonical(compression));
//        }
//
//        /**
//         * Preserves windowing of input elements and writes them to files based on the element's window.
//         *
//         * <p>If using {@link #to(FileBasedSink.FilenamePolicy)}. Filenames will be generated using
//         * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
//         */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withWindowedWrites() {
//            return toBuilder().setWindowedWrites(true).build();
//        }
//
//        /** See {@link WriteFiles#withNoSpilling()}. */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> withNoSpilling() {
//            return toBuilder().setNoSpilling(true).build();
//        }
//
//        /** Don't write any output files if the PCollection is empty. */
//        public com.impact.utils.io.TextIO.TypedWrite<UserT, DestinationT> skipIfEmpty() {
//            return toBuilder().setSkipIfEmpty(true).build();
//        }
//
//        private DynamicDestinations<UserT, DestinationT, String> resolveDynamicDestinations() {
//            DynamicDestinations<UserT, DestinationT, String> dynamicDestinations =
//                    getDynamicDestinations();
//            if (dynamicDestinations == null) {
//                if (getDestinationFunction() != null) {
//                    // In this case, DestinationT == Params
//                    dynamicDestinations =
//                            (DynamicDestinations)
//                                    DynamicFileDestinations.toDefaultPolicies(
//                                            getDestinationFunction(), getEmptyDestination(), getFormatFunction());
//                } else {
//                    // In this case, DestinationT == Void
//                    FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
//                    if (usedFilenamePolicy == null) {
//                        usedFilenamePolicy =
//                                DefaultFilenamePolicy.fromStandardParameters(
//                                        getFilenamePrefix(),
//                                        getShardTemplate(),
//                                        getFilenameSuffix(),
//                                        getWindowedWrites());
//                    }
//                    dynamicDestinations =
//                            (DynamicDestinations)
//                                    DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction());
//                }
//            }
//            return dynamicDestinations;
//        }
//
//        @Override
//        public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
//            checkState(
//                    getFilenamePrefix() != null || getTempDirectory() != null,
//                    "Need to set either the filename prefix or the tempDirectory of a TextIO.Write "
//                            + "transform.");
//
//            List<?> allToArgs =
//                    Lists.newArrayList(
//                            getFilenamePolicy(),
//                            getDynamicDestinations(),
//                            getFilenamePrefix(),
//                            getDestinationFunction());
//            checkArgument(
//                    1
//                            == Iterables.size(
//                            allToArgs.stream()
//                                    .filter(Predicates.notNull()::apply)
//                                    .collect(Collectors.toList())),
//                    "Exactly one of filename policy, dynamic destinations, filename prefix, or destination "
//                            + "function must be set");
//
//            if (getDynamicDestinations() != null) {
//                checkArgument(
//                        getFormatFunction() == null,
//                        "A format function should not be specified "
//                                + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
//            }
//            if (getFilenamePolicy() != null || getDynamicDestinations() != null) {
//                checkState(
//                        getShardTemplate() == null && getFilenameSuffix() == null,
//                        "shardTemplate and filenameSuffix should only be used with the default "
//                                + "filename policy");
//            }
//            ValueProvider<ResourceId> tempDirectory = getTempDirectory();
//            if (tempDirectory == null) {
//                tempDirectory = getFilenamePrefix();
//            }
//            WriteFiles<UserT, DestinationT, String> write =
//                    WriteFiles.to(
//                            new TextSink<>(
//                                    tempDirectory,
//                                    resolveDynamicDestinations(),
//                                    getDelimiter(),
//                                    getHeader(),
//                                    getFooter(),
//                                    getWritableByteChannelFactory()));
//            if (getNumShards() != null) {
//                write = write.withNumShards(getNumShards());
//            }
//            if (getWindowedWrites()) {
//                write = write.withWindowedWrites();
//            }
//            if (getNoSpilling()) {
//                write = write.withNoSpilling();
//            }
//            if (getSkipIfEmpty()) {
//                write = write.withSkipIfEmpty();
//            }
//            return input.apply("WriteFiles", write);
//        }
//
//        @Override
//        public void populateDisplayData(DisplayData.Builder builder) {
//            super.populateDisplayData(builder);
//
//            resolveDynamicDestinations().populateDisplayData(builder);
//            builder
//                    .addIfNotNull(
//                            DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"))
//                    .addIfNotNull(
//                            DisplayData.item("tempDirectory", getTempDirectory())
//                                    .withLabel("Directory for temporary files"))
//                    .addIfNotNull(DisplayData.item("fileHeader", getHeader()).withLabel("File Header"))
//                    .addIfNotNull(DisplayData.item("fileFooter", getFooter()).withLabel("File Footer"))
//                    .add(
//                            DisplayData.item(
//                                            "writableByteChannelFactory", getWritableByteChannelFactory().toString())
//                                    .withLabel("Compression/Transformation Type"));
//        }
//    }
//
//    /**
//     * This class is used as the default return value of {@link org.apache.beam.sdk.io.TextIO#write()}.
//     *
//     * <p>All methods in this class delegate to the appropriate method of {@link org.apache.beam.sdk.io.TextIO.TypedWrite}.
//     * This class exists for backwards compatibility, and will be removed in Beam 3.0.
//     */
//    public static class Write extends PTransform<PCollection<String>, PDone> {
//        @VisibleForTesting
//        com.impact.utils.io.TextIO.TypedWrite<String, ?> inner;
//
//        Write() {
//            this(com.impact.utils.io.TextIO.writeCustomType());
//        }
//
//        Write(com.impact.utils.io.TextIO.TypedWrite<String, ?> inner) {
//            this.inner = inner;
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#to(String)}. */
//        public com.impact.utils.io.TextIO.Write to(String filenamePrefix) {
//            return new com.impact.utils.io.TextIO.Write(
//                    inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#to(ResourceId)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.Write to(ResourceId filenamePrefix) {
//            return new com.impact.utils.io.TextIO.Write(
//                    inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#to(ValueProvider)}. */
//        public com.impact.utils.io.TextIO.Write to(ValueProvider<String> outputPrefix) {
//            return new com.impact.utils.io.TextIO.Write(inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#toResource(ValueProvider)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.Write toResource(ValueProvider<ResourceId> filenamePrefix) {
//            return new com.impact.utils.io.TextIO.Write(
//                    inner.toResource(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#to(FilenamePolicy)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.Write to(FilenamePolicy filenamePolicy) {
//            return new com.impact.utils.io.TextIO.Write(
//                    inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.identity()));
//        }
//
//        /**
//         * See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#to(DynamicDestinations)}.
//         *
//         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} ()} with {@link
//         *     #sink()} instead.
//         */
//        @Experimental(Kind.FILESYSTEM)
//        @Deprecated
//        public com.impact.utils.io.TextIO.Write to(DynamicDestinations<String, ?, String> dynamicDestinations) {
//            return new com.impact.utils.io.TextIO.Write(
//                    inner.to((DynamicDestinations) dynamicDestinations).withFormatFunction(null));
//        }
//
//        /**
//         * See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#to(SerializableFunction, Params)}.
//         *
//         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} ()} with {@link
//         *     #sink()} instead.
//         */
//        @Experimental(Kind.FILESYSTEM)
//        @Deprecated
//        public com.impact.utils.io.TextIO.Write to(
//                SerializableFunction<String, Params> destinationFunction, Params emptyDestination) {
//            return new com.impact.utils.io.TextIO.Write(
//                    inner
//                            .to(destinationFunction, emptyDestination)
//                            .withFormatFunction(SerializableFunctions.identity()));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withTempDirectory(ValueProvider)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.Write withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
//            return new com.impact.utils.io.TextIO.Write(inner.withTempDirectory(tempDirectory));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withTempDirectory(ResourceId)}. */
//        @Experimental(Kind.FILESYSTEM)
//        public com.impact.utils.io.TextIO.Write withTempDirectory(ResourceId tempDirectory) {
//            return new com.impact.utils.io.TextIO.Write(inner.withTempDirectory(tempDirectory));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withShardNameTemplate(String)}. */
//        public com.impact.utils.io.TextIO.Write withShardNameTemplate(String shardTemplate) {
//            return new com.impact.utils.io.TextIO.Write(inner.withShardNameTemplate(shardTemplate));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withSuffix(String)}. */
//        public com.impact.utils.io.TextIO.Write withSuffix(String filenameSuffix) {
//            return new com.impact.utils.io.TextIO.Write(inner.withSuffix(filenameSuffix));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withNumShards(int)}. */
//        public com.impact.utils.io.TextIO.Write withNumShards(int numShards) {
//            return new com.impact.utils.io.TextIO.Write(inner.withNumShards(numShards));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withNumShards(ValueProvider)}. */
//        public com.impact.utils.io.TextIO.Write withNumShards(@Nullable ValueProvider<Integer> numShards) {
//            return new com.impact.utils.io.TextIO.Write(inner.withNumShards(numShards));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withoutSharding()}. */
//        public com.impact.utils.io.TextIO.Write withoutSharding() {
//            return new com.impact.utils.io.TextIO.Write(inner.withoutSharding());
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withDelimiter(char[])}. */
//        public com.impact.utils.io.TextIO.Write withDelimiter(char[] delimiter) {
//            return new com.impact.utils.io.TextIO.Write(inner.withDelimiter(delimiter));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withHeader(String)}. */
//        public com.impact.utils.io.TextIO.Write withHeader(@Nullable String header) {
//            return new com.impact.utils.io.TextIO.Write(inner.withHeader(header));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withFooter(String)}. */
//        public com.impact.utils.io.TextIO.Write withFooter(@Nullable String footer) {
//            return new com.impact.utils.io.TextIO.Write(inner.withFooter(footer));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withWritableByteChannelFactory(WritableByteChannelFactory)}. */
//        public com.impact.utils.io.TextIO.Write withWritableByteChannelFactory(
//                WritableByteChannelFactory writableByteChannelFactory) {
//            return new com.impact.utils.io.TextIO.Write(inner.withWritableByteChannelFactory(writableByteChannelFactory));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withCompression(Compression)}. */
//        public com.impact.utils.io.TextIO.Write withCompression(Compression compression) {
//            return new com.impact.utils.io.TextIO.Write(inner.withCompression(compression));
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withWindowedWrites}. */
//        public com.impact.utils.io.TextIO.Write withWindowedWrites() {
//            return new com.impact.utils.io.TextIO.Write(inner.withWindowedWrites());
//        }
//
//        /** See {@link org.apache.beam.sdk.io.TextIO.TypedWrite#withNoSpilling}. */
//        public com.impact.utils.io.TextIO.Write withNoSpilling() {
//            return new com.impact.utils.io.TextIO.Write(inner.withNoSpilling());
//        }
//
//        /**
//         * Specify that output filenames are wanted.
//         *
//         * <p>The nested {@link org.apache.beam.sdk.io.TextIO.TypedWrite}transform always has access to output filenames, however due
//         * to backwards-compatibility concerns, {@link org.apache.beam.sdk.io.TextIO.Write} cannot return them. This method simply
//         * returns the inner {@link org.apache.beam.sdk.io.TextIO.TypedWrite} transform which has {@link WriteFilesResult} as its
//         * output type, allowing access to output files.
//         *
//         * <p>The supplied {@code DestinationT} type must be: the same as that supplied in {@link
//         * #to(DynamicDestinations)} if that method was used; {@link Params} if {@link
//         * #to(SerializableFunction, Params)} was used, or {@code Void} otherwise.
//         */
//        public <DestinationT> com.impact.utils.io.TextIO.TypedWrite<String, DestinationT> withOutputFilenames() {
//            return (com.impact.utils.io.TextIO.TypedWrite) inner;
//        }
//
//        @Override
//        public void populateDisplayData(DisplayData.Builder builder) {
//            inner.populateDisplayData(builder);
//        }
//
//        @Override
//        public PDone expand(PCollection<String> input) {
//            inner.expand(input);
//            return PDone.in(input.getPipeline());
//        }
//    }

    // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< TODO >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    /** @deprecated Use {@link Compression}. */
    @Deprecated
    public enum CompressionType {
        /** @see Compression#AUTO */
        AUTO(Compression.AUTO),

        /** @see Compression#UNCOMPRESSED */
        UNCOMPRESSED(Compression.UNCOMPRESSED),

        /** @see Compression#GZIP */
        GZIP(Compression.GZIP),

        /** @see Compression#BZIP2 */
        BZIP2(Compression.BZIP2),

        /** @see Compression#ZIP */
        ZIP(Compression.ZIP),

        /** @see Compression#ZSTD */
        ZSTD(Compression.ZSTD),

        /** @see Compression#DEFLATE */
        DEFLATE(Compression.DEFLATE);

        private final Compression canonical;

        CompressionType(Compression canonical) {
            this.canonical = canonical;
        }

        /** @see Compression#matches */
        public boolean matches(String filename) {
            return canonical.matches(filename);
        }
    }

    //////////////////////////////////////////////////////////////////////////////

    /**
     * Creates a {@link org.apache.beam.sdk.io.TextIO.Sink} that writes newline-delimited strings in UTF-8, for use with {@link
     * FileIO#write}.
     */
    public static com.impact.utils.io.TextIO.Sink sink() {
        return new AutoValue_TextIO_Sink.Builder().build();
    }

    /** Implementation of {@link #sink}. */
    @AutoValue
    public abstract static class Sink implements FileIO.Sink<String> {

        abstract @Nullable String getHeader();

        abstract @Nullable String getFooter();

        abstract com.impact.utils.io.TextIO.Sink.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.TextIO.Sink.Builder setHeader(String header);

            abstract com.impact.utils.io.TextIO.Sink.Builder setFooter(String footer);

            abstract com.impact.utils.io.TextIO.Sink build();
        }

        public com.impact.utils.io.TextIO.Sink withHeader(String header) {
            checkArgument(header != null, "header can not be null");
            return toBuilder().setHeader(header).build();
        }

        public com.impact.utils.io.TextIO.Sink withFooter(String footer) {
            checkArgument(footer != null, "footer can not be null");
            return toBuilder().setFooter(footer).build();
        }

        private transient @Nullable PrintWriter writer;

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            writer =
                    new PrintWriter(
                            new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8)));
            if (getHeader() != null) {
                writer.println(getHeader());
            }
        }

        @Override
        public void write(String element) throws IOException {
            writer.println(element);
        }

        @Override
        public void flush() throws IOException {
            if (getFooter() != null) {
                writer.println(getFooter());
            }
            // BEAM-7813: don't close writer here
            writer.flush();
        }
    }

    /** Disable construction of utility class. */
    private TextIO() {}
}

