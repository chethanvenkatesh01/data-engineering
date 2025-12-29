package com.impact.utils.io;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation detail of {@link TextIO.Write}.
 *
 * <p>A {@link FileBasedSink} for text files. Produces text files with the newline separator {@code
 * '\n'} represented in {@code UTF-8} format as the record separator. Each record (including the
 * last) is terminated.
 */
@SuppressWarnings({
        "nullness", // TODO(https://github.com/apache/beam/issues/20497)
        "rawtypes"
})
class TextSink<UserT, DestinationT> extends FileBasedSink<UserT, DestinationT, String> {
    private final @Nullable String header;
    private final @Nullable String footer;
    private final char[] delimiter;

    TextSink(
            ValueProvider<ResourceId> baseOutputFilename,
            DynamicDestinations<UserT, DestinationT, String> dynamicDestinations,
            char[] delimiter,
            @Nullable String header,
            @Nullable String footer,
            WritableByteChannelFactory writableByteChannelFactory) {
        super(baseOutputFilename, dynamicDestinations, writableByteChannelFactory);
        this.header = header;
        this.footer = footer;
        this.delimiter = delimiter;
    }

    @Override
    public WriteOperation<DestinationT, String> createWriteOperation() {
        return new com.impact.utils.io.TextSink.TextWriteOperation<>(this, delimiter, header, footer);
    }

    /** A {@link WriteOperation WriteOperation} for text files. */
    private static class TextWriteOperation<DestinationT>
            extends WriteOperation<DestinationT, String> {
        private final @Nullable String header;
        private final @Nullable String footer;
        private final char[] delimiter;

        private TextWriteOperation(
                com.impact.utils.io.TextSink sink, char[] delimiter, @Nullable String header, @Nullable String footer) {
            super(sink);
            this.header = header;
            this.footer = footer;
            this.delimiter = delimiter;
        }

        @Override
        public Writer<DestinationT, String> createWriter() throws Exception {
            return new com.impact.utils.io.TextSink.TextWriter<>(this, delimiter, header, footer);
        }
    }

    /** A {@link Writer Writer} for text files. */
    private static class TextWriter<DestinationT> extends Writer<DestinationT, String> {
        private final @Nullable String header;
        private final @Nullable String footer;
        private final char[] delimiter;

        // Initialized in prepareWrite
        private @Nullable OutputStreamWriter out;

        public TextWriter(
                WriteOperation<DestinationT, String> writeOperation,
                char[] delimiter,
                @Nullable String header,
                @Nullable String footer) {
            super(writeOperation, MimeTypes.TEXT);
            this.delimiter = delimiter;
            this.header = header;
            this.footer = footer;
        }

        /** Writes {@code value} followed by a newline character if {@code value} is not null. */
        private void writeIfNotNull(@Nullable String value) throws IOException {
            if (value != null) {
                writeLine(value);
            }
        }

        /** Writes {@code value} followed by the delimiter byte sequence. */
        private void writeLine(String value) throws IOException {
            out.write(value);
            out.write(delimiter);
        }

        @Override
        protected void prepareWrite(WritableByteChannel channel) throws Exception {
            out = new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8);
        }

        @Override
        protected void writeHeader() throws Exception {
            writeIfNotNull(header);
        }

        @Override
        public void write(String value) throws Exception {
            writeLine(value);
        }

        @Override
        protected void writeFooter() throws Exception {
            writeIfNotNull(footer);
        }

        @Override
        protected void finishWrite() throws Exception {
            out.flush();
        }
    }
}
