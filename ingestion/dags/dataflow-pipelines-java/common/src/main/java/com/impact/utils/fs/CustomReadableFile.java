package com.impact.utils.fs;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

public class CustomReadableFile {
    private final MatchResult.Metadata metadata;
    private final Compression compression;


    CustomReadableFile(MatchResult.Metadata metadata, Compression compression) {
        this.metadata = metadata;
        this.compression = compression;
    }

    /** Returns the {@link MatchResult.Metadata} of the file. */
    public MatchResult.Metadata getMetadata() {
        return metadata;
    }

    /** Returns the method with which this file will be decompressed in {@link #open}. */
    public Compression getCompression() {
        return compression;
    }

    /**
     * Returns a {@link ReadableByteChannel} reading the data from this file, potentially
     * decompressing it using {@link #getCompression}.
     */
    public ReadableByteChannel open() throws IOException {
        return compression.readDecompressed(FileSystems.open(metadata.resourceId()));
    }

    /**
     * Returns a {@link SeekableByteChannel} equivalent to {@link #open}, but fails if this file is
     * not {@link MatchResult.Metadata#isReadSeekEfficient seekable}.
     */
    public SeekableByteChannel openSeekable() throws IOException {
        checkState(
                getMetadata().isReadSeekEfficient(),
                "The file %s is not seekable",
                metadata.resourceId());
        return (SeekableByteChannel) open();
    }

    /** Returns the full contents of the file as bytes. */
    public byte[] readFullyAsBytes() throws IOException {
        try (InputStream stream = Channels.newInputStream(open())) {
            return StreamUtils.getBytesWithoutClosing(stream);
        }
    }

    /** Returns the full contents of the file as a {@link String} decoded as UTF-8. */
    public String readFullyAsUTF8String() throws IOException {
        return new String(readFullyAsBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "ReadableFile{metadata=" + metadata + ", compression=" + compression + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomReadableFile that = (CustomReadableFile) o;
        return Objects.equal(metadata, that.metadata) && compression == that.compression;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(metadata, compression);
    }

}
