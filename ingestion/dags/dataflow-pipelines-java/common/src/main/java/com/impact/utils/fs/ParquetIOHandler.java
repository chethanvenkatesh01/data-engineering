package com.impact.utils.fs;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

@Slf4j
public class ParquetIOHandler {
    public static void readParquet(String filePath) throws IOException {
        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create()); // This line should be set if GCS file system has to be accessed
        MatchResult match = FileSystems.match(filePath);
        MatchResult.Metadata metadata = match.metadata().get(0);
        Compression compression = Compression.AUTO;
        compression =
                (compression == Compression.AUTO)
                        ? Compression.detect(metadata.resourceId().getFilename())
                        : compression;
        CustomReadableFile readableFile = new CustomReadableFile(
                MatchResult.Metadata.builder()
                        .setResourceId(metadata.resourceId())
                        .setSizeBytes(metadata.sizeBytes())
                        .setLastModifiedMillis(metadata.lastModifiedMillis())
                        .setIsReadSeekEfficient(
                                metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
                        .build(),
                compression
        );
        log.info(String.valueOf(readableFile.getMetadata().sizeBytes()));
        log.info(readableFile.getCompression().toString());

         ParquetFileReader reader = ParquetFileReader.open(new BeamParquetInputFile(readableFile.openSeekable()));
         //reader.getFileMetaData().getSchema().
         FileMetaData parquetFileMetadata = reader.getFileMetaData(); //reader.getFooter().getFileMetaData();
         MessageType fileSchema = parquetFileMetadata.getSchema();
         log.info(String.format("Record Count %s", reader.getRecordCount()));
        List<Type> fields = fileSchema.getFields();
        log.info(fileSchema.getType("global_facility_key") + ";");
        for(Type t : fields) {

            log.info(t.getName() + "// " + t.asPrimitiveType() + "//" + t.getLogicalTypeAnnotation());
        }
    }

    public static List<Map<String, String>> readParquetSchema(String filePath) throws IOException {
        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create()); // This line should be set if GCS file system has to be accessed
        MatchResult match = FileSystems.match(filePath);
        MatchResult.Metadata metadata = match.metadata().get(0);
        Compression compression = Compression.AUTO;
        compression =
                (compression == Compression.AUTO)
                        ? Compression.detect(metadata.resourceId().getFilename())
                        : compression;
        CustomReadableFile readableFile = new CustomReadableFile(
                MatchResult.Metadata.builder()
                        .setResourceId(metadata.resourceId())
                        .setSizeBytes(metadata.sizeBytes())
                        .setLastModifiedMillis(metadata.lastModifiedMillis())
                        .setIsReadSeekEfficient(
                                metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
                        .build(),
                compression
        );
        //log.info(String.valueOf(readableFile.getMetadata().sizeBytes()));
        //log.info(readableFile.getCompression().toString());
        ParquetFileReader reader = ParquetFileReader.open(new BeamParquetInputFile(readableFile.openSeekable()));
        //reader.getFileMetaData().getSchema().
        FileMetaData parquetFileMetadata = reader.getFileMetaData(); //reader.getFooter().getFileMetaData();
        MessageType fileSchema = parquetFileMetadata.getSchema();
        //log.info(String.format("Record Count %s", reader.getRecordCount()));
        List<Type> fields = fileSchema.getFields();
        //log.info(fileSchema.getType("global_facility_key") + ";");
        List<Map<String, String>> schema = new ArrayList<Map<String, String>>();
        for(Type type : fields) {
            Map<String, String> fieldMetadata = new LinkedHashMap<String, String>();
            fieldMetadata.put("name", type.getName());
            fieldMetadata.put("physicalType", type.asPrimitiveType().toString().split(" ")[1].toUpperCase());
            fieldMetadata.put("logicalType", String.valueOf(type.getLogicalTypeAnnotation()).toUpperCase());
            //log.info(t.getName() + "// " + t.asPrimitiveType() + "//" + t.getLogicalTypeAnnotation());
            schema.add(fieldMetadata);
        }
        return schema;
    }

    public static MessageType getParquetSchema(String filePath) throws IOException {
        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create()); // This line should be set if GCS file system has to be accessed
        MatchResult match = FileSystems.match(filePath);
        MatchResult.Metadata metadata = match.metadata().get(0);
        Compression compression = Compression.AUTO;
        compression =
                (compression == Compression.AUTO)
                        ? Compression.detect(metadata.resourceId().getFilename())
                        : compression;
        CustomReadableFile readableFile = new CustomReadableFile(
                MatchResult.Metadata.builder()
                        .setResourceId(metadata.resourceId())
                        .setSizeBytes(metadata.sizeBytes())
                        .setLastModifiedMillis(metadata.lastModifiedMillis())
                        .setIsReadSeekEfficient(
                                metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
                        .build(),
                compression
        );
        //log.info(String.valueOf(readableFile.getMetadata().sizeBytes()));
        //log.info(readableFile.getCompression().toString());
        ParquetFileReader reader = ParquetFileReader.open(new BeamParquetInputFile(readableFile.openSeekable()));
        //reader.getFileMetaData().getSchema().
        FileMetaData parquetFileMetadata = reader.getFileMetaData(); //reader.getFooter().getFileMetaData();
        MessageType fileSchema = parquetFileMetadata.getSchema();
        return  fileSchema;
    }


    private static class BeamParquetInputFile implements InputFile {
        private final SeekableByteChannel seekableByteChannel;

        BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
            this.seekableByteChannel = seekableByteChannel;
        }

        @Override
        public long getLength() throws IOException {
            return seekableByteChannel.size();
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

                @Override
                public long getPos() throws IOException {
                    return seekableByteChannel.position();
                }

                @Override
                public void seek(long newPos) throws IOException {
                    seekableByteChannel.position(newPos);
                }
            };
        }
    }
}
