package com.impact.utils.io;

import static org.apache.parquet.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;

import com.impact.utils.date.DateHelper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
//import org.apache.beam.sdk.io.parquet.ParquetIO.ReadFiles.SplitReadFn;
import com.impact.utils.io.ParquetIO.ReadFiles.SplitReadFn;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Experimental(Kind.SOURCE_SINK)
@SuppressWarnings({
        "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
        "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ParquetIO {
    private static final Logger LOG = LoggerFactory.getLogger(com.impact.utils.io.ParquetIO.class);

    /**
     * Reads {@link GenericRecord} from a Parquet file (or multiple Parquet files matching the
     * pattern).
     */
    public static com.impact.utils.io.ParquetIO.Read read(Schema schema) {
        return new AutoValue_ParquetIO_Read.Builder()
                .setSchema(schema)
                .setInferBeamSchema(false)
                .build();
    }

    /**
     * Like {@link #read(Schema)}, but reads each file in a {@link PCollection} of {@link
     * ReadableFile}, which allows more flexible usage.
     */
    public static com.impact.utils.io.ParquetIO.ReadFiles readFiles(Schema schema) {
        return new AutoValue_ParquetIO_ReadFiles.Builder()
                .setSchema(schema)
                .setInferBeamSchema(false)
                .build();
    }

    /**
     * Reads {@link GenericRecord} from a Parquet file (or multiple Parquet files matching the
     * pattern) and converts to user defined type using provided parseFn.
     */
    public static <T> com.impact.utils.io.ParquetIO.Parse<T> parseGenericRecords(SerializableFunction<GenericRecord, T> parseFn) {
        return new AutoValue_ParquetIO_Parse.Builder<T>().setParseFn(parseFn).build();
    }

    /**
     * Reads {@link GenericRecord} from Parquet files and converts to user defined type using provided
     * {@code parseFn}.
     */
    public static <T> com.impact.utils.io.ParquetIO.ParseFiles<T> parseFilesGenericRecords(
            SerializableFunction<GenericRecord, T> parseFn) {
        return new AutoValue_ParquetIO_ParseFiles.Builder<T>().setParseFn(parseFn).build();
    }

    /** Implementation of {@link #read(Schema)}. */
    @AutoValue
    public abstract static class Read extends PTransform<PBegin, PCollection<GenericRecord>> {

        abstract @Nullable ValueProvider<String> getFilepattern();

        abstract @Nullable Schema getSchema();

        abstract @Nullable Schema getProjectionSchema();

        abstract @Nullable Schema getEncoderSchema();

        abstract @Nullable GenericData getAvroDataModel();

        abstract @Nullable SerializableConfiguration getConfiguration();

        abstract boolean getInferBeamSchema();

        abstract com.impact.utils.io.ParquetIO.Read.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {

            abstract com.impact.utils.io.ParquetIO.Read.Builder setInferBeamSchema(boolean inferBeamSchema);

            abstract com.impact.utils.io.ParquetIO.Read.Builder setFilepattern(ValueProvider<String> filepattern);

            abstract com.impact.utils.io.ParquetIO.Read.Builder setSchema(Schema schema);

            abstract com.impact.utils.io.ParquetIO.Read.Builder setEncoderSchema(Schema schema);

            abstract com.impact.utils.io.ParquetIO.Read.Builder setProjectionSchema(Schema schema);

            abstract com.impact.utils.io.ParquetIO.Read.Builder setAvroDataModel(GenericData model);

            abstract com.impact.utils.io.ParquetIO.Read.Builder setConfiguration(SerializableConfiguration configuration);

            abstract com.impact.utils.io.ParquetIO.Read build();
        }

        /** Reads from the given filename or filepattern. */
        public com.impact.utils.io.ParquetIO.Read from(ValueProvider<String> filepattern) {
            return toBuilder().setFilepattern(filepattern).build();
        }

        /** Like {@link #from(ValueProvider)}. */
        public com.impact.utils.io.ParquetIO.Read from(String filepattern) {
            return from(ValueProvider.StaticValueProvider.of(filepattern));
        }
        /** Enable the reading with projection. */
        public com.impact.utils.io.ParquetIO.Read withProjection(Schema projectionSchema, Schema encoderSchema) {
            return toBuilder()
                    .setProjectionSchema(projectionSchema)
                    .setEncoderSchema(encoderSchema)
                    .build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.Read withConfiguration(Map<String, String> configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.Read withConfiguration(Configuration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
        }

        @Experimental(Kind.SCHEMAS)
        public com.impact.utils.io.ParquetIO.Read withBeamSchemas(boolean inferBeamSchema) {
            return toBuilder().setInferBeamSchema(inferBeamSchema).build();
        }

        /**
         * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
         */
        public com.impact.utils.io.ParquetIO.Read withAvroDataModel(GenericData model) {
            return toBuilder().setAvroDataModel(model).build();
        }

        @Override
        public PCollection<GenericRecord> expand(PBegin input) {
            checkNotNull(getFilepattern(), "Filepattern cannot be null.");
            PCollection<ReadableFile> inputFiles =
                    input
                            .apply(
                                    "Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                            .apply(FileIO.matchAll())
                            .apply(FileIO.readMatches());

            com.impact.utils.io.ParquetIO.ReadFiles readFiles =
                    readFiles(getSchema())
                            .withBeamSchemas(getInferBeamSchema())
                            .withAvroDataModel(getAvroDataModel())
                            .withProjection(getProjectionSchema(), getEncoderSchema());
            if (getConfiguration() != null) {
                readFiles = readFiles.withConfiguration(getConfiguration().get());
            }

            return inputFiles.apply(readFiles);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .addIfNotNull(
                            DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
                    .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
                    .add(
                            DisplayData.item("inferBeamSchema", getInferBeamSchema())
                                    .withLabel("Infer Beam Schema"))
                    .addIfNotNull(DisplayData.item("projectionSchema", String.valueOf(getProjectionSchema())))
                    .addIfNotNull(DisplayData.item("avroDataModel", String.valueOf(getAvroDataModel())));
            if (this.getConfiguration() != null) {
                Configuration configuration = this.getConfiguration().get();
                for (Entry<String, String> entry : configuration) {
                    if (entry.getKey().startsWith("parquet")) {
                        builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }
    }

    /** Implementation of {@link #parseGenericRecords(SerializableFunction)}. */
    @AutoValue
    public abstract static class Parse<T> extends PTransform<PBegin, PCollection<T>> {
        abstract @Nullable ValueProvider<String> getFilepattern();

        abstract SerializableFunction<GenericRecord, T> getParseFn();

        abstract @Nullable Coder<T> getCoder();

        abstract @Nullable SerializableConfiguration getConfiguration();

        abstract com.impact.utils.io.ParquetIO.Parse.Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract com.impact.utils.io.ParquetIO.Parse.Builder<T> setFilepattern(ValueProvider<String> inputFiles);

            abstract com.impact.utils.io.ParquetIO.Parse.Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

            abstract com.impact.utils.io.ParquetIO.Parse.Builder<T> setCoder(Coder<T> coder);

            abstract com.impact.utils.io.ParquetIO.Parse.Builder<T> setConfiguration(SerializableConfiguration configuration);

            abstract com.impact.utils.io.ParquetIO.Parse<T> build();
        }

        public com.impact.utils.io.ParquetIO.Parse<T> from(ValueProvider<String> filepattern) {
            return toBuilder().setFilepattern(filepattern).build();
        }

        public com.impact.utils.io.ParquetIO.Parse<T> from(String filepattern) {
            return from(ValueProvider.StaticValueProvider.of(filepattern));
        }

        /** Specify the output coder to use for output of the {@code ParseFn}. */
        public com.impact.utils.io.ParquetIO.Parse<T> withCoder(Coder<T> coder) {
            return (coder == null) ? this : toBuilder().setCoder(coder).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.Parse<T> withConfiguration(Map<String, String> configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.Parse<T> withConfiguration(Configuration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
        }

        @Override
        public PCollection<T> expand(PBegin input) {
            checkNotNull(getFilepattern(), "Filepattern cannot be null.");
            return input
                    .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                    .apply(FileIO.matchAll())
                    .apply(FileIO.readMatches())
                    .apply(
                            parseFilesGenericRecords(getParseFn())
                                    .toBuilder()
                                    .setCoder(getCoder())
                                    .setConfiguration(getConfiguration())
                                    .build());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .addIfNotNull(
                            DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
                    .add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
            if (this.getCoder() != null) {
                builder.add(DisplayData.item("coder", getCoder().getClass()));
            }
            if (this.getConfiguration() != null) {
                Configuration configuration = this.getConfiguration().get();
                for (Entry<String, String> entry : configuration) {
                    if (entry.getKey().startsWith("parquet")) {
                        builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }
    }

    /** Implementation of {@link #parseFilesGenericRecords(SerializableFunction)}. */
    @AutoValue
    public abstract static class ParseFiles<T>
            extends PTransform<PCollection<ReadableFile>, PCollection<T>> {

        abstract SerializableFunction<GenericRecord, T> getParseFn();

        abstract @Nullable Coder<T> getCoder();

        abstract @Nullable SerializableConfiguration getConfiguration();

        abstract com.impact.utils.io.ParquetIO.ParseFiles.Builder<T> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<T> {
            abstract com.impact.utils.io.ParquetIO.ParseFiles.Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn);

            abstract com.impact.utils.io.ParquetIO.ParseFiles.Builder<T> setCoder(Coder<T> coder);

            abstract com.impact.utils.io.ParquetIO.ParseFiles.Builder<T> setConfiguration(SerializableConfiguration configuration);

            abstract com.impact.utils.io.ParquetIO.ParseFiles<T> build();
        }

        /** Specify the output coder to use for output of the {@code ParseFn}. */
        public com.impact.utils.io.ParquetIO.ParseFiles<T> withCoder(Coder<T> coder) {
            return (coder == null) ? this : toBuilder().setCoder(coder).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.ParseFiles<T> withConfiguration(Map<String, String> configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.ParseFiles<T> withConfiguration(Configuration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
        }

        @Override
        public PCollection<T> expand(PCollection<ReadableFile> input) {
            checkArgument(!isGenericRecordOutput(), "Parse can't be used for reading as GenericRecord.");

            return input
                    .apply(ParDo.of(new SplitReadFn<>(null, null, getParseFn(), getConfiguration())))
                    .setCoder(inferCoder(input.getPipeline().getCoderRegistry()));
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("parseFn", getParseFn().getClass()).withLabel("Parse function"));
            if (this.getCoder() != null) {
                builder.add(DisplayData.item("coder", getCoder().getClass()));
            }
            if (this.getConfiguration() != null) {
                Configuration configuration = this.getConfiguration().get();
                for (Entry<String, String> entry : configuration) {
                    if (entry.getKey().startsWith("parquet")) {
                        builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        /** Returns true if expected output is {@code PCollection<GenericRecord>}. */
        private boolean isGenericRecordOutput() {
            String outputType = TypeDescriptors.outputOf(getParseFn()).getType().getTypeName();
            return outputType.equals(GenericRecord.class.getTypeName());
        }

        /**
         * Identifies the {@code Coder} to be used for the output PCollection.
         *
         * <p>throws an exception if expected output is of type {@link GenericRecord}.
         *
         * @param coderRegistry the {@link org.apache.beam.sdk.Pipeline}'s CoderRegistry to identify
         *     Coder for expected output type of {@link #getParseFn()}
         */
        private Coder<T> inferCoder(CoderRegistry coderRegistry) {
            if (isGenericRecordOutput()) {
                throw new IllegalArgumentException("Parse can't be used for reading as GenericRecord.");
            }

            // Use explicitly provided coder
            if (getCoder() != null) {
                return getCoder();
            }

            // If not GenericRecord infer it from ParseFn.
            try {
                return coderRegistry.getCoder(TypeDescriptors.outputOf(getParseFn()));
            } catch (CannotProvideCoderException e) {
                throw new IllegalArgumentException(
                        "Unable to infer coder for output of parseFn. Specify it explicitly using .withCoder().",
                        e);
            }
        }
    }

    /** Implementation of {@link #readFiles(Schema)}. */
    @AutoValue
    public abstract static class ReadFiles
            extends PTransform<PCollection<ReadableFile>, PCollection<GenericRecord>> {

        abstract @Nullable Schema getSchema();

        abstract @Nullable GenericData getAvroDataModel();

        abstract @Nullable Schema getEncoderSchema();

        abstract @Nullable Schema getProjectionSchema();

        abstract @Nullable SerializableConfiguration getConfiguration();

        abstract boolean getInferBeamSchema();

        abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder toBuilder();

        // Added additional attributes to check if audit column (such as file date) should be added to Generic Record
        abstract @Nullable  String getAuditColumnName();

        abstract @Nullable Schema getUpdatedSchema();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setSchema(Schema schema);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setAvroDataModel(GenericData model);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setEncoderSchema(Schema schema);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setProjectionSchema(Schema schema);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setConfiguration(SerializableConfiguration configuration);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setInferBeamSchema(boolean inferBeamSchema);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setAuditColumnName(String columnName);

            abstract com.impact.utils.io.ParquetIO.ReadFiles.Builder setUpdatedSchema(Schema updatedSchema);

            abstract com.impact.utils.io.ParquetIO.ReadFiles build();
        }

        /**
         * Define the Avro data model; see {@link AvroParquetReader.Builder#withDataModel(GenericData)}.
         */
        public com.impact.utils.io.ParquetIO.ReadFiles withAvroDataModel(GenericData model) {
            return toBuilder().setAvroDataModel(model).build();
        }

        public com.impact.utils.io.ParquetIO.ReadFiles withProjection(Schema projectionSchema, Schema encoderSchema) {
            return toBuilder()
                    .setProjectionSchema(projectionSchema)
                    .setEncoderSchema(encoderSchema)
                    .build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.ReadFiles withConfiguration(Map<String, String> configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.ReadFiles withConfiguration(Configuration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
        }

        @Experimental(Kind.SCHEMAS)
        public com.impact.utils.io.ParquetIO.ReadFiles withBeamSchemas(boolean inferBeamSchema) {
            return toBuilder().setInferBeamSchema(inferBeamSchema).build();
        }

        public com.impact.utils.io.ParquetIO.ReadFiles withAuditColumnName(String columnName) {
            return toBuilder().setAuditColumnName(columnName).build();
        }

        public com.impact.utils.io.ParquetIO.ReadFiles withUpdatedSchema(Schema updatedSchema) {
            return toBuilder().setUpdatedSchema(updatedSchema).build();
        }

        @Override
        public PCollection<GenericRecord> expand(PCollection<ReadableFile> input) {
            checkNotNull(getSchema(), "Schema can not be null");
            return input
                    .apply(
                            ParDo.of(
                                    new SplitReadFn<>(
                                            getAvroDataModel(),
                                            getProjectionSchema(),
                                            GenericRecordPassthroughFn.create(),
                                            getConfiguration(), getAuditColumnName())))
                    //.setCoder(getCollectionCoder())
                    .setCoder(getCollectionCoderv2());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .addIfNotNull(DisplayData.item("schema", String.valueOf(getSchema())))
                    .add(
                            DisplayData.item("inferBeamSchema", getInferBeamSchema())
                                    .withLabel("Infer Beam Schema"))
                    .addIfNotNull(DisplayData.item("projectionSchema", String.valueOf(getProjectionSchema())))
                    .addIfNotNull(DisplayData.item("avroDataModel", String.valueOf(getAvroDataModel())));
            if (this.getConfiguration() != null) {
                Configuration configuration = this.getConfiguration().get();
                for (Entry<String, String> entry : configuration) {
                    if (entry.getKey().startsWith("parquet")) {
                        builder.addIfNotNull(DisplayData.item(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        /**
         * Returns {@link org.apache.beam.sdk.schemas.SchemaCoder} when using Beam schemas, {@link
         * AvroCoder} when not using Beam schema.
         */
        @Experimental(Kind.SCHEMAS)
        private Coder<GenericRecord> getCollectionCoder() {
            Schema coderSchema = getProjectionSchema() != null ? getEncoderSchema() : getSchema();

            return getInferBeamSchema() ? AvroUtils.schemaCoder(coderSchema) : AvroCoder.of(coderSchema);
        }

        private Coder<GenericRecord> getCollectionCoderv2() {
            Schema coderSchema = null;
            if(getAuditColumnName()!=null) {
                coderSchema = getProjectionSchema() != null ? getEncoderSchema() : getUpdatedSchema();

            }
            else {
                coderSchema = getProjectionSchema() != null ? getEncoderSchema() : getSchema();
            }
            //Schema coderSchema = getProjectionSchema() != null ? getEncoderSchema() : getSchema();
            return getInferBeamSchema() ? AvroUtils.schemaCoder(coderSchema) : AvroCoder.of(coderSchema);
        }

        @DoFn.BoundedPerElement
         static class SplitReadFn<T> extends DoFn<ReadableFile, T> {
            private final Class<? extends GenericData> modelClass;
            private final String requestSchemaString;
            // Default initial splitting the file into blocks of 64MB. Unit of SPLIT_LIMIT is byte.
            private static final long SPLIT_LIMIT = 64000000;

            private @Nullable final SerializableConfiguration configuration;

            private final SerializableFunction<GenericRecord, T> parseFn;

            private String auditColumnName;

            SplitReadFn(
                    GenericData model,
                    Schema requestSchema,
                    SerializableFunction<GenericRecord, T> parseFn,
                    @Nullable SerializableConfiguration configuration, String auditColumnName) {

                this.modelClass = model != null ? model.getClass() : null;
                this.requestSchemaString = requestSchema != null ? requestSchema.toString() : null;
                this.parseFn = checkNotNull(parseFn, "GenericRecord parse function can't be null");
                this.configuration = configuration;
                this.auditColumnName = auditColumnName;
            }

            SplitReadFn(
                    GenericData model,
                    Schema requestSchema,
                    SerializableFunction<GenericRecord, T> parseFn,
                    @Nullable SerializableConfiguration configuration) {

                this.modelClass = model != null ? model.getClass() : null;
                this.requestSchemaString = requestSchema != null ? requestSchema.toString() : null;
                this.parseFn = checkNotNull(parseFn, "GenericRecord parse function can't be null");
                this.configuration = configuration;
                this.auditColumnName = null;
            }

            private ParquetFileReader getParquetFileReader(ReadableFile file) throws Exception {
                ParquetReadOptions options = HadoopReadOptions.builder(getConfWithModelClass()).build();
                return ParquetFileReader.open(new com.impact.utils.io.ParquetIO.ReadFiles.BeamParquetInputFile(file.openSeekable()), options);
            }

            @ProcessElement
            public void processElement(
                    @Element ReadableFile file,
                    RestrictionTracker<OffsetRange, Long> tracker,
                    OutputReceiver<T> outputReceiver)
                    throws Exception {
                LOG.debug(
                        "start {} to {}",
                        tracker.currentRestriction().getFrom(),
                        tracker.currentRestriction().getTo());
                Configuration conf = getConfWithModelClass();
                GenericData model = null;
                if (modelClass != null) {
                    model = (GenericData) modelClass.getMethod("get").invoke(null);
                }
                AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>(model);
                if (requestSchemaString != null) {
                    AvroReadSupport.setRequestedProjection(
                            conf, new Schema.Parser().parse(requestSchemaString));
                }
                ParquetReadOptions options = HadoopReadOptions.builder(conf).build();
                try (ParquetFileReader reader =
                             ParquetFileReader.open(new com.impact.utils.io.ParquetIO.ReadFiles.BeamParquetInputFile(file.openSeekable()), options)) {
                    Filter filter = checkNotNull(options.getRecordFilter(), "filter");
                    Configuration hadoopConf = ((HadoopReadOptions) options).getConf();
                    FileMetaData parquetFileMetadata = reader.getFooter().getFileMetaData();
                    if(reader.getRecordCount()==0) {
                        return;
                    }
                    String fileDate = null;
                    if(auditColumnName!=null){
                        // Audit Column is provided. Adding file date as additional field to Generic Record
                        String filePath = file.getMetadata().resourceId().toString();
                        fileDate = DateHelper.getFileDateFromPath(filePath);
                        //LOG.info(String.format("File Date: %s", fileDate));
                    }
                    MessageType fileSchema = parquetFileMetadata.getSchema();
                    Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
                    ReadSupport.ReadContext readContext =
                            readSupport.init(
                                    new InitContext(
                                            hadoopConf,
                                            Maps.transformValues(fileMetadata, ImmutableSet::of),
                                            fileSchema));
                    ColumnIOFactory columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());

                    RecordMaterializer<GenericRecord> recordConverter =
                            readSupport.prepareForRead(hadoopConf, fileMetadata, fileSchema, readContext);
                    reader.setRequestedSchema(readContext.getRequestedSchema());
                    MessageColumnIO columnIO =
                            columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema, true);
                    long currentBlock = tracker.currentRestriction().getFrom();
                    for (int i = 0; i < currentBlock; i++) {
                        reader.skipNextRowGroup();
                    }
                    while (tracker.tryClaim(currentBlock)) {
                        PageReadStore pages = reader.readNextRowGroup();
                        LOG.debug("block {} read in memory. row count = {}", currentBlock, pages.getRowCount());
                        currentBlock += 1;
                        RecordReader<GenericRecord> recordReader =
                                columnIO.getRecordReader(
                                        pages, recordConverter, options.useRecordFilter() ? filter : FilterCompat.NOOP);
                        long currentRow = 0;
                        long totalRows = pages.getRowCount();
                        while (currentRow < totalRows) {
                            try {
                                GenericRecord record;
                                currentRow += 1;
                                try {
                                    record = recordReader.read();
                                    //Adds an audit column (file date) to generic record
                                    if(auditColumnName!=null) {
                                        Schema originalSchema = record.getSchema();
                                        List<Schema.Field> fieldsLockedArray = originalSchema.getFields();
                                        List<Schema.Field> fields = new ArrayList<Schema.Field>();
                                        for(Schema.Field field : fieldsLockedArray) {
                                            fields.add(new Schema.Field(field.name(), field.schema(), null, null));
                                        }
                                        fields.add(new Schema.Field(auditColumnName, Schema.create(Schema.Type.STRING), null, fileDate));
                                        Schema newSchema = Schema.createRecord(
                                                null,
                                                "docstring",
                                                "namespace",
                                                false // Not a union
                                        );
                                        newSchema.setFields(fields);
                                        GenericRecord newRecord = new GenericData.Record(newSchema);
//                                        for(Schema.Field field : fields) {
//                                            newRecord.put(field.name(), record.get(field.name()));
//                                        }
                                        for(Schema.Field field : fieldsLockedArray) {
                                            newRecord.put(field.name(), record.get(field.name()));
                                        }
                                        newRecord.put(auditColumnName, fileDate);
                                        record = newRecord;
                                    }
                                } catch (RecordMaterializer.RecordMaterializationException e) {
                                    LOG.warn(
                                            "skipping a corrupt record at {} in block {} in file {}",
                                            currentRow,
                                            currentBlock,
                                            file.toString());
                                    continue;
                                }
                                if (record == null) {
                                    // it happens when a record is filtered out in this block
                                    LOG.debug(
                                            "record is filtered out by reader in block {} in file {}",
                                            currentBlock,
                                            file.toString());
                                    continue;
                                }
                                if (recordReader.shouldSkipCurrentRecord()) {
                                    // this record is being filtered via the filter2 package
                                    LOG.debug(
                                            "skipping record at {} in block {} in file {}",
                                            currentRow,
                                            currentBlock,
                                            file.toString());
                                    continue;
                                }
                                outputReceiver.output(parseFn.apply(record));
                            } catch (RuntimeException e) {

                                throw new ParquetDecodingException(
                                        format(
                                                "Can not read value at %d in block %d in file %s",
                                                currentRow, currentBlock, file.toString()),
                                        e);
                            }
                        }
                        LOG.debug(
                                "Finish processing {} rows from block {} in file {}",
                                currentRow,
                                currentBlock - 1,
                                file.toString());
                    }
                }
            }

            public Configuration getConfWithModelClass() throws ReflectiveOperationException {
                Configuration conf = SerializableConfiguration.newConfiguration(configuration);
                GenericData model = buildModelObject(modelClass);

                if (model != null
                        && (model.getClass() == GenericData.class || model.getClass() == SpecificData.class)) {
                    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
                } else {
                    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false);
                }
                return conf;
            }

            @GetInitialRestriction
            public OffsetRange getInitialRestriction(@Element ReadableFile file) throws Exception {
                try (ParquetFileReader reader = getParquetFileReader(file)) {
                    return new OffsetRange(0, reader.getRowGroups().size());
                }
            }

            @SplitRestriction
            public void split(
                    @Restriction OffsetRange restriction,
                    OutputReceiver<OffsetRange> out,
                    @Element ReadableFile file)
                    throws Exception {
                try (ParquetFileReader reader = getParquetFileReader(file)) {
                    List<BlockMetaData> rowGroups = reader.getRowGroups();
                    for (OffsetRange offsetRange :
                            splitBlockWithLimit(
                                    restriction.getFrom(), restriction.getTo(), rowGroups, SPLIT_LIMIT)) {
                        out.output(offsetRange);
                    }
                }
            }

            public ArrayList<OffsetRange> splitBlockWithLimit(
                    long start, long end, List<BlockMetaData> blockList, long limit) {
                ArrayList<OffsetRange> offsetList = new ArrayList<>();
                long totalSize = 0;
                long rangeStart = start;
                for (long rangeEnd = start; rangeEnd < end; rangeEnd++) {
                    totalSize += blockList.get((int) rangeEnd).getTotalByteSize();
                    if (totalSize >= limit) {
                        offsetList.add(new OffsetRange(rangeStart, rangeEnd + 1));
                        rangeStart = rangeEnd + 1;
                        totalSize = 0;
                    }
                }
                if (totalSize != 0) {
                    offsetList.add(new OffsetRange(rangeStart, end));
                }
                return offsetList;
            }

            @NewTracker
            public RestrictionTracker<OffsetRange, Long> newTracker(
                    @Restriction OffsetRange restriction, @Element ReadableFile file) throws Exception {
                com.impact.utils.io.ParquetIO.ReadFiles.SplitReadFn.CountAndSize recordCountAndSize = getRecordCountAndSize(file, restriction);
                return new com.impact.utils.io.ParquetIO.ReadFiles.BlockTracker(
                        restriction,
                        Math.round(recordCountAndSize.getSize()),
                        Math.round(recordCountAndSize.getCount()));
            }

            @GetRestrictionCoder
            public OffsetRange.Coder getRestrictionCoder() {
                return new OffsetRange.Coder();
            }

            @GetSize
            public double getSize(@Element ReadableFile file, @Restriction OffsetRange restriction)
                    throws Exception {
                return getRecordCountAndSize(file, restriction).getSize();
            }

            private com.impact.utils.io.ParquetIO.ReadFiles.SplitReadFn.CountAndSize getRecordCountAndSize(ReadableFile file, OffsetRange restriction)
                    throws Exception {
                try (ParquetFileReader reader = getParquetFileReader(file)) {
                    double size = 0;
                    double recordCount = 0;
                    for (long i = restriction.getFrom(); i < restriction.getTo(); i++) {
                        BlockMetaData block = reader.getRowGroups().get((int) i);
                        recordCount += block.getRowCount();
                        size += block.getTotalByteSize();
                    }
                    return com.impact.utils.io.ParquetIO.ReadFiles.SplitReadFn.CountAndSize.create(recordCount, size);
                }
            }

            @AutoValue
            abstract static class CountAndSize {
                static com.impact.utils.io.ParquetIO.ReadFiles.SplitReadFn.CountAndSize create(double count, double size) {
                    return new AutoValue_ParquetIO_ReadFiles_SplitReadFn_CountAndSize(count, size);
                }

                abstract double getCount();

                abstract double getSize();
            }
        }

        public static class BlockTracker extends OffsetRangeTracker {
            private long totalWork;
            private long progress;
            private long approximateRecordSize;

            public BlockTracker(OffsetRange range, long totalByteSize, long recordCount) {
                super(range);
                if (recordCount != 0) {
                    this.approximateRecordSize = totalByteSize / recordCount;
                    // Ensure that totalWork = approximateRecordSize * recordCount
                    this.totalWork = approximateRecordSize * recordCount;
                    this.progress = 0;
                }
            }

            public void makeProgress() throws IOException {
                progress += approximateRecordSize;
                if (progress > totalWork) {
                    throw new IOException("Making progress out of range");
                }
            }

            @Override
            // TODO(BEAM-10842): Refine the BlockTracker to provide better progress.
            public Progress getProgress() {
                return super.getProgress();
            }
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

    /** Creates a {@link org.apache.beam.sdk.io.parquet.ParquetIO.Sink} that, for use with {@link FileIO#write}. */
    public static com.impact.utils.io.ParquetIO.Sink sink(Schema schema) {
        return new AutoValue_ParquetIO_Sink.Builder()
                .setJsonSchema(schema.toString())
                .setCompressionCodec(CompressionCodecName.SNAPPY)
                // This resembles the default value for ParquetWriter.rowGroupSize.
                .setRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .build();
    }

    /** Implementation of {@link #sink}. */
    @AutoValue
    public abstract static class Sink implements FileIO.Sink<GenericRecord> {

        abstract @Nullable String getJsonSchema();

        abstract CompressionCodecName getCompressionCodec();

        abstract @Nullable SerializableConfiguration getConfiguration();

        abstract int getRowGroupSize();

        abstract @Nullable Class<? extends GenericData> getAvroDataModelClass();

        abstract com.impact.utils.io.ParquetIO.Sink.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract com.impact.utils.io.ParquetIO.Sink.Builder setJsonSchema(String jsonSchema);

            abstract com.impact.utils.io.ParquetIO.Sink.Builder setCompressionCodec(CompressionCodecName compressionCodec);

            abstract com.impact.utils.io.ParquetIO.Sink.Builder setConfiguration(SerializableConfiguration configuration);

            abstract com.impact.utils.io.ParquetIO.Sink.Builder setRowGroupSize(int rowGroupSize);

            abstract com.impact.utils.io.ParquetIO.Sink.Builder setAvroDataModelClass(Class<? extends GenericData> modelClass);

            abstract com.impact.utils.io.ParquetIO.Sink build();
        }

        /** Specifies compression codec. By default, CompressionCodecName.SNAPPY. */
        public com.impact.utils.io.ParquetIO.Sink withCompressionCodec(CompressionCodecName compressionCodecName) {
            return toBuilder().setCompressionCodec(compressionCodecName).build();
        }

        /** Specifies configuration to be passed into the sink's writer. */
        public com.impact.utils.io.ParquetIO.Sink withConfiguration(Map<String, String> configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(SerializableConfiguration.fromMap(configuration)).build();
        }

        /** Specify Hadoop configuration for ParquetReader. */
        public com.impact.utils.io.ParquetIO.Sink withConfiguration(Configuration configuration) {
            checkArgument(configuration != null, "configuration can not be null");
            return toBuilder().setConfiguration(new SerializableConfiguration(configuration)).build();
        }

        /** Specify row-group size; if not set or zero, a default is used by the underlying writer. */
        public com.impact.utils.io.ParquetIO.Sink withRowGroupSize(int rowGroupSize) {
            checkArgument(rowGroupSize > 0, "rowGroupSize must be positive");
            return toBuilder().setRowGroupSize(rowGroupSize).build();
        }

        /**
         * Define the Avro data model; see {@link AvroParquetWriter.Builder#withDataModel(GenericData)}.
         */
        public com.impact.utils.io.ParquetIO.Sink withAvroDataModel(GenericData model) {
            return toBuilder().setAvroDataModelClass(model.getClass()).build();
        }

        private transient @Nullable ParquetWriter<GenericRecord> writer;

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            checkNotNull(getJsonSchema(), "Schema cannot be null");

            Schema schema = new Schema.Parser().parse(getJsonSchema());
            Class<? extends GenericData> modelClass = getAvroDataModelClass();

            com.impact.utils.io.ParquetIO.Sink.BeamParquetOutputFile beamParquetOutputFile =
                    new com.impact.utils.io.ParquetIO.Sink.BeamParquetOutputFile(Channels.newOutputStream(channel));

            AvroParquetWriter.Builder<GenericRecord> builder =
                    AvroParquetWriter.<GenericRecord>builder(beamParquetOutputFile)
                            .withSchema(schema)
                            .withCompressionCodec(getCompressionCodec())
                            .withWriteMode(OVERWRITE)
                            .withConf(SerializableConfiguration.newConfiguration(getConfiguration()))
                            .withRowGroupSize(getRowGroupSize());
            if (modelClass != null) {
                try {
                    builder.withDataModel(buildModelObject(modelClass));
                } catch (ReflectiveOperationException e) {
                    throw new IOException(
                            "Couldn't set the specified Avro data model " + modelClass.getName(), e);
                }
            }
            this.writer = builder.build();
        }

        @Override
        public void write(GenericRecord element) throws IOException {
            checkNotNull(writer, "Writer cannot be null");
            writer.write(element);
        }

        @Override
        public void flush() throws IOException {
            // the only way to completely flush the output is to call writer.close() here
            writer.close();
        }

        private static class BeamParquetOutputFile implements OutputFile {

            private final OutputStream outputStream;

            BeamParquetOutputFile(OutputStream outputStream) {
                this.outputStream = outputStream;
            }

            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new com.impact.utils.io.ParquetIO.Sink.BeamOutputStream(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return new com.impact.utils.io.ParquetIO.Sink.BeamOutputStream(outputStream);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        }

        private static class BeamOutputStream extends PositionOutputStream {
            private long position = 0;
            private final OutputStream outputStream;

            private BeamOutputStream(OutputStream outputStream) {
                this.outputStream = outputStream;
            }

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) throws IOException {
                position++;
                outputStream.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                write(b, 0, b.length);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputStream.write(b, off, len);
                position += len;
            }

            @Override
            public void flush() throws IOException {
                outputStream.flush();
            }

            @Override
            public void close() throws IOException {
                outputStream.close();
            }
        }
    }

    /** Returns a model object created using provided modelClass or null. */
    private static GenericData buildModelObject(@Nullable Class<? extends GenericData> modelClass)
            throws ReflectiveOperationException {
        return (modelClass == null) ? null : (GenericData) modelClass.getMethod("get").invoke(null);
    }

    /**
     * Passthrough function to provide seamless backward compatibility to ParquetIO's functionality.
     */
    @VisibleForTesting
    static final class GenericRecordPassthroughFn
            implements SerializableFunction<GenericRecord, GenericRecord> {

        private static final com.impact.utils.io.ParquetIO.GenericRecordPassthroughFn singleton = new com.impact.utils.io.ParquetIO.GenericRecordPassthroughFn();

        static com.impact.utils.io.ParquetIO.GenericRecordPassthroughFn create() {
            return singleton;
        }

        @Override
        public GenericRecord apply(GenericRecord input) {
            //LOG.info(String.format("SYNCSTARTDATETIME Inside GenericRecordPassthroughFn %s", input.get("SYNCSTARTDATETIME")));
            return input;
        }

        /** Enforce singleton pattern, by disallowing construction with {@code new} operator. */
        private GenericRecordPassthroughFn() {}
    }

    /** Disallow construction of utility class. */
    private ParquetIO() {}
}


