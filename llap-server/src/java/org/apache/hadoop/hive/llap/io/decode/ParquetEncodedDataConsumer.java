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
package org.apache.hadoop.hive.llap.io.decode;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.Includes;
import org.apache.hadoop.hive.llap.io.encoded.ParquetEncodedColumnBatch;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetRowGroupDecoder;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedColumnReader;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import com.google.common.base.Strings;

/**
 * The Parquet counterpart of {@link OrcEncodedDataConsumer}: it turns one row group's worth of
 * cached column-chunk buffers ({@link ParquetEncodedColumnBatch}) into {@link ColumnVectorBatch}es
 * and hands them downstream.
 *
 * <p>Rather than reimplementing Parquet value decoding, this consumer builds a
 * {@link ParquetCachedPageReadStore} directly over the cached buffers (pages are parsed in place and
 * decompressed lazily, no ParquetFileReader and no whole-chunk copy) and drives the very same
 * {@link VectorizedColumnReader}s the non-cached vectorized reader uses (via {@link ParquetRowGroupDecoder}).
 */
public class ParquetEncodedDataConsumer
    extends EncodedDataConsumer<Object, ParquetEncodedColumnBatch> {

  private final Configuration jobConf;
  private final boolean useDecimal64ColumnVectors;
  private final CompressionCodecFactory codecFactory;
  private final ParquetMetadataConverter converter;
  private ParquetMetadata footer;
  private MessageType requestedSchema;
  private Path path;
  private Map<String, Object> initialDefaults;

  // Derived lazily (once) from the footer + job conf, then reused across row groups.
  private List<TypeInfo> columnTypesList;
  private List<Integer> colsToInclude;
  private boolean readAllColumns;
  private boolean skipTimestampConversion;
  private boolean skipProlepticConversion;
  private boolean legacyConversionEnabled;
  private ZoneId writerTimezone;
  private boolean schemaInitialized = false;

  public ParquetEncodedDataConsumer(Consumer<ColumnVectorBatch> consumer, Includes includes,
      QueryFragmentCounters counters, LlapDaemonIOMetrics ioMetrics, Configuration jobConf) {
    super(consumer, includes.getPhysicalColumnIds().size(), ioMetrics, counters);
    this.jobConf = jobConf;
    this.useDecimal64ColumnVectors = HiveConf.getVar(jobConf,
        ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
    ParquetReadOptions options = HadoopReadOptions.builder(jobConf).build();
    this.codecFactory = options.getCodecFactory();
    this.converter = new ParquetMetadataConverter(options);
  }

  public void setFileMetadata(ParquetMetadata footer, MessageType requestedSchema, Path path) {
    this.footer = footer;
    this.requestedSchema = requestedSchema;
    this.path = path;
  }

  public void setInitialDefaults(Map<String, Object> initialDefaults) {
    this.initialDefaults = initialDefaults;
  }

  /**
   * Derives the Hive type / projection lists and the conversion flags the same way
   * {@link org.apache.hadoop.hive.ql.io.parquet.ParquetRecordReaderBase} does from the job conf,
   * so the shared {@link ParquetRowGroupDecoder} produces identical readers.
   */
  private void initSchema() {
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = footer.getFileMetaData();
    java.util.Map<String, String> kvMeta = fileMetaData.getKeyValueMetaData();

    this.columnTypesList =
        DataWritableReadSupport.getColumnTypes(jobConf.get(IOConstants.COLUMNS_TYPES));
    this.colsToInclude = ColumnProjectionUtils.getReadColumnIDs(jobConf);
    this.readAllColumns = ColumnProjectionUtils.isReadAllColumns(jobConf);

    this.writerTimezone = DataWritableReadSupport.getWriterTimeZoneId(kvMeta);
    if (HiveConf.getBoolVar(jobConf, ConfVars.HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION)) {
      this.skipTimestampConversion =
          !Strings.nullToEmpty(fileMetaData.getCreatedBy()).startsWith("parquet-mr");
    } else {
      this.skipTimestampConversion = false;
    }
    Boolean proleptic = DataWritableReadSupport.getWriterDateProleptic(kvMeta);
    if (proleptic == null) {
      proleptic = HiveConf.getBoolVar(jobConf, ConfVars.HIVE_PARQUET_DATE_PROLEPTIC_GREGORIAN_DEFAULT);
    }
    this.skipProlepticConversion = proleptic;
    this.legacyConversionEnabled = DataWritableReadSupport.getZoneConversionLegacy(kvMeta, jobConf);

    this.schemaInitialized = true;
  }

  @Override
  protected void decodeBatch(ParquetEncodedColumnBatch batch,
      Consumer<ColumnVectorBatch> downstreamConsumer) throws InterruptedException {
    if (!schemaInitialized) {
      initSchema();
    }

    long startTime = counters.startTimeCounter();
    try {
      PageReadStore pages = new ParquetCachedPageReadStore(footer, batch, codecFactory, converter);

      VectorizedColumnReader[] columnReaders =
          new ParquetRowGroupDecoder(footer.getFileMetaData().getSchema(), initialDefaults).buildColumnReaders(
              pages, requestedSchema, columnTypesList, colsToInclude, readAllColumns,
              skipTimestampConversion, writerTimezone, skipProlepticConversion,
              legacyConversionEnabled);

      long rowCount = pages.getRowCount();
      long rowsLeft = rowCount;
      int batches = 0;
      while (rowsLeft > 0) {
        int batchSize = (int) Math.min(VectorizedRowBatch.DEFAULT_SIZE, rowsLeft);

        ColumnVectorBatch cvb = cvbPool.take();
        cvb.filterContext.reset();
        cvb.size = batchSize;

        // columnReaders[i] is requestedSchema field i, i.e. the i-th projected column.
        for (int i = 0; i < columnReaders.length; ++i) {
          if (columnReaders[i] == null) {
            continue;
          }
          TypeInfo columnType = readAllColumns
              ? columnTypesList.get(i)
              : columnTypesList.get(colsToInclude.get(i));
          ColumnVector cv = prepareColumnVector(cvb, i, columnType, batchSize);
          columnReaders[i].readBatch(batchSize, cv, columnType);
        }

        downstreamConsumer.consumeData(cvb);
        counters.incrCounter(LlapIOCounters.ROWS_EMITTED, batchSize);
        rowsLeft -= batchSize;
        ++batches;
      }
      counters.incrWallClockCounter(LlapIOCounters.DECODE_TIME_NS, startTime);
      counters.incrCounter(LlapIOCounters.NUM_VECTOR_BATCHES, batches);
      counters.incrCounter(LlapIOCounters.NUM_DECODED_BATCHES);
    } catch (IOException | RuntimeException e) {
      // parquet-mr reports decode failures as runtime ParquetDecodingException.
      LlapIoImpl.LOG.error("Parquet decodeBatch failed for rowGroup " + batch.rowGroupIx + " of " + path, e);
      downstreamConsumer.setError(e);
    } finally {
      // Returns the pooled Hadoop decompressors after each row group; getDecompressor re-creates them.
      codecFactory.release();
    }
  }

  private ColumnVector prepareColumnVector(ColumnVectorBatch cvb, int idx, TypeInfo columnType,
      int batchSize) {
    if (cvb.cols[idx] == null) {
      cvb.cols[idx] = VectorizedBatchUtil.createColumnVector(columnType, physicalVariation(columnType));
    }
    ColumnVector cv = cvb.cols[idx];
    cv.reset();
    cv.ensureSize(batchSize, false);
    cv.isRepeating = true;
    return cv;
  }

  private DataTypePhysicalVariation physicalVariation(TypeInfo columnType) {
    if (useDecimal64ColumnVectors && columnType instanceof DecimalTypeInfo
        && ((DecimalTypeInfo) columnType).precision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
      return DataTypePhysicalVariation.DECIMAL_64;
    }
    return DataTypePhysicalVariation.NONE;
  }

  @Override
  public SchemaEvolution getSchemaEvolution() {
    return null;
  }
}
