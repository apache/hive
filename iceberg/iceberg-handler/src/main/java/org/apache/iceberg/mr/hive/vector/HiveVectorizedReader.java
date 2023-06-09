/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.VectorizedParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.iceberg.org.apache.orc.OrcConf;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.orc.VectorizedReadUtils;
import org.apache.iceberg.parquet.ParquetFooterInputFromCache;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.impl.OrcTail;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Utility class to create vectorized readers for Hive.
 * As per the file format of the task, it will create a matching vectorized record reader that is already implemented
 * in Hive. It will also do some tweaks on the produced vectors for Iceberg's use e.g. partition column handling.
 */
public class HiveVectorizedReader {


  private HiveVectorizedReader() {

  }

  public static CloseableIterable<HiveBatchContext> reader(Table table, Path path, FileScanTask task,
      Map<Integer, ?> idToConstant, TaskAttemptContext context, Expression residual, Schema readSchema) {

    HiveDeleteFilter deleteFilter = null;
    Schema requiredSchema = readSchema;

    if (!task.deletes().isEmpty()) {
      deleteFilter = new HiveDeleteFilter(table.io(), task, table.schema(), prepareSchemaForDeleteFilter(readSchema));
      requiredSchema = deleteFilter.requiredSchema();
      // TODO: take requiredSchema and adjust readColumnIds below accordingly for equality delete cases
      // and remove below limitation
      if (task.deletes().stream().anyMatch(d -> d.content() == FileContent.EQUALITY_DELETES)) {
        throw new UnsupportedOperationException("Vectorized reading with equality deletes is not supported yet.");
      }
    }


    // Tweaks on jobConf here are relevant for this task only, so we need to copy it first as context's conf is reused..
    JobConf job = new JobConf(context.getConfiguration());
    FileFormat format = task.file().format();
    Reporter reporter = ((MapredIcebergInputFormat.CompatibilityTaskAttemptContextImpl) context).getLegacyReporter();

    // Hive by default requires partition columns to be read too. This is not required for identity partition
    // columns, as we will add this as constants later.

    int[] partitionColIndices = null;
    Object[] partitionValues = null;
    PartitionSpec partitionSpec = task.spec();
    List<Integer> readColumnIds = ColumnProjectionUtils.getReadColumnIDs(job);

    if (!partitionSpec.isUnpartitioned()) {

      List<PartitionField> fields = partitionSpec.fields();
      List<Integer> partitionColIndicesList = Lists.newLinkedList();
      List<Object> partitionValuesList = Lists.newLinkedList();

      for (PartitionField partitionField : fields) {
        if (partitionField.transform().isIdentity()) {

          // Get columns in read schema order (which matches those of readColumnIds) to find partition column indices
          List<Types.NestedField> columns = task.spec().schema().columns();
          for (int colIdx = 0; colIdx < columns.size(); ++colIdx) {
            if (columns.get(colIdx).fieldId() == partitionField.sourceId()) {
              // Skip reading identity partition columns from source file...
              readColumnIds.remove((Integer) colIdx);

              // ...and use the corresponding constant value instead
              partitionColIndicesList.add(colIdx);
              partitionValuesList.add(idToConstant.get(partitionField.sourceId()));
              break;
            }
          }
        }
      }

      partitionColIndices = ArrayUtils.toPrimitive(partitionColIndicesList.toArray(new Integer[0]));
      partitionValues = partitionValuesList.toArray(new Object[0]);

      ColumnProjectionUtils.setReadColumns(job, readColumnIds);
    }

    try {

      long start = task.start();
      long length = task.length();

      // TODO: Iceberg currently does not track the last modification time of a file. Until that's added,
      // we need to set Long.MIN_VALUE as last modification time in the fileId triplet.
      SyntheticFileId fileId = new SyntheticFileId(path, task.file().fileSizeInBytes(), Long.MIN_VALUE);
      fileId.toJobConf(job);
      RecordReader<NullWritable, VectorizedRowBatch> recordReader = null;

      switch (format) {
        case ORC:
          recordReader = orcRecordReader(job, reporter, task, path, start, length, readColumnIds,
              fileId, residual, table.name());
          break;

        case PARQUET:
          recordReader = parquetRecordReader(job, reporter, task, path, start, length, fileId);
          break;
        default:
          throw new UnsupportedOperationException("Vectorized Hive reading unimplemented for format: " + format);
      }

      CloseableIterable<HiveBatchContext> vrbIterable =
          createVectorizedRowBatchIterable(recordReader, job, partitionColIndices, partitionValues, idToConstant);

      return deleteFilter != null ? deleteFilter.filterBatch(vrbIterable) : vrbIterable;

    } catch (IOException ioe) {
      throw new RuntimeException("Error creating vectorized record reader for " + path, ioe);
    }
  }

  private static RecordReader<NullWritable, VectorizedRowBatch> orcRecordReader(JobConf job, Reporter reporter,
      FileScanTask task, Path path, long start, long length, List<Integer> readColumnIds,
      SyntheticFileId fileId, Expression residual, String tableName) throws IOException {
    RecordReader<NullWritable, VectorizedRowBatch> recordReader = null;

    // Need to turn positional schema evolution off since we use column name based schema evolution for projection
    // and Iceberg will make a mapping between the file schema and the current reading schema.
    job.setBoolean(OrcConf.FORCE_POSITIONAL_EVOLUTION.getHiveConfName(), false);

    // Metadata information has to be passed along in the OrcSplit. Without specifying this, the vectorized
    // reader will assume that the ORC file ends at the task's start + length, and might fail reading the tail..
    ByteBuffer serializedOrcTail = VectorizedReadUtils.getSerializedOrcTail(path, fileId, job);
    OrcTail orcTail = VectorizedReadUtils.deserializeToOrcTail(serializedOrcTail);

    VectorizedReadUtils.handleIcebergProjection(task, job,
        VectorizedReadUtils.deserializeToShadedOrcTail(serializedOrcTail).getSchema(), residual);

    // If LLAP enabled, try to retrieve an LLAP record reader - this might yield to null in some special cases
    // TODO: add support for reading files with positional deletes with LLAP (LLAP would need to provide file row num)
    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.LLAP_IO_ENABLED, LlapProxy.isDaemon()) &&
        LlapProxy.getIo() != null && task.deletes().isEmpty() && !InputFormatConfig.fetchVirtualColumns(job)) {
      boolean isDisableVectorization =
          job.getBoolean(HiveIcebergInputFormat.getVectorizationConfName(tableName), false);
      if (isDisableVectorization) {
        // Required to prevent LLAP from dealing with decimal64, HiveIcebergInputFormat.getSupportedFeatures()
        HiveConf.setVar(job, HiveConf.ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED, "");
      }
      recordReader = LlapProxy.getIo().llapVectorizedOrcReaderForPath(fileId, path, null, readColumnIds,
          job, start, length, reporter);
    }

    if (recordReader == null) {
      InputSplit split = new OrcSplit(path, fileId, start, length, (String[]) null, orcTail,
          false, false, com.google.common.collect.Lists.newArrayList(), 0, length, path.getParent(), null);
      recordReader = new VectorizedOrcInputFormat().getRecordReader(split, job, reporter);
    }

    return recordReader;
  }

  private static RecordReader<NullWritable, VectorizedRowBatch> parquetRecordReader(JobConf job, Reporter reporter,
      FileScanTask task, Path path, long start, long length, SyntheticFileId fileId) throws IOException {
    InputSplit split = new FileSplit(path, start, length, job);
    VectorizedParquetInputFormat inputFormat = new VectorizedParquetInputFormat();

    MemoryBufferOrBuffers footerData = null;
    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.LLAP_IO_ENABLED, LlapProxy.isDaemon()) &&
        LlapProxy.getIo() != null && LlapProxy.getIo().usingLowLevelCache()) {
      LlapProxy.getIo().initCacheOnlyInputFormat(inputFormat);
      footerData = LlapProxy.getIo().getParquetFooterBuffersFromCache(path, job, fileId);
    }

    ParquetMetadata parquetMetadata = footerData != null ?
        ParquetFileReader.readFooter(new ParquetFooterInputFromCache(footerData), ParquetMetadataConverter.NO_FILTER) :
        ParquetFileReader.readFooter(job, path);
    inputFormat.setMetadata(parquetMetadata);

    MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
    MessageType typeWithIds = null;
    Schema expectedSchema = task.spec().schema();

    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      typeWithIds = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
    } else {
      typeWithIds = ParquetSchemaUtil.pruneColumnsFallback(ParquetSchemaUtil.addFallbackIds(fileSchema),
          expectedSchema);
    }

    ParquetSchemaFieldNameVisitor psv = new ParquetSchemaFieldNameVisitor(fileSchema);
    TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), typeWithIds, psv);
    job.set(IOConstants.COLUMNS, psv.retrieveColumnNameList());

    return inputFormat.getRecordReader(split, job, reporter);
  }

  private static CloseableIterable<HiveBatchContext> createVectorizedRowBatchIterable(
      RecordReader<NullWritable, VectorizedRowBatch> hiveRecordReader, JobConf job, int[] partitionColIndices,
      Object[] partitionValues, Map<Integer, ?> idToConstant) {

    HiveBatchIterator iterator =
        new HiveBatchIterator(hiveRecordReader, job, partitionColIndices, partitionValues, idToConstant);

    return new CloseableIterable<HiveBatchContext>() {

      @Override
      public CloseableIterator iterator() {
        return iterator;
      }

      @Override
      public void close() throws IOException {
        iterator.close();
      }
    };
  }

  /**
   * We need to add IS_DELETED metadata field so that DeleteFilter marks deleted rows rather than filering them out.
   * @param schema original schema
   * @return adjusted schema
   */
  private static Schema prepareSchemaForDeleteFilter(Schema schema) {
    List<Types.NestedField> columns = Lists.newArrayList(schema.columns());
    columns.add(MetadataColumns.IS_DELETED);
    return new Schema(columns);
  }

}
