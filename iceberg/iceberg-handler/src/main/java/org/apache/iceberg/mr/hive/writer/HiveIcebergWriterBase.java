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

package org.apache.iceberg.mr.hive.writer;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.io.ClusteredDataWriter;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.hive.writer.WriterBuilder.Context;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:VisibilityModifier")
abstract class HiveIcebergWriterBase implements HiveIcebergWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergWriterBase.class);

  protected final FileIO io;
  protected final InternalRecordWrapper wrapper;
  protected final Map<Integer, PartitionSpec> specs;
  protected final Map<Integer, PartitionKey> partitionKeys;
  protected final PartitioningWriter writer;

  HiveIcebergWriterBase(Table table, PartitioningWriter writer) {
    this.io = table.io();
    this.wrapper = new InternalRecordWrapper(table.schema().asStruct());
    this.specs = table.specs();
    this.partitionKeys = Maps.newHashMapWithExpectedSize(specs.size());
    this.writer = writer;
  }

  @Override
  public void close(boolean abort) throws IOException {
    writer.close();
    FilesForCommit result = files();

    // If abort then remove the unnecessary files
    if (abort) {
      Tasks.foreach(result.allFiles())
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exception) -> LOG.debug("Failed on to remove file {} on abort", file, exception))
          .run(file -> io.deleteFile(file.location()));
      LOG.warn("HiveIcebergWriter is closed with abort");
    }

    LOG.info("Created {} data files and {} delete files",
        result.dataFiles().size(), result.deleteFiles().size());
    LOG.debug(result.toString());
  }

  protected PartitionKey partition(Record row, int specId) {
    PartitionKey partitionKey = partitionKeys.computeIfAbsent(specId,
        id -> new PartitionKey(specs.get(id), specs.get(id).schema()));
    partitionKey.partition(wrapper.wrap(row));
    return partitionKey;
  }

  // use a fanout writer only if enabled and the input is unordered and the table is partitioned
  static PartitioningWriter<Record, DataWriteResult> newDataWriter(
      Table table, HiveFileWriterFactory writers, OutputFileFactory files, Context context) {

    FileIO io = table.io();
    boolean useFanoutWriter = context.useFanoutWriter();
    long targetFileSize = context.targetDataFileSize();

    if (table.spec().isPartitioned() && useFanoutWriter) {
      return new FanoutDataWriter<>(writers, files, io, targetFileSize);
    } else {
      return new ClusteredDataWriter<>(writers, files, io, targetFileSize);
    }
  }

  // Use the DV writer for V3+ tables
  // The spec requires position deletes to be ordered by file and position for V2 tables
  // use a fanout writer if the input is unordered no matter whether fanout writers are enabled
  // clustered writers assume that the position deletes are already ordered by file and position
  static PartitioningWriter<PositionDelete<Record>, DeleteWriteResult> newDeleteWriter(
      Table table, Map<String, DeleteFileSet> rewritableDeletes, HiveFileWriterFactory writers,
      OutputFileFactory files, Context context) {

    Function<CharSequence, PositionDeleteIndex> previousDeleteLoader =
        PreviousDeleteLoader.create(table, rewritableDeletes);
    FileIO io = table.io();
    boolean inputOrdered = context.inputOrdered();
    long targetFileSize = context.targetDeleteFileSize();
    DeleteGranularity deleteGranularity = context.deleteGranularity();

    if (context.useDVs()) {
      return new PartitioningDVWriter<>(files, previousDeleteLoader);
    } else if (inputOrdered && rewritableDeletes == null) {
      return new ClusteredPositionDeleteWriter<>(writers, files, io, targetFileSize, deleteGranularity);
    } else {
      return new FanoutPositionOnlyDeleteWriter<>(
          writers, files, io, targetFileSize, deleteGranularity, previousDeleteLoader);
    }
  }

  private static class PreviousDeleteLoader implements Function<CharSequence, PositionDeleteIndex> {
    private final Map<String, DeleteFileSet> deleteFiles;
    private final DeleteLoader deleteLoader;

    private PreviousDeleteLoader(Table table, Map<String, DeleteFileSet> deleteFiles) {
      this.deleteFiles = deleteFiles;
      this.deleteLoader = new BaseDeleteLoader(
          deleteFile -> EncryptingFileIO.combine(table.io(), table.encryption()).newInputFile(deleteFile));
    }

    @Override
    public PositionDeleteIndex apply(CharSequence path) {
      DeleteFileSet deleteFileSet = deleteFiles.get(path.toString());
      if (deleteFileSet == null) {
        return null;
      }
      return deleteLoader.loadPositionDeletes(deleteFileSet, path);
    }

    public static Function<CharSequence, PositionDeleteIndex> create(
        Table table, Map<String, DeleteFileSet> deleteFiles) {
      if (deleteFiles == null) {
        return path -> null;
      }
      return new PreviousDeleteLoader(table, deleteFiles);
    }
  }

}
