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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergBufferedDeleteWriter implements HiveIcebergWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergBufferedDeleteWriter.class);

  private static final String DELETE_FILE_THREAD_POOL_SIZE = "iceberg.delete.file.thread.pool.size";
  private static final int DELETE_FILE_THREAD_POOL_SIZE_DEFAULT = 10;

  // Storing deleted data in a map Partition -> FileName -> BitMap
  private final Map<PartitionKey, Map<String, Roaring64Bitmap>> buffer = Maps.newHashMap();
  private final Map<Integer, PartitionSpec> specs;
  private final Map<PartitionKey, PartitionSpec> keyToSpec = Maps.newHashMap();
  private final FileFormat format;
  private final FileWriterFactory<Record> writerFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final Configuration configuration;
  private final Record record;
  private final InternalRecordWrapper wrapper;
  private FilesForCommit filesForCommit;

  HiveIcebergBufferedDeleteWriter(Schema schema, Map<Integer, PartitionSpec> specs, FileFormat format,
      FileWriterFactory<Record> writerFactory, OutputFileFactory fileFactory, FileIO io, long targetFileSize,
      Configuration configuration) {
    this.specs = specs;
    this.format = format;
    this.writerFactory = writerFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.configuration = configuration;
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.record = GenericRecord.create(schema);
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    IcebergAcidUtil.getOriginalFromUpdatedRecord(rec, record);
    String filePath = (String) rec.getField(MetadataColumns.FILE_PATH.name());
    int specId = IcebergAcidUtil.parseSpecId(rec);

    Map<String, Roaring64Bitmap> deleteMap =
        buffer.computeIfAbsent(partition(record, specId), key -> {
          keyToSpec.put(key, specs.get(specId));
          return Maps.newHashMap();
        });
    Roaring64Bitmap deletes = deleteMap.computeIfAbsent(filePath, unused -> new Roaring64Bitmap());
    deletes.add((Long) rec.getField(MetadataColumns.ROW_POSITION.name()));
  }

  @Override
  public void close(boolean abort) throws IOException {
    long startTime = System.currentTimeMillis();
    Collection<DeleteFile> deleteFiles = new ConcurrentLinkedQueue<>();
    if (!abort) {
      LOG.info("Delete file flush is started");
      Tasks.foreach(buffer.keySet())
          .retry(3)
          .executeWith(fileExecutor(configuration))
          .onFailure((partition, exception) -> LOG.debug("Failed to write delete file {}", partition, exception))
          .run(partition -> {
            PositionDelete<Record> positionDelete = PositionDelete.create();
            PartitioningWriter writerForData =
                new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, format, targetFileSize);
            try (PartitioningWriter writer = writerForData) {
              writerForData = writer;
              for (String filePath : new TreeSet<>(buffer.get(partition).keySet())) {
                Roaring64Bitmap deletes = buffer.get(partition).get(filePath);
                deletes.forEach(position -> {
                  positionDelete.set(filePath, position, null);
                  writer.write(positionDelete, keyToSpec.get(partition), partition);
                });
              }
            }
            deleteFiles.addAll(((DeleteWriteResult) writerForData.result()).deleteFiles());
          }, IOException.class);
    }

    LOG.info("HiveIcebergBufferedDeleteWriter is closed with abort={}. Created {} delete files and it took {} ns.",
        abort, deleteFiles.size(), System.currentTimeMillis() - startTime);
    LOG.debug("Delete files written {}", deleteFiles);

    this.filesForCommit = FilesForCommit.onlyDelete(deleteFiles);
  }

  @Override
  public FilesForCommit files() {
    return filesForCommit;
  }

  protected PartitionKey partition(Record row, int specId) {
    PartitionKey partitionKey = new PartitionKey(specs.get(specId), specs.get(specId).schema());
    partitionKey.partition(wrapper.wrap(row));
    return partitionKey;
  }

  /**
   * Executor service for parallel writing of delete files.
   * @param conf The configuration containing the pool size
   * @return The generated executor service
   */
  private static ExecutorService fileExecutor(Configuration conf) {
    int size = conf.getInt(DELETE_FILE_THREAD_POOL_SIZE, DELETE_FILE_THREAD_POOL_SIZE_DEFAULT);
    return Executors.newFixedThreadPool(
        size,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setPriority(Thread.NORM_PRIORITY)
            .setNameFormat("iceberg-delete-file-pool-%d")
            .build());
  }
}
