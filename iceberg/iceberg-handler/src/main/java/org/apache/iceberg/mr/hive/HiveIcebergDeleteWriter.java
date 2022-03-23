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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergDeleteWriter extends ClusteredPositionDeleteWriter<Record> implements HiveIcebergWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergDeleteWriter.class);
  // <TaskAttemptId, <TABLE_NAME, HiveIcebergDeleteWriter>> map to store the active writers
  // Stored in concurrent map, since some executor engines can share containers
  private static final Map<TaskAttemptID, Map<String, HiveIcebergDeleteWriter>> writers = Maps.newConcurrentMap();

  static Map<String, HiveIcebergDeleteWriter> removeWriters(TaskAttemptID taskAttemptID) {
    return writers.remove(taskAttemptID);
  }
  static Map<String, HiveIcebergDeleteWriter> getWriters(TaskAttemptID taskAttemptID) {
    return writers.get(taskAttemptID);
  }

  private final FileIO io;
  private final PartitionSpec spec;
  private final InternalRecordWrapper wrapper;

  HiveIcebergDeleteWriter(FileWriterFactory<Record> writerFactory, Schema schema, PartitionSpec spec,
      FileFormat fileFormat, OutputFileFactory fileFactory, FileIO io, long targetFileSize, TaskAttemptID taskAttemptID,
      String tableName) {
    super(writerFactory, fileFactory, io, fileFormat, targetFileSize);
    this.io = io;
    this.spec = spec;
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    writers.putIfAbsent(taskAttemptID, Maps.newConcurrentMap());
    writers.get(taskAttemptID).put(tableName, this);
  }

  public DeleteFile[] deleteFiles() {
    return result().deleteFiles().toArray(new DeleteFile[0]);
  }

  @Override
  public void write(Writable row) throws IOException {
    Record rec = ((Container<Record>) row).get();
    PositionDelete<Record> positionDelete = IcebergAcidUtil.buildPositionDelete(spec.schema(), rec);
    PartitionKey partitionKey = new PartitionKey(spec, spec.schema());
    partitionKey.partition(wrapper.wrap(positionDelete.row()));
    super.write(positionDelete, spec, partitionKey);
  }

  @Override
  public void write(NullWritable key, Container value) throws IOException {
    write(value);
  }

  @Override
  public void close(boolean abort) throws IOException {
    super.close();

    // If abort then remove the unnecessary files
    if (abort) {
      List<DeleteFile> files = result().deleteFiles();
      Tasks.foreach(files)
          .retry(3)
          .suppressFailureWhenFinished()
          .onFailure((file, exception) -> LOG.debug("Failed on to remove delete file {} on abort", file, exception))
          .run(deleteFile -> io.deleteFile(deleteFile.path().toString()));
    }

    LOG.info("IcebergDeleteWriter is closed with abort={}.", abort);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
