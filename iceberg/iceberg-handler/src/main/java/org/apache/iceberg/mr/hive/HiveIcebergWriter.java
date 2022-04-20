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
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class HiveIcebergWriter implements FileSinkOperator.RecordWriter,
    org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergWriter.class);

  private static final Map<TaskAttemptID, Map<String, HiveIcebergWriter>> writers = Maps.newConcurrentMap();

  static Map<String, HiveIcebergWriter> removeWriters(TaskAttemptID taskAttemptID) {
    return writers.remove(taskAttemptID);
  }

  static Map<String, HiveIcebergWriter> getWriters(TaskAttemptID taskAttemptID) {
    return writers.get(taskAttemptID);
  }

  protected final FileIO io;
  protected final InternalRecordWrapper wrapper;
  protected final Map<Integer, PartitionSpec> specs;
  protected final Map<Integer, PartitionKey> partitionKeys;
  protected final PartitioningWriter writer;

  protected HiveIcebergWriter(Schema schema, Map<Integer, PartitionSpec> specs, FileIO io, TaskAttemptID attemptID,
      String tableName, PartitioningWriter writer) {
    this.io = io;
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.specs = specs;
    this.partitionKeys = Maps.newHashMapWithExpectedSize(specs.size());
    this.writer = writer;
    writers.putIfAbsent(attemptID, Maps.newConcurrentMap());
    writers.get(attemptID).put(tableName, this);
  }

  protected abstract FilesForCommit files();

  @Override
  public void write(NullWritable key, Container value) throws IOException {
    write(value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
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
          .run(file -> io.deleteFile(file.path().toString()));
    }

    LOG.info("HiveIcebergWriter is closed with abort={}. Created {} data files and {} delete files", abort,
        result.dataFiles().size(), result.deleteFiles().size());
  }

  protected PartitionKey partition(Record row) {
    // get partition key for the latest spec
    return partition(row, specs.size() - 1);
  }

  protected PartitionKey partition(Record row, int specId) {
    PartitionKey partitionKey = partitionKeys.computeIfAbsent(specId,
        id -> new PartitionKey(specs.get(id), specs.get(id).schema()));
    partitionKey.partition(wrapper.wrap(row));
    return partitionKey;
  }
}
