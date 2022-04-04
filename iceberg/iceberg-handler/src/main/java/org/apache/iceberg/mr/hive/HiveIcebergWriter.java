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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class HiveIcebergWriter implements FileSinkOperator.RecordWriter,
    org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> {

  private static final Map<TaskAttemptID, Map<String, HiveIcebergWriter>> recordWriters = Maps.newConcurrentMap();
  private static final Map<TaskAttemptID, Map<String, HiveIcebergWriter>> deleteWriters = Maps.newConcurrentMap();

  static void removeWriters(TaskAttemptID taskAttemptID) {
    recordWriters.remove(taskAttemptID);
    deleteWriters.remove(taskAttemptID);
  }

  static Map<String, HiveIcebergWriter> getRecordWriters(TaskAttemptID taskAttemptID) {
    return recordWriters.get(taskAttemptID);
  }

  static Map<String, HiveIcebergWriter> getDeleteWriters(TaskAttemptID taskAttemptID) {
    return deleteWriters.get(taskAttemptID);
  }

  protected final PartitionKey currentKey;
  protected final FileIO io;
  protected final InternalRecordWrapper wrapper;
  protected final PartitionSpec spec;

  protected HiveIcebergWriter(Schema schema, PartitionSpec spec, FileIO io, TaskAttemptID attemptID, String tableName,
      boolean isDeleteWriter) {
    this.io = io;
    this.currentKey = new PartitionKey(spec, schema);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.spec = spec;
    if (isDeleteWriter) {
      deleteWriters.putIfAbsent(attemptID, Maps.newConcurrentMap());
      deleteWriters.get(attemptID).put(tableName, this);
    } else {
      recordWriters.putIfAbsent(attemptID, Maps.newConcurrentMap());
      recordWriters.get(attemptID).put(tableName, this);
    }
  }

  protected PartitionKey partition(Record row) {
    currentKey.partition(wrapper.wrap(row));
    return currentKey;
  }

  protected List<DataFile> dataFiles() {
    return Collections.emptyList();
  }

  protected List<DeleteFile> deleteFiles() {
    return Collections.emptyList();
  }

  @Override
  public void write(NullWritable key, Container value) throws IOException {
    write(value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
