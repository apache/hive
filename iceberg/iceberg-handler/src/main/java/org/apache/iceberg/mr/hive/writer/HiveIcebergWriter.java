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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.hive.FilesForCommit;
import org.apache.iceberg.mr.mapred.Container;

public interface HiveIcebergWriter extends FileSinkOperator.RecordWriter,
    org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> {
  FilesForCommit files();
  void close(boolean abort) throws IOException;
  void write(Writable row) throws IOException;

  default void close(Reporter reporter) throws IOException {
    close(false);
  }

  default void write(NullWritable key, Container value) throws IOException {
    write(value);
  }
}
