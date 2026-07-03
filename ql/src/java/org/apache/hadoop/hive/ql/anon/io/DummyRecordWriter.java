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

package org.apache.hadoop.hive.ql.anon.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public class DummyRecordWriter extends RecordWriter<NullWritable, OrcStruct> {

  final private Path path;
  private Writer writer = null;
  final private OrcFile.WriterOptions options;
  final private StructObjectInspector soi;

  public DummyRecordWriter(Path path, OrcFile.WriterOptions options, StructObjectInspector soi) {
    this.path = path;
    this.options = options;
    this.soi = soi;

    this.options.inspector(this.soi);
  }

  @Override
  public void write(NullWritable key, OrcStruct value) throws IOException {
    if (writer == null) {
      init();
    }

    int numFields = value.getNumFields();
    List row = new ArrayList();
    for (int i = 0; i < numFields; i++) {
      row.add(value.getFieldValue(i));
    }

    writer.addRow(row);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    if (writer == null) {
      return;
    }
    writer.close();
  }

  private void init() throws IOException {
    writer = OrcFile.createWriter(path, options);
  }

}
