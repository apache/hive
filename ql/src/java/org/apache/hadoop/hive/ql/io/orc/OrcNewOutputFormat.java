/**
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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** An OutputFormat that writes ORC files. */
public class OrcNewOutputFormat extends
    FileOutputFormat<NullWritable, OrcSerdeRow> {

  private static class OrcRecordWriter
  extends RecordWriter<NullWritable, OrcSerdeRow> {
    private Writer writer = null;
    private final Path path;
    private final OrcFile.WriterOptions options;
    OrcRecordWriter(Path path, OrcFile.WriterOptions options) {
      this.path = path;
      this.options = options;
    }
    @Override
    public void write(NullWritable key, OrcSerdeRow row)
        throws IOException, InterruptedException {
      if (writer == null) {
        options.inspector(row.getInspector());
        writer = OrcFile.createWriter(path, options);
      }
      writer.addRow(row.getRow());
    }

    @Override
    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
      if (writer == null) {
        // a row with no columns
        ObjectInspector inspector = ObjectInspectorFactory.
            getStandardStructObjectInspector(new ArrayList<String>(),
                new ArrayList<ObjectInspector>());
        options.inspector(inspector);
        writer = OrcFile.createWriter(path, options);
      }
      writer.close();
    }
  }

  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Path file = getDefaultWorkFile(context, "");
    return new
        OrcRecordWriter(file, OrcFile.writerOptions(
            ShimLoader.getHadoopShims().getConfiguration(context)));
  }
}
