/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.write;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;

public class ParquetRecordWriterWrapper implements RecordWriter<Void, ArrayWritable>,
  FSRecordWriter {

  public static final Log LOG = LogFactory.getLog(ParquetRecordWriterWrapper.class);

  private final org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable> realWriter;
  private final TaskAttemptContext taskContext;

  public ParquetRecordWriterWrapper(
      final OutputFormat<Void, ArrayWritable> realOutputFormat,
      final JobConf jobConf,
      final String name,
      final Progressable progress) throws IOException {
    try {
      // create a TaskInputOutputContext
      TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }
      taskContext = ContextUtil.newTaskAttemptContext(jobConf, taskAttemptID);

      LOG.info("creating real writer to write at " + name);
      realWriter = (org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable>)
          ((ParquetOutputFormat) realOutputFormat).getRecordWriter(taskContext, new Path(name));
      LOG.info("real writer: " + realWriter);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close(final Reporter reporter) throws IOException {
    try {
      realWriter.close(taskContext);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(final Void key, final ArrayWritable value) throws IOException {
    try {
      realWriter.write(key, value);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close(final boolean abort) throws IOException {
    close(null);
  }

  @Override
  public void write(final Writable w) throws IOException {
    write(null, (ArrayWritable) w);
  }

}
