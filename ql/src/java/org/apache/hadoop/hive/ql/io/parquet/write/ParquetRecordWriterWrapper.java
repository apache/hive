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
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.util.Progressable;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.util.ContextUtil;

public class ParquetRecordWriterWrapper implements RecordWriter<Void, ParquetHiveRecord>,
  org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {

  public static final Log LOG = LogFactory.getLog(ParquetRecordWriterWrapper.class);

  private final org.apache.hadoop.mapreduce.RecordWriter<Void, ParquetHiveRecord> realWriter;
  private final TaskAttemptContext taskContext;

  public ParquetRecordWriterWrapper(
      final OutputFormat<Void, ParquetHiveRecord> realOutputFormat,
      final JobConf jobConf,
      final String name,
      final Progressable progress, Properties tableProperties) throws
          IOException {
    try {
      // create a TaskInputOutputContext
      TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }
      taskContext = ContextUtil.newTaskAttemptContext(jobConf, taskAttemptID);

      LOG.info("initialize serde with table properties.");
      initializeSerProperties(taskContext, tableProperties);

      LOG.info("creating real writer to write at " + name);

      realWriter =
              ((ParquetOutputFormat) realOutputFormat).getRecordWriter(taskContext, new Path(name));

      LOG.info("real writer: " + realWriter);
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void initializeSerProperties(JobContext job, Properties tableProperties) {
    String blockSize = tableProperties.getProperty(ParquetOutputFormat.BLOCK_SIZE);
    Configuration conf = ContextUtil.getConfiguration(job);
    if (blockSize != null && !blockSize.isEmpty()) {
      LOG.debug("get override parquet.block.size property via tblproperties");
      conf.setInt(ParquetOutputFormat.BLOCK_SIZE, Integer.valueOf(blockSize));
    }

    String enableDictionaryPage =
      tableProperties.getProperty(ParquetOutputFormat.ENABLE_DICTIONARY);
    if (enableDictionaryPage != null && !enableDictionaryPage.isEmpty()) {
      LOG.debug("get override parquet.enable.dictionary property via tblproperties");
      conf.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY,
        Boolean.valueOf(enableDictionaryPage));
    }

    String compressionName = tableProperties.getProperty(ParquetOutputFormat.COMPRESSION);
    if (compressionName != null && !compressionName.isEmpty()) {
      //get override compression properties via "tblproperties" clause if it is set
      LOG.debug("get override compression properties via tblproperties");
      CompressionCodecName codecName = CompressionCodecName.fromConf(compressionName);
      conf.set(ParquetOutputFormat.COMPRESSION, codecName.name());
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
  public void write(final Void key, final ParquetHiveRecord value) throws IOException {
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
    write(null, (ParquetHiveRecord) w);
  }

}
