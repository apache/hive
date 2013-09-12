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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.rcfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The RC file input format using new Hadoop mapreduce APIs.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.rcfile.RCFileMapReduceOutputFormat} instead
 */
public class RCFileMapReduceOutputFormat extends
  FileOutputFormat<WritableComparable<?>, BytesRefArrayWritable> {

  /**
   * Set number of columns into the given configuration.
   * @param conf
   *          configuration instance which need to set the column number
   * @param columnNum
   *          column number for RCFile's Writer
   *
   */
  public static void setColumnNumber(Configuration conf, int columnNum) {
    assert columnNum > 0;
    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
  }

  /* (non-Javadoc)
  * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
  */
  @Override
  public org.apache.hadoop.mapreduce.RecordWriter<WritableComparable<?>, BytesRefArrayWritable> getRecordWriter(
    TaskAttemptContext task) throws IOException, InterruptedException {

    //FileOutputFormat.getWorkOutputPath takes TaskInputOutputContext instead of
    //TaskAttemptContext, so can't use that here
    FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(task);
    Path outputPath = committer.getWorkPath();

    FileSystem fs = outputPath.getFileSystem(task.getConfiguration());

    if (!fs.exists(outputPath)) {
      fs.mkdirs(outputPath);
    }

    Path file = getDefaultWorkFile(task, "");

    CompressionCodec codec = null;
    if (getCompressOutput(task)) {
      Class<?> codecClass = getOutputCompressorClass(task, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, task.getConfiguration());
    }

    final RCFile.Writer out = new RCFile.Writer(fs, task.getConfiguration(), file, task, codec);

    return new RecordWriter<WritableComparable<?>, BytesRefArrayWritable>() {

      /* (non-Javadoc)
      * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
      */
      @Override
      public void write(WritableComparable<?> key, BytesRefArrayWritable value)
        throws IOException {
        out.append(value);
      }

      /* (non-Javadoc)
      * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
      */
      @Override
      public void close(TaskAttemptContext task) throws IOException, InterruptedException {
        out.close();
      }
    };
  }

}
