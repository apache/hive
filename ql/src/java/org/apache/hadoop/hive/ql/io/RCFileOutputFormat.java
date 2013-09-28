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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * RCFileOutputFormat.
 *
 */
public class RCFileOutputFormat extends
    FileOutputFormat<WritableComparable, BytesRefArrayWritable> implements
    HiveOutputFormat<WritableComparable, BytesRefArrayWritable> {

  /**
   * set number of columns into the given configuration.
   *
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

  /**
   * Returns the number of columns set in the conf for writers.
   *
   * @param conf
   * @return number of columns for RCFile's writer
   */
  public static int getColumnNumber(Configuration conf) {
    return conf.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
  }

  /** {@inheritDoc} */
  @Override
  public RecordWriter<WritableComparable, BytesRefArrayWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {

    Path outputPath = getWorkOutputPath(job);
    FileSystem fs = outputPath.getFileSystem(job);
    Path file = new Path(outputPath, name);
    CompressionCodec codec = null;
    if (getCompressOutput(job)) {
      Class<?> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
    }
    final RCFile.Writer out = new RCFile.Writer(fs, job, file, progress, codec);

    return new RecordWriter<WritableComparable, BytesRefArrayWritable>() {

      @Override
      public void close(Reporter reporter) throws IOException {
        out.close();
      }

      @Override
      public void write(WritableComparable key, BytesRefArrayWritable value)
          throws IOException {
        out.append(value);
      }
    };
  }

  /**
   * create the final out file.
   *
   * @param jc
   *          the job configuration file
   * @param finalOutPath
   *          the final output file to be created
   * @param valueClass
   *          the value class used for create
   * @param isCompressed
   *          whether the content is compressed or not
   * @param tableProperties
   *          the tableInfo of this file's corresponding table
   * @param progress
   *          progress used for status report
   * @throws IOException
   */
  @Override
  public FSRecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
      boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {

    String[] cols = null;
    String columns = tableProperties.getProperty("columns");
    if (columns == null || columns.trim().equals("")) {
      cols = new String[0];
    } else {
      cols = StringUtils.split(columns, ",");
    }

    RCFileOutputFormat.setColumnNumber(jc, cols.length);
    final RCFile.Writer outWriter = Utilities.createRCFileWriter
      (jc, finalOutPath.getFileSystem(jc),
       finalOutPath, isCompressed);

    return new FSRecordWriter() {
      public void write(Writable r) throws IOException {
        outWriter.append(r);
      }

      public void close(boolean abort) throws IOException {
        outWriter.close();
      }
    };
  }
}
