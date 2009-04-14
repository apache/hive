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
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * HiveIgnoreKeyTextOutputFormat replaces key with null before feeding the <key,
 * value> to TextOutputFormat.RecordWriter.
 * 
 */
public class HiveIgnoreKeyTextOutputFormat<K extends WritableComparable, V extends Writable>
    extends TextOutputFormat<K, V> implements HiveOutputFormat<K, V> {

  /**
   * create the final out file, and output row by row. After one row is
   * appended, a configured row separator is appended
   * 
   * @param jc
   *          the job configuration file
   * @param outPath
   *          the final output file to be created
   * @param valueClass
   *          the value class used for create
   * @param isCompressed
   *          whether the content is compressed or not
   * @param tableProperties
   *          the tableProperties of this file's corresponding table
   * @param progress
   *          progress used for status report
   * @return the RecordWriter
   */
  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path outPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    int rowSeparator = 0;
    String rowSeparatorString = tableProperties.getProperty(
        Constants.LINE_DELIM, "\n");
    try {
      rowSeparator = Byte.parseByte(rowSeparatorString);
    } catch (NumberFormatException e) {
      rowSeparator = rowSeparatorString.charAt(0);
    }

    final int finalRowSeparator = rowSeparator;
    final OutputStream outStream = Utilities.createCompressedStream(jc,
        FileSystem.get(jc).create(outPath), isCompressed);
    return new RecordWriter() {
      public void write(Writable r) throws IOException {
        if (r instanceof Text) {
          Text tr = (Text) r;
          outStream.write(tr.getBytes(), 0, tr.getLength());
          outStream.write(finalRowSeparator);
        } else {
          // DynamicSerDe always writes out BytesWritable
          BytesWritable bw = (BytesWritable) r;
          outStream.write(bw.get(), 0, bw.getSize());
          outStream.write(finalRowSeparator);
        }
      }

      public void close(boolean abort) throws IOException {
        outStream.close();
      }
    };
  }

  protected static class IgnoreKeyWriter<K extends WritableComparable, V extends Writable>
      implements org.apache.hadoop.mapred.RecordWriter<K, V> {

    private org.apache.hadoop.mapred.RecordWriter<K, V> mWriter;

    public IgnoreKeyWriter(org.apache.hadoop.mapred.RecordWriter<K, V> writer) {
      this.mWriter = writer;
    }

    public synchronized void write(K key, V value) throws IOException {
      this.mWriter.write(null, value);
    }

    public void close(Reporter reporter) throws IOException {
      this.mWriter.close(reporter);
    }
  }

  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {

    return new IgnoreKeyWriter<K, V>(super.getRecordWriter(ignored, job, name,
        progress));
  }

}
