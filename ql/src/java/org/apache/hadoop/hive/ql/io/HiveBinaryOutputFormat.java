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
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * HiveBinaryOutputFormat writes out the values consecutively without any
 * separators.  It can be used to create a binary data file.
 */
public class HiveBinaryOutputFormat<K extends WritableComparable, V extends Writable>
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
   *          ignored. Currently we don't support compression.
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

    FileSystem fs = outPath.getFileSystem(jc);
    final OutputStream outStream = fs.create(outPath);

    return new RecordWriter() {
      public void write(Writable r) throws IOException {
        if (r instanceof Text) {
          Text tr = (Text) r;
          outStream.write(tr.getBytes(), 0, tr.getLength());
        } else {
          // DynamicSerDe always writes out BytesWritable
          BytesWritable bw = (BytesWritable) r;
          outStream.write(bw.get(), 0, bw.getSize());
        }
      }

      public void close(boolean abort) throws IOException {
        outStream.close();
      }
    };
  }

}
