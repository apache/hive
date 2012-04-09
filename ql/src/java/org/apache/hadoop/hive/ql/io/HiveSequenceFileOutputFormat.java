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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

/** A {@link HiveOutputFormat} that writes {@link SequenceFile}s. */
public class HiveSequenceFileOutputFormat<K,V> extends SequenceFileOutputFormat<K,V>
    implements HiveOutputFormat<K, V> {

  BytesWritable EMPTY_KEY = new BytesWritable();

  /**
   * create the final out file, and output an empty key as the key.
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
   * @return the RecordWriter for the output file
   */
  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {

    FileSystem fs = finalOutPath.getFileSystem(jc);
    final SequenceFile.Writer outStream = Utilities.createSequenceWriter(jc,
        fs, finalOutPath, BytesWritable.class, valueClass, isCompressed);

    return new RecordWriter() {
      public void write(Writable r) throws IOException {
        outStream.append(EMPTY_KEY, r);
      }

      public void close(boolean abort) throws IOException {
        outStream.close();
      }
    };
  }

}
