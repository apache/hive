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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 * A {@link HiveOutputFormat} that writes {@link SequenceFile}s with the
 * content saved in the keys, and null in the values.
 */
public class HiveNullValueSequenceFileOutputFormat<K,V>
  extends SequenceFileOutputFormat<K,V>
  implements HiveOutputFormat<K,V> {

  private static final Writable NULL_WRITABLE = NullWritable.get();

  private HiveKey keyWritable;
  private boolean keyIsText;

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {

    FileSystem fs = finalOutPath.getFileSystem(jc);
    final SequenceFile.Writer outStream = Utilities.createSequenceWriter(jc, fs, finalOutPath,
    HiveKey.class, NullWritable.class, isCompressed, progress);

    keyWritable = new HiveKey();
    keyIsText = valueClass.equals(Text.class);
    return new RecordWriter() {
      @Override
      public void write(Writable r) throws IOException {
        if (keyIsText) {
          Text text = (Text) r;
          keyWritable.set(text.getBytes(), 0, text.getLength());
        } else {
          BytesWritable bw = (BytesWritable) r;
          // Once we drop support for old Hadoop versions, change these
          // to getBytes() and getLength() to fix the deprecation warnings.
          // Not worth a shim.
          keyWritable.set(bw.get(), 0, bw.getSize());
        }
        keyWritable.setHashCode(r.hashCode());
        outStream.append(keyWritable, NULL_WRITABLE);
      }

      @Override
      public void close(boolean abort) throws IOException {
        outStream.close();
      }
    };
  }

}
