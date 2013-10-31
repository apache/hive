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

package org.apache.hadoop.hive.ql.io.udf;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.FSRecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class Rot13OutputFormat
  extends HiveIgnoreKeyTextOutputFormat<LongWritable,Text> {

  @Override
  public FSRecordWriter
    getHiveRecordWriter(JobConf jc,
                        Path outPath,
                        Class<? extends Writable> valueClass,
                        boolean isCompressed,
                        Properties tableProperties,
                        Progressable progress) throws IOException {
    final FSRecordWriter result =
      super.getHiveRecordWriter(jc,outPath,valueClass,isCompressed,
        tableProperties,progress);
    final Reporter reporter = (Reporter) progress;
    reporter.setStatus("got here");
    System.out.println("Got a reporter " + reporter);
    return new FSRecordWriter() {
      @Override
      public void write(Writable w) throws IOException {
        if (w instanceof Text) {
          Text value = (Text) w;
          Rot13InputFormat.rot13(value.getBytes(), 0, value.getLength());
          result.write(w);
        } else if (w instanceof BytesWritable) {
          BytesWritable value = (BytesWritable) w;
          Rot13InputFormat.rot13(value.getBytes(), 0, value.getLength());
          result.write(w);
        } else {
          throw new IllegalArgumentException("need text or bytes writable " +
            " instead of " + w.getClass().getName());
        }
      }

      @Override
      public void close(boolean abort) throws IOException {
        result.close(abort);
      }
    };
  }
}
