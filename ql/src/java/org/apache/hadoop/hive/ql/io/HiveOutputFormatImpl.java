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
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Hive does not use OutputFormat's in a conventional way, but constructs and uses
 * the defined OutputFormat for each table from FileSinkOperator. HiveOutputFormatImpl is
 * used for basic setup, especially for calling checkOutputSpecs().
 */
public class HiveOutputFormatImpl<K extends WritableComparable<K>, V extends Writable>
  implements OutputFormat<K, V> {

  //no records will be emited from Hive
  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) {
    return new RecordWriter<K, V>() {
      public void write(K key, V value) {
        throw new RuntimeException("Should not be called");
      }

      public void close(Reporter reporter) {
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    MapredWork work = Utilities.getMapRedWork(job);

    List<Operator<?>> opList = work.getAllOperators();

    for (Operator<?> op : opList) {
      if (op instanceof FileSinkOperator) {
        ((FileSinkOperator) op).checkOutputSpecs(ignored, job);
      }
    }
  }
}
