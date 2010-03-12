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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

/**
 * HiveHBaseTableOutputFormat implements TableOutputFormat for HBase tables.
 */
public class HiveHBaseTableOutputFormat extends 
    TableOutputFormat implements
    HiveOutputFormat<ImmutableBytesWritable, BatchUpdate> {
  
  private final ImmutableBytesWritable key = new ImmutableBytesWritable();

  /**
   * Update to the final out table, and output an empty key as the key.
   * 
   * @param jc
   *          the job configuration file
   * @param finalOutPath
   *          the final output table name
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
    String hbaseTableName = jc.get(HBaseSerDe.HBASE_TABLE_NAME);
    jc.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);

    final org.apache.hadoop.mapred.RecordWriter<
      ImmutableBytesWritable, BatchUpdate> tblWriter =
      this.getRecordWriter(null, jc, null, progress);

    return new RecordWriter() {
      
      @Override
      public void close(boolean abort) throws IOException {
        tblWriter.close(null);
      }

      @Override
      public void write(Writable w) throws IOException {
        BatchUpdate bu = (BatchUpdate) w;
        key.set(bu.getRow());
        tblWriter.write(key, bu);
      }
    };
  }

}
