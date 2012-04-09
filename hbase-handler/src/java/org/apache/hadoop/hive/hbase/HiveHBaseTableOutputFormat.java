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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Progressable;

/**
 * HiveHBaseTableOutputFormat implements HiveOutputFormat for HBase tables.
 * We also need to implement the @deprecated org.apache.hadoop.mapred.OutFormat<?,?>
 * class to keep it compliant with Hive interfaces.
 */
public class HiveHBaseTableOutputFormat extends
    TableOutputFormat<ImmutableBytesWritable> implements
    HiveOutputFormat<ImmutableBytesWritable, Put>,
    OutputFormat<ImmutableBytesWritable, Put> {

  static final Log LOG = LogFactory.getLog(HiveHBaseTableOutputFormat.class);
  public static final String HBASE_WAL_ENABLED = "hive.hbase.wal.enabled";

  /**
   * Update the out table, and output an empty key as the key.
   *
   * @param jc the job configuration file
   * @param finalOutPath the final output table name
   * @param valueClass the value class
   * @param isCompressed whether the content is compressed or not
   * @param tableProperties the table info of the corresponding table
   * @param progress progress used for status report
   * @return the RecordWriter for the output file
   */
  @Override
  public RecordWriter getHiveRecordWriter(
      JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      final Progressable progressable) throws IOException {

    String hbaseTableName = jc.get(HBaseSerDe.HBASE_TABLE_NAME);
    jc.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);
    final boolean walEnabled = HiveConf.getBoolVar(
        jc, HiveConf.ConfVars.HIVE_HBASE_WAL_ENABLED);
    final HTable table = new HTable(HBaseConfiguration.create(jc), hbaseTableName);
    table.setAutoFlush(false);

    return new RecordWriter() {

      @Override
      public void close(boolean abort) throws IOException {
        if (!abort) {
          table.flushCommits();
        }
      }

      @Override
      public void write(Writable w) throws IOException {
        Put put = (Put) w;
        put.setWriteToWAL(walEnabled);
        table.put(put);
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {

    //obtain delegation tokens for the job
    TableMapReduceUtil.initCredentials(jc);

    String hbaseTableName = jc.get(HBaseSerDe.HBASE_TABLE_NAME);
    jc.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);
    Job job = new Job(jc);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);

    try {
      checkOutputSpecs(jobContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public
  org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Put>
  getRecordWriter(
      FileSystem fileSystem,
      JobConf jobConf,
      String name,
      Progressable progressable) throws IOException {

    throw new RuntimeException("Error: Hive should not invoke this method.");
  }
}
