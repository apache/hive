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

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HiveHBaseTableOutputFormat implements HiveOutputFormat for HBase tables.
 */
public class HiveHBaseTableOutputFormat extends
    TableOutputFormat<ImmutableBytesWritable> implements
    HiveOutputFormat<ImmutableBytesWritable, Object> {

  static final Logger LOG = LoggerFactory.getLogger(HiveHBaseTableOutputFormat.class);

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {

    //obtain delegation tokens for the job
    if (UserGroupInformation.getCurrentUser().hasKerberosCredentials()) {
      TableMapReduceUtil.initCredentials(jc);
    }

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
  org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Object>
  getRecordWriter(
      FileSystem fileSystem,
      JobConf jobConf,
      String name,
      Progressable progressable) throws IOException {
    return getMyRecordWriter(jobConf);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException,
  InterruptedException {
    return new TableOutputCommitter();
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jobConf, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    return getMyRecordWriter(jobConf);
  }

  private MyRecordWriter getMyRecordWriter(JobConf jobConf) throws IOException {
    String hbaseTableName = jobConf.get(HBaseSerDe.HBASE_TABLE_NAME);
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);
    final boolean walEnabled = HiveConf.getBoolVar(
        jobConf, HiveConf.ConfVars.HIVE_HBASE_WAL_ENABLED);
    final Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create(jobConf));
    final BufferedMutator table = conn.getBufferedMutator(TableName.valueOf(hbaseTableName));
    return new MyRecordWriter(table, conn, walEnabled);
  }

  private static class MyRecordWriter
      implements org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Object>,
      org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {
    private final BufferedMutator m_table;
    private final boolean m_walEnabled;
    private final Connection m_connection;

    public MyRecordWriter(BufferedMutator table, Connection connection, boolean walEnabled) {
      m_table = table;
      m_walEnabled = walEnabled;
      m_connection = connection;
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      m_table.close();
    }

    @Override
    public void write(ImmutableBytesWritable key,
        Object value) throws IOException {
      Put put;
      if (value instanceof Put){
        put = (Put)value;
      } else if (value instanceof PutWritable) {
        put = new Put(((PutWritable)value).getPut());
      } else {
        throw new IllegalArgumentException("Illegal Argument " + (value == null ? "null" : value.getClass().getName()));
      }
      if(m_walEnabled) {
        put.setDurability(Durability.SYNC_WAL);
      } else {
        put.setDurability(Durability.SKIP_WAL);
      }
      m_table.mutate(put);
    }

    @Override
    protected void finalize() throws Throwable {
      try {
        m_table.close();
        m_connection.close();
      } finally {
        super.finalize();
      }
    }

    @Override
    public void write(Writable w) throws IOException {
      write(null, w);
    }

    @Override
    public void close(boolean abort) throws IOException {
      close(null);
    }
  }
}
