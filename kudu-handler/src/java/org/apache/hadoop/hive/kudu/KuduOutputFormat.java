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
package org.apache.hadoop.hive.kudu;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import static org.apache.hadoop.hive.kudu.KuduHiveUtils.createOverlayedConf;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;

/**
 * A Kudu OutputFormatKuduPredicateHandler implementation for use by Hive.
 */
public class KuduOutputFormat extends OutputFormat<NullWritable, KuduWritable>
    implements HiveOutputFormat<NullWritable, KuduWritable> {

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                                           Class valueClass, boolean isCompressed,
                                                           Properties tableProperties,
                                                           Progressable progress)
      throws IOException {
    return new KuduRecordWriter(createOverlayedConf(jc, tableProperties));
  }


  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, KuduWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    return new KuduRecordWriter(job);
  }

  @Override
  public RecordWriter<NullWritable, KuduWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    return new KuduRecordWriter(context.getConfiguration());
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  @Override
  public void checkOutputSpecs(JobContext context) {
    // Not doing any check.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new KuduOuputComitter();
  }

  static class KuduRecordWriter extends RecordWriter<NullWritable, KuduWritable>
      implements FileSinkOperator.RecordWriter,
      org.apache.hadoop.mapred.RecordWriter<NullWritable, KuduWritable> {
    private KuduClient client;
    private KuduTable table;
    private KuduSession session;

    KuduRecordWriter(Configuration conf) throws IOException {
      this.client = KuduHiveUtils.getKuduClient(conf);

      String tableName = conf.get(KUDU_TABLE_NAME_KEY);
      if (StringUtils.isEmpty(tableName)) {
        throw new IllegalArgumentException(KUDU_TABLE_NAME_KEY + " is not set.");
      }
      if (!client.tableExists(tableName)) {
        throw new IllegalArgumentException("Kudu table does not exist: " + tableName);
      }

      this.table = client.openTable(tableName);
      this.session = client.newSession();
      this.session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Override
    public void write(Writable row) throws IOException {
      Preconditions.checkArgument(row instanceof KuduWritable);
      Operation op = table.newUpsert();
      ((KuduWritable) row).populateRow(op.getRow());
      session.apply(op);
    }

    @Override
    public void write(NullWritable key, KuduWritable value) throws IOException {
      write(value);
    }

    @Override
    public void close(boolean abort) throws IOException {
      session.close();
      processErrors();
      client.close();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
      close(false);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      close(false);
    }

    private void processErrors() throws IOException {
      RowErrorsAndOverflowStatus pendingErrors = session.getPendingErrors();
      if (pendingErrors.getRowErrors().length != 0) {
        RowError[] errors = pendingErrors.getRowErrors();
        // Build a sample of error strings.
        int sampleSize = 5;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < errors.length; i++) {
          if (i == sampleSize) {
            break;
          }
          sb.append(errors[i].getErrorStatus().toString());
        }
        if (pendingErrors.isOverflowed()) {
          throw new IOException(
              "PendingErrors overflowed. Failed to write at least " + errors.length + " rows " +
                  "to Kudu; Sample errors: " + sb.toString());
        } else {
          throw new IOException(
              "Failed to write " + errors.length + " rows to Kudu; Sample errors: " +
                  sb.toString());
        }
      }
    }
  }

  /**
   * A dummy committer class that does not do anything.
   */
  static class KuduOuputComitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
      // do nothing.
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) {
      // do nothing.
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) {
      // do nothing.
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) {
      // do nothing.
    }
  }
}
