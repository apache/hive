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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;
import org.apache.hcatalog.hbase.snapshot.Transaction;

/**
 * "Direct" implementation of OutputFormat for HBase. Uses HTable client's put
 * API to write each row to HBase one a time. Presently it is just using
 * TableOutputFormat as the underlying implementation in the future we can tune
 * this to make the writes faster such as permanently disabling WAL, caching,
 * etc.
 */
class HBaseDirectOutputFormat extends HBaseBaseOutputFormat {

  private TableOutputFormat outputFormat;

  public HBaseDirectOutputFormat() {
    this.outputFormat = new TableOutputFormat();
  }

  @Override
  public RecordWriter<WritableComparable<?>, Object> getRecordWriter(FileSystem ignored,
                                  JobConf job, String name, Progressable progress)
    throws IOException {
    HBaseHCatStorageHandler.setHBaseSerializers(job);
    long version = HBaseRevisionManagerUtil.getOutputRevision(job);
    return new HBaseDirectRecordWriter(outputFormat.getRecordWriter(ignored, job, name,
      progress), version);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
    throws IOException {
    outputFormat.checkOutputSpecs(ignored, job);
    HBaseUtil.addHBaseDelegationToken(job);
  }

  private static class HBaseDirectRecordWriter implements
    RecordWriter<WritableComparable<?>, Object> {

    private RecordWriter<WritableComparable<?>, Object> baseWriter;
    private final Long outputVersion;

    public HBaseDirectRecordWriter(
      RecordWriter<WritableComparable<?>, Object> baseWriter,
      Long outputVersion) {
      this.baseWriter = baseWriter;
      this.outputVersion = outputVersion;
    }

    @Override
    public void write(WritableComparable<?> key, Object value)
      throws IOException {
      Put original = toPut(value);
      Put put = original;
      if (outputVersion != null) {
        put = new Put(original.getRow(), outputVersion.longValue());
        for (List<? extends Cell> row : original.getFamilyMap().values()) {
          for (Cell cell : row) {
            KeyValue el = (KeyValue)cell;
            put.add(el.getFamily(), el.getQualifier(), el.getValue());
          }
        }
      }
      baseWriter.write(key, put);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      baseWriter.close(reporter);
    }

  }

  public static class HBaseDirectOutputCommitter extends OutputCommitter {

    public HBaseDirectOutputCommitter() throws IOException {
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext)
      throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext)
      throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext)
      throws IOException {
    }

    @Override
    public void abortJob(JobContext jobContext, int status)
      throws IOException {
      super.abortJob(jobContext, status);
      RevisionManager rm = null;
      try {
        rm = HBaseRevisionManagerUtil
          .getOpenedRevisionManager(jobContext.getConfiguration());
        Transaction writeTransaction = HBaseRevisionManagerUtil
          .getWriteTransaction(jobContext.getConfiguration());
        rm.abortWriteTransaction(writeTransaction);
      } finally {
        if (rm != null)
          rm.close();
      }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      RevisionManager rm = null;
      try {
        rm = HBaseRevisionManagerUtil
          .getOpenedRevisionManager(jobContext.getConfiguration());
        rm.commitWriteTransaction(HBaseRevisionManagerUtil.getWriteTransaction(jobContext
          .getConfiguration()));
      } finally {
        if (rm != null)
          rm.close();
      }
    }
  }
}
