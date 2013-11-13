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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hcatalog.hbase.snapshot.RevisionManager;

/**
 * Class which imports data into HBase via it's "bulk load" feature. Wherein
 * regions are created by the MR job using HFileOutputFormat and then later
 * "moved" into the appropriate region server.
 */
class HBaseBulkOutputFormat extends HBaseBaseOutputFormat {

  private final static ImmutableBytesWritable EMPTY_LIST = new ImmutableBytesWritable(
    new byte[0]);
  private SequenceFileOutputFormat<WritableComparable<?>, Object> baseOutputFormat;

  public HBaseBulkOutputFormat() {
    baseOutputFormat = new SequenceFileOutputFormat<WritableComparable<?>, Object>();
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
    throws IOException {
    baseOutputFormat.checkOutputSpecs(ignored, job);
    HBaseUtil.addHBaseDelegationToken(job);
    addJTDelegationToken(job);
  }

  @Override
  public RecordWriter<WritableComparable<?>, Object> getRecordWriter(
    FileSystem ignored, JobConf job, String name, Progressable progress)
    throws IOException {
    HBaseHCatStorageHandler.setHBaseSerializers(job);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);
    long version = HBaseRevisionManagerUtil.getOutputRevision(job);
    return new HBaseBulkRecordWriter(baseOutputFormat.getRecordWriter(
      ignored, job, name, progress), version);
  }

  private void addJTDelegationToken(JobConf job) throws IOException {
    // Get jobTracker delegation token if security is enabled
    // we need to launch the ImportSequenceFile job
    if (User.isSecurityEnabled()) {
      JobClient jobClient = new JobClient(new JobConf(job));
      try {
        job.getCredentials().addToken(new Text("my mr token"),
          jobClient.getDelegationToken(null));
      } catch (InterruptedException e) {
        throw new IOException("Error while getting JT delegation token", e);
      }
    }
  }

  private static class HBaseBulkRecordWriter implements
    RecordWriter<WritableComparable<?>, Object> {

    private RecordWriter<WritableComparable<?>, Object> baseWriter;
    private final Long outputVersion;

    public HBaseBulkRecordWriter(
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
      // we ignore the key
      baseWriter.write(EMPTY_LIST, put);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      baseWriter.close(reporter);
    }
  }

  public static class HBaseBulkOutputCommitter extends OutputCommitter {

    private final OutputCommitter baseOutputCommitter;

    public HBaseBulkOutputCommitter() {
      baseOutputCommitter = new FileOutputCommitter();
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext)
      throws IOException {
      baseOutputCommitter.abortTask(taskContext);
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext)
      throws IOException {
      // baseOutputCommitter.commitTask(taskContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
      return baseOutputCommitter.needsTaskCommit(taskContext);
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      baseOutputCommitter.setupJob(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext)
      throws IOException {
      baseOutputCommitter.setupTask(taskContext);
    }

    @Override
    public void abortJob(JobContext jobContext, int status)
      throws IOException {
      baseOutputCommitter.abortJob(jobContext, status);
      RevisionManager rm = null;
      try {
        rm = HBaseRevisionManagerUtil
          .getOpenedRevisionManager(jobContext.getConfiguration());
        rm.abortWriteTransaction(HBaseRevisionManagerUtil
          .getWriteTransaction(jobContext.getConfiguration()));
      } finally {
        cleanIntermediate(jobContext);
        if (rm != null)
          rm.close();
      }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      baseOutputCommitter.commitJob(jobContext);
      RevisionManager rm = null;
      try {
        Configuration conf = jobContext.getConfiguration();
        Path srcPath = FileOutputFormat.getOutputPath(jobContext.getJobConf());
        if (!FileSystem.get(conf).exists(srcPath)) {
          throw new IOException("Failed to bulk import hfiles. " +
            "Intermediate data directory is cleaned up or missing. " +
            "Please look at the bulk import job if it exists for failure reason");
        }
        Path destPath = new Path(srcPath.getParent(), srcPath.getName() + "_hfiles");
        boolean success = ImportSequenceFile.runJob(jobContext,
          conf.get(HBaseConstants.PROPERTY_OUTPUT_TABLE_NAME_KEY),
          srcPath,
          destPath);
        if (!success) {
          cleanIntermediate(jobContext);
          throw new IOException("Failed to bulk import hfiles." +
            " Please look at the bulk import job for failure reason");
        }
        rm = HBaseRevisionManagerUtil.getOpenedRevisionManager(conf);
        rm.commitWriteTransaction(HBaseRevisionManagerUtil.getWriteTransaction(conf));
        cleanIntermediate(jobContext);
      } finally {
        if (rm != null)
          rm.close();
      }
    }

    private void cleanIntermediate(JobContext jobContext)
      throws IOException {
      FileSystem fs = FileSystem.get(jobContext.getConfiguration());
      fs.delete(FileOutputFormat.getOutputPath(jobContext.getJobConf()), true);
    }
  }
}
