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

package org.apache.hive.hcatalog.data.transfer.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;
import org.apache.hive.hcatalog.data.transfer.state.StateProvider;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

/**
 * This writer writes via {@link HCatOutputFormat}
 *
 */
public class HCatOutputFormatWriter extends HCatWriter {

  public HCatOutputFormatWriter(WriteEntity we, Map<String, String> config) {
    super(we, config);
  }

  public HCatOutputFormatWriter(Configuration config, StateProvider sp) {
    super(config, sp);
  }

  @Override
  public WriterContext prepareWrite() throws HCatException {
    OutputJobInfo jobInfo = OutputJobInfo.create(we.getDbName(),
      we.getTableName(), we.getPartitionKVs());
    Job job;
    try {
      job = new Job(conf);
      HCatOutputFormat.setOutput(job, jobInfo);
      HCatOutputFormat.setSchema(job, HCatOutputFormat.getTableSchema(job));
      HCatOutputFormat outFormat = new HCatOutputFormat();
      outFormat.checkOutputSpecs(job);
      outFormat.getOutputCommitter(ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(
          job.getConfiguration(), ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID())).setupJob(job);
    } catch (IOException e) {
      throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
    } catch (InterruptedException e) {
      throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
    }
    WriterContext cntxt = new WriterContext();
    cntxt.setConf(job.getConfiguration());
    return cntxt;
  }

  @Override
  public void write(Iterator<HCatRecord> recordItr) throws HCatException {

    int id = sp.getId();
    setVarsInConf(id);
    HCatOutputFormat outFormat = new HCatOutputFormat();
    TaskAttemptContext cntxt = ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(
        conf, new TaskAttemptID(ShimLoader.getHadoopShims().getHCatShim().createTaskID(), id));
    OutputCommitter committer = null;
    RecordWriter<WritableComparable<?>, HCatRecord> writer;
    try {
      committer = outFormat.getOutputCommitter(cntxt);
      committer.setupTask(cntxt);
      writer = outFormat.getRecordWriter(cntxt);
      while (recordItr.hasNext()) {
        HCatRecord rec = recordItr.next();
        writer.write(null, rec);
      }
      writer.close(cntxt);
      if (committer.needsTaskCommit(cntxt)) {
        committer.commitTask(cntxt);
      }
    } catch (IOException e) {
      if (null != committer) {
        try {
          committer.abortTask(cntxt);
        } catch (IOException e1) {
          throw new HCatException(ErrorType.ERROR_INTERNAL_EXCEPTION, e1);
        }
      }
      throw new HCatException("Failed while writing", e);
    } catch (InterruptedException e) {
      if (null != committer) {
        try {
          committer.abortTask(cntxt);
        } catch (IOException e1) {
          throw new HCatException(ErrorType.ERROR_INTERNAL_EXCEPTION, e1);
        }
      }
      throw new HCatException("Failed while writing", e);
    }
  }

  @Override
  public void commit(WriterContext context) throws HCatException {
    try {
      new HCatOutputFormat().getOutputCommitter(ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(
          context.getConf(), ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID()))
        .commitJob(ShimLoader.getHadoopShims().getHCatShim().createJobContext(context.getConf(), null));
    } catch (IOException e) {
      throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
    } catch (InterruptedException e) {
      throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
    }
  }

  @Override
  public void abort(WriterContext context) throws HCatException {
    try {
      new HCatOutputFormat().getOutputCommitter(ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(
        context.getConf(), ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID()))
        .abortJob(ShimLoader.getHadoopShims().getHCatShim().createJobContext(
            context.getConf(), null), State.FAILED);
    } catch (IOException e) {
      throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
    } catch (InterruptedException e) {
      throw new HCatException(ErrorType.ERROR_NOT_INITIALIZED, e);
    }
  }

  private void setVarsInConf(int id) {

    // Following two config keys are required by FileOutputFormat to work
    // correctly.
    // In usual case of Hadoop, JobTracker will set these before launching
    // tasks.
    // Since there is no jobtracker here, we set it ourself.
    conf.setInt("mapred.task.partition", id);
    conf.set("mapred.task.id", "attempt__0000_r_000000_" + id);
  }
}
