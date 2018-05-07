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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Bare bones implementation of OutputFormatContainer. Does only the required
 * tasks to work properly with HCatalog. HCatalog features which require a
 * storage specific implementation are unsupported (ie partitioning).
 */
class DefaultOutputFormatContainer extends OutputFormatContainer {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  public DefaultOutputFormatContainer(org.apache.hadoop.mapred.OutputFormat<WritableComparable<?>, Writable> of) {
    super(of);
  }

  static synchronized String getOutputName(TaskAttemptContext context) {
    return context.getConfiguration().get("mapreduce.output.basename", "part")
        + "-" + NUMBER_FORMAT.format(context.getTaskAttemptID().getTaskID().getId());
  }

  /**
   * Get the record writer for the job. Uses the storagehandler's OutputFormat
   * to get the record writer.
   * @param context the information about the current task.
   * @return a RecordWriter to write the output for the job.
   * @throws IOException
   */
  @Override
  public RecordWriter<WritableComparable<?>, HCatRecord>
  getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    String name = getOutputName(context);
    return new DefaultRecordWriterContainer(context,
      getBaseOutputFormat().getRecordWriter(null, new JobConf(context.getConfiguration()), name, InternalUtil.createReporter(context)));
  }


  /**
   * Get the output committer for this output format. This is responsible
   * for ensuring the output is committed correctly.
   * @param context the task context
   * @return an output committer
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new DefaultOutputCommitterContainer(context, new JobConf(context.getConfiguration()).getOutputCommitter());
  }

  /**
   * Check for validity of the output-specification for the job.
   * @param context information about the job
   * @throws IOException when output should not be attempted
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    org.apache.hadoop.mapred.OutputFormat<? super WritableComparable<?>, ? super Writable> outputFormat = getBaseOutputFormat();
    JobConf jobConf = new JobConf(context.getConfiguration());
    outputFormat.checkOutputSpecs(null, jobConf);
    HCatUtil.copyConf(jobConf, context.getConfiguration());
  }

}
