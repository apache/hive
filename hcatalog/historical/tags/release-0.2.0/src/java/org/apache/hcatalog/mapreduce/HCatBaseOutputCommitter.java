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

package org.apache.hcatalog.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hcatalog.common.HCatException;

public abstract class HCatBaseOutputCommitter extends OutputCommitter {

  /** The underlying output committer */
  protected final OutputCommitter baseCommitter;

  public HCatBaseOutputCommitter(JobContext context, OutputCommitter baseCommitter) {
    this.baseCommitter = baseCommitter;
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
      baseCommitter.abortTask(context);
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
      baseCommitter.commitTask(context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
      return baseCommitter.needsTaskCommit(context);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    if( baseCommitter != null ) {
      baseCommitter.setupJob(context);
    }
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
      baseCommitter.setupTask(context);
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException {
    if(baseCommitter != null) {
      baseCommitter.abortJob(jobContext, state);
    }
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext);

    doAbortJob(jobContext, jobInfo);

    Path src = new Path(jobInfo.getLocation());
    FileSystem fs = src.getFileSystem(jobContext.getConfiguration());
    fs.delete(src, true);
  }

  protected void doAbortJob(JobContext jobContext, OutputJobInfo jobInfo) throws HCatException {
  }

  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";

  private static boolean getOutputDirMarking(Configuration conf) {
    return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
                           false);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    if(baseCommitter != null) {
      baseCommitter.commitJob(jobContext);
    }
    // create _SUCCESS FILE if so requested.
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext);
    if(getOutputDirMarking(jobContext.getConfiguration())) {
      Path outputPath = new Path(jobInfo.getLocation());
      if (outputPath != null) {
        FileSystem fileSys = outputPath.getFileSystem(jobContext.getConfiguration());
        // create a file in the folder to mark it
        if (fileSys.exists(outputPath)) {
          Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
          if(!fileSys.exists(filePath)) { // may have been created by baseCommitter.commitJob()
            fileSys.create(filePath).close();
          }
        }
      }
    }
    cleanupJob(jobContext);
  }
}
