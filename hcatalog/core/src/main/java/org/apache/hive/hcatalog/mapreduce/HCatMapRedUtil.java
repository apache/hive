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

import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;

public class HCatMapRedUtil {

  public static TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapreduce.TaskAttemptContext context) {
    return  createTaskAttemptContext(new JobConf(context.getConfiguration()),
                              org.apache.hadoop.mapred.TaskAttemptID.forName(context.getTaskAttemptID().toString()),
                               Reporter.NULL);
  }

  public static org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf, org.apache.hadoop.mapreduce.TaskAttemptID id) {
    return ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(conf,id);
  }

  public static TaskAttemptContext createTaskAttemptContext(JobConf conf, TaskAttemptID id, Progressable progressable) {
    return ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptContext(conf, id, (Reporter) progressable);
  }
  public static org.apache.hadoop.mapreduce.TaskAttemptID createTaskAttemptID(JobID jobId, boolean isMap, int taskId, int id) {
    return ShimLoader.getHadoopShims().newTaskAttemptID(jobId, isMap, taskId, id);
  }
  public static org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapreduce.JobContext context) {
    return createJobContext((JobConf)context.getConfiguration(),
                context.getJobID(),
                Reporter.NULL);
  }

  public static JobContext createJobContext(JobConf conf, org.apache.hadoop.mapreduce.JobID id, Progressable progressable) {
    return ShimLoader.getHadoopShims().getHCatShim().createJobContext(conf, id, (Reporter) progressable);
  }
}
