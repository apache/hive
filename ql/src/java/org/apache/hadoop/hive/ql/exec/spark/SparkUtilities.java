/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.TaskContext;

/**
 * Contains utilities methods used as part of Spark tasks
 */
public class SparkUtilities {
  public static void setTaskInfoInJobConf(JobConf jobConf, TaskContext taskContext) {
    // Set mapred.task.partition in executor side.
    jobConf.setInt("mapred.task.partition", taskContext.getPartitionId());

    // Set mapred.task.id as taskId_attemptId. The taskId is 6 digits in length (prefixed with 0 if
    // necessary). Similarly attemptId is two digits in length.
    jobConf.set("mapred.task.id",
        String.format("%06d_%02d", taskContext.getPartitionId(), taskContext.getAttemptId()));
  }
}
