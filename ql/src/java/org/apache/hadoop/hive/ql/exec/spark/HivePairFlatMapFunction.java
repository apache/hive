/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import java.text.NumberFormat;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;


public abstract class HivePairFlatMapFunction<T, K, V> implements PairFlatMapFunction<T, K, V> {
  private final NumberFormat taskIdFormat = NumberFormat.getInstance();
  private final NumberFormat stageIdFormat = NumberFormat.getInstance();
  {
    taskIdFormat.setGroupingUsed(false);
    taskIdFormat.setMinimumIntegerDigits(6);
    stageIdFormat.setGroupingUsed(false);
    stageIdFormat.setMinimumIntegerDigits(4);
  }

  protected transient JobConf jobConf;
  protected SparkReporter sparkReporter;

  private byte[] buffer;

  public HivePairFlatMapFunction(byte[] buffer, SparkReporter sparkReporter) {
    this.buffer = buffer;
    this.sparkReporter = sparkReporter;
  }

  protected void initJobConf() {
    if (jobConf == null) {
      jobConf = KryoSerializer.deserializeJobConf(this.buffer);
      SmallTableCache.initialize(jobConf);
      setupMRLegacyConfigs();
    }
  }

  protected abstract boolean isMap();

  // Some Hive features depends on several MR configuration legacy, build and add
  // these configuration to JobConf here.
  private void setupMRLegacyConfigs() {
    StringBuilder taskAttemptIdBuilder = new StringBuilder("attempt_");
    taskAttemptIdBuilder.append(System.currentTimeMillis())
      .append("_")
      .append(stageIdFormat.format(TaskContext.get().stageId()))
      .append("_");

    if (isMap()) {
      taskAttemptIdBuilder.append("m_");
    } else {
      taskAttemptIdBuilder.append("r_");
    }

    // Hive requires this TaskAttemptId to be unique. MR's TaskAttemptId is composed
    // of "attempt_timestamp_jobNum_m/r_taskNum_attemptNum". The counterpart for
    // Spark should be "attempt_timestamp_stageNum_m/r_partitionId_attemptNum".
    // When there're multiple attempts for a task, Hive will rely on the partitionId
    // to figure out if the data are duplicate or not when collecting the final outputs
    // (see org.apache.hadoop.hive.ql.exec.Utils.removeTempOrDuplicateFiles)
    taskAttemptIdBuilder.append(taskIdFormat.format(TaskContext.get().partitionId()))
      .append("_").append(TaskContext.get().attemptNumber());

    String taskAttemptIdStr = taskAttemptIdBuilder.toString();
    jobConf.set("mapred.task.id", taskAttemptIdStr);
    jobConf.set("mapreduce.task.attempt.id", taskAttemptIdStr);
    jobConf.setInt("mapred.task.partition", TaskContext.get().partitionId());
  }
}
