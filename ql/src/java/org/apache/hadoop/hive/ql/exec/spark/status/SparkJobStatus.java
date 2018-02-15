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
package org.apache.hadoop.hive.ql.exec.spark.status;

import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.JobExecutionStatus;

import java.util.Map;

/**
 * SparkJobStatus identify what Hive want to know about the status of a Spark job.
 */
public interface SparkJobStatus {

  String getAppID();

  int getJobId();

  JobExecutionStatus getState() throws HiveException;

  int[] getStageIds() throws HiveException;

  Map<String, SparkStageProgress> getSparkStageProgress() throws HiveException;

  SparkCounters getCounter();

  SparkStatistics getSparkStatistics();

  String getWebUIURL();

  void cleanup();

  Throwable getError();

  void setError(Throwable e);
}
