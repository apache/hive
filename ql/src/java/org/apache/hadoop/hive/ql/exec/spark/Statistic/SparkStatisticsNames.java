/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.Statistic;

/**
 * A collection of names that define different {@link SparkStatistic} objects.
 */
public class SparkStatisticsNames {

  public static final String EXECUTOR_DESERIALIZE_TIME = "ExecutorDeserializeTime";
  public static final String EXECUTOR_DESERIALIZE_CPU_TIME = "ExecutorDeserializeCpuTime";
  public static final String EXECUTOR_RUN_TIME = "ExecutorRunTime";
  public static final String EXECUTOR_CPU_TIME = "ExecutorCpuTime";
  public static final String RESULT_SIZE = "ResultSize";
  public static final String JVM_GC_TIME = "JvmGCTime";
  public static final String RESULT_SERIALIZATION_TIME = "ResultSerializationTime";
  public static final String MEMORY_BYTES_SPILLED = "MemoryBytesSpilled";
  public static final String DISK_BYTES_SPILLED = "DiskBytesSpilled";
  public static final String BYTES_READ = "BytesRead";
  public static final String REMOTE_BLOCKS_FETCHED = "RemoteBlocksFetched";
  public static final String LOCAL_BLOCKS_FETCHED = "LocalBlocksFetched";
  public static final String TOTAL_BLOCKS_FETCHED = "TotalBlocksFetched";
  public static final String FETCH_WAIT_TIME = "FetchWaitTime";
  public static final String REMOTE_BYTES_READ = "RemoteBytesRead";
  public static final String SHUFFLE_BYTES_WRITTEN = "ShuffleBytesWritten";
  public static final String SHUFFLE_WRITE_TIME = "ShuffleWriteTime";
  public static final String TASK_DURATION_TIME = "TaskDurationTime";

  public static final String SPARK_GROUP_NAME = "SPARK";
}
