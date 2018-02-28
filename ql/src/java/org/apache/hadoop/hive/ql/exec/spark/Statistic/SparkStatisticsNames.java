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

  public static final String TASK_DURATION_TIME = "TaskDurationTime";

  // Input Metrics
  public static final String BYTES_READ = "BytesRead";
  public static final String RECORDS_READ = "RecordsRead";

  // Shuffle Read Metrics
  public static final String SHUFFLE_FETCH_WAIT_TIME = "ShuffleFetchWaitTime";
  public static final String SHUFFLE_REMOTE_BYTES_READ = "ShuffleRemoteBytesRead";
  public static final String SHUFFLE_LOCAL_BYTES_READ = "ShuffleLocalBytesRead";
  public static final String SHUFFLE_TOTAL_BYTES_READ = "ShuffleTotalBytesRead";
  public static final String SHUFFLE_REMOTE_BLOCKS_FETCHED = "ShuffleRemoteBlocksFetched";
  public static final String SHUFFLE_LOCAL_BLOCKS_FETCHED = "ShuffleLocalBlocksFetched";
  public static final String SHUFFLE_TOTAL_BLOCKS_FETCHED = "ShuffleTotalBlocksFetched";
  public static final String SHUFFLE_REMOTE_BYTES_READ_TO_DISK = "ShuffleRemoteBytesReadToDisk";
  public static final String SHUFFLE_RECORDS_READ = "ShuffleRecordsRead";

  // Shuffle Write Metrics
  public static final String SHUFFLE_BYTES_WRITTEN = "ShuffleBytesWritten";
  public static final String SHUFFLE_WRITE_TIME = "ShuffleWriteTime";
  public static final String SHUFFLE_RECORDS_WRITTEN = "ShuffleRecordsWritten";

  public static final String RECORDS_WRITTEN = "RecordsWritten";
  public static final String BYTES_WRITTEN = "BytesWritten";

  public static final String SPARK_GROUP_NAME = "SPARK";
}
