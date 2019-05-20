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

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Utility class for update Spark-level metrics.
 */
public final class SparkMetricUtils {

  private SparkMetricUtils() {
    // Do nothing
  }

  public static void updateSparkRecordsWrittenMetrics(long numRows) {
    TaskContext taskContext = TaskContext.get();
    if (taskContext != null && numRows > 0) {
      taskContext.taskMetrics().outputMetrics().setRecordsWritten(numRows);
    }
  }

  public static void updateSparkBytesWrittenMetrics(Logger log, FileSystem fs, Path[]
          commitPaths) {
    AtomicLong bytesWritten = new AtomicLong();
    Arrays.stream(commitPaths).parallel().forEach(path -> {
      try {
        bytesWritten.addAndGet(fs.getFileStatus(path).getLen());
      } catch (IOException e) {
        log.debug("Unable to collect stats for file: " + path + " output metrics may be inaccurate",
                e);
      }
    });
    if (bytesWritten.get() > 0) {
      TaskContext.get().taskMetrics().outputMetrics().setBytesWritten(bytesWritten.get());
    }
  }
}
