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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;

/**
 * MapRedStats.
 *
 * A data structure to keep one mapreduce's stats:
 * number of mappers, number of reducers, accumulative CPU time and whether it
 * succeeds.
 *
 */
public class MapRedStats {
  int numMap;
  int numReduce;
  long cpuMSec;
  Counters counters = null;
  boolean success;

  String jobId;

  public MapRedStats(int numMap, int numReduce, long cpuMSec, boolean ifSuccess, String jobId) {
    this.numMap = numMap;
    this.numReduce = numReduce;
    this.cpuMSec = cpuMSec;
    this.success = ifSuccess;
    this.jobId = jobId;
  }

  public boolean isSuccess() {
    return success;
  }

  public long getCpuMSec() {
    return cpuMSec;
  }

  public int getNumMap() {
    return numMap;
  }

  public void setNumMap(int numMap) {
    this.numMap = numMap;
  }

  public int getNumReduce() {
    return numReduce;
  }

  public void setNumReduce(int numReduce) {
    this.numReduce = numReduce;
  }

  public void setCounters(Counters taskCounters) {
    this.counters = taskCounters;
  }

  public Counters getCounters() {
    return this.counters;
  }

  public void setCpuMSec(long cpuMSec) {
    this.cpuMSec = cpuMSec;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getTaskNumbers() {
    StringBuilder sb = new StringBuilder();
    if (numMap > 0) {
      sb.append("Map: " + numMap + "  ");
    }
    if (numReduce > 0) {
      sb.append("Reduce: " + numReduce + "  ");
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (numMap > 0) {
      sb.append("Map: " + numMap + "  ");
    }

    if (numReduce > 0) {
      sb.append("Reduce: " + numReduce + "  ");
    }

    if (cpuMSec > 0) {
      sb.append(" Cumulative CPU: " + (cpuMSec / 1000D) + " sec  ");
    }

    if (counters != null) {
      Counter hdfsReadCntr = counters.findCounter("FileSystemCounters",
          "HDFS_BYTES_READ");
      long hdfsRead;
      if (hdfsReadCntr != null && (hdfsRead = hdfsReadCntr.getValue()) >= 0) {
        sb.append(" HDFS Read: " + hdfsRead);
      }

      Counter hdfsWrittenCntr = counters.findCounter("FileSystemCounters",
          "HDFS_BYTES_WRITTEN");
      long hdfsWritten;
      if (hdfsWrittenCntr != null && (hdfsWritten = hdfsWrittenCntr.getValue()) >= 0) {
        sb.append(" HDFS Write: " + hdfsWritten);
      }
    }

    sb.append(" " + (success ? "SUCCESS" : "FAIL"));

    return sb.toString();
  }
}
