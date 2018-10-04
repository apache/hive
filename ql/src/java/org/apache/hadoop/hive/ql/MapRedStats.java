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

package org.apache.hadoop.hive.ql;

import java.io.IOException;

import org.apache.hadoop.hive.ql.processors.ErasureProcessor;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapRedStats.
 *
 * A data structure to keep one mapreduce's stats:
 * number of mappers, number of reducers, accumulative CPU time and whether it
 * succeeds.
 *
 */
public class MapRedStats {
  private static final String CLASS_NAME = MapRedStats.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private JobConf jobConf;
  private int numMap;
  private int numReduce;
  private long cpuMSec;
  private Counters counters = null;
  private boolean success;

  private String jobId;

  private long numModifiedRows;

  public MapRedStats(JobConf jobConf, int numMap, int numReduce, long cpuMSec, boolean ifSuccess, String jobId) {
    this.jobConf = jobConf;
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

  public long getNumModifiedRows() {
    return numModifiedRows;
  }

  public void setNumModifiedRows(long numModifiedRows) {
    this.numModifiedRows = numModifiedRows;
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

      HadoopShims.HdfsErasureCodingShim erasureShim = getHdfsErasureCodingShim();

      if (erasureShim != null && erasureShim.isMapReduceStatAvailable()) {
        // Erasure Coding stats - added in HADOOP-15507, expected in Hadoop 3.2.0
        Counter hdfsReadEcCntr = counters.findCounter("FileSystemCounters",
            "HDFS_BYTES_READ_EC"); // FileSystemCounter.BYTES_READ_EC
        if (hdfsReadEcCntr != null) {
          long hdfsReadEc = hdfsReadEcCntr.getValue();
          if (hdfsReadEc >= 0) {
            sb.append(" HDFS EC Read: " + hdfsReadEc);
          }
        }
      }
    }

    sb.append(" " + (success ? "SUCCESS" : "FAIL"));

    return sb.toString();
  }

  /**
   * Get the Erasure Coding Shim.
   * @return a HdfsErasureCodingShim
   */
  private HadoopShims.HdfsErasureCodingShim getHdfsErasureCodingShim() {
    HadoopShims.HdfsErasureCodingShim erasureShim = null;
    try {
      erasureShim = ErasureProcessor.getErasureShim(jobConf);
    } catch (IOException e) {
      // this should not happen
      LOG.warn("Could not get Erasure Coding shim for reason: " + e.getMessage());
      // fall through
    }
    return erasureShim;
  }
}
