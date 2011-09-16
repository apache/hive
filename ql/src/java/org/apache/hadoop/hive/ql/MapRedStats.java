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
  long hdfsRead = -1;
  long hdfsWrite = -1;
  long mapInputRecords = -1;
  long mapOutputRecords = -1;
  long reduceInputRecords = -1;
  long reduceOutputRecords = -1;
  long reduceShuffleBytes = -1;
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

  public long getHdfsRead() {
    return hdfsRead;
  }

  public void setHdfsRead(long hdfsRead) {
    this.hdfsRead = hdfsRead;
  }

  public long getHdfsWrite() {
    return hdfsWrite;
  }

  public void setHdfsWrite(long hdfsWrite) {
    this.hdfsWrite = hdfsWrite;
  }

  public long getMapInputRecords() {
    return mapInputRecords;
  }

  public void setMapInputRecords(long mapInputRecords) {
    this.mapInputRecords = mapInputRecords;
  }

  public long getMapOutputRecords() {
    return mapOutputRecords;
  }

  public void setMapOutputRecords(long mapOutputRecords) {
    this.mapOutputRecords = mapOutputRecords;
  }

  public long getReduceInputRecords() {
    return reduceInputRecords;
  }

  public void setReduceInputRecords(long reduceInputRecords) {
    this.reduceInputRecords = reduceInputRecords;
  }

  public long getReduceOutputRecords() {
    return reduceOutputRecords;
  }

  public void setReduceOutputRecords(long reduceOutputRecords) {
    this.reduceOutputRecords = reduceOutputRecords;
  }

  public long getReduceShuffleBytes() {
    return reduceShuffleBytes;
  }

  public void setReduceShuffleBytes(long reduceShuffleBytes) {
    this.reduceShuffleBytes = reduceShuffleBytes;
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
      sb.append(" Accumulative CPU: " + (cpuMSec / 1000D) + " sec  ");
    }

    if (hdfsRead >= 0) {
      sb.append(" HDFS Read: " + hdfsRead);
    }

    if (hdfsWrite >= 0) {
      sb.append(" HDFS Write: " + hdfsWrite);
    }

    sb.append(" " + (success ? "SUCESS" : "FAIL"));

    return sb.toString();
  }
}
