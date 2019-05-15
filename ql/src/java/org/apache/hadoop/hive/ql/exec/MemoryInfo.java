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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

/**
 * Contains information about executor memory, various memory thresholds used for join conversions etc. based on
 * execution engine.
 **/

public class MemoryInfo {

  private Configuration conf;
  private boolean isTez;
  private boolean isLlap;
  private long maxExecutorMemory;
  private long mapJoinMemoryThreshold;
  private long dynPartJoinMemoryThreshold;

  public MemoryInfo(Configuration conf) {
    this.isTez = "tez".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
    this.isLlap = "llap".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE));
    if (isLlap) {
      LlapClusterStateForCompile llapInfo = LlapClusterStateForCompile.getClusterInfo(conf);
      llapInfo.initClusterInfo();
      if (llapInfo.hasClusterInfo()) {
        this.maxExecutorMemory = llapInfo.getMemoryPerExecutor();
      } else {
        long memPerInstance =
            HiveConf.getLongVar(conf, HiveConf.ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB) * 1024L * 1024L;
        long numExecutors = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
        this.maxExecutorMemory = memPerInstance / numExecutors;
      }
    } else {
      if (isTez) {
        float heapFraction = HiveConf.getFloatVar(conf, HiveConf.ConfVars.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION);
        int containerSizeMb = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) > 0 ?
            HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE) :
            conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);
        // this can happen when config is explicitly set to "-1", in which case defaultValue also does not work
        if (containerSizeMb < 0) {
          containerSizeMb =  MRJobConfig.DEFAULT_MAP_MEMORY_MB;
        }
        this.maxExecutorMemory = (long) ((containerSizeMb * 1024L * 1024L) * heapFraction);
      } else {
        this.maxExecutorMemory =
            conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB) * 1024L * 1024L;
        // this can happen when config is explicitly set to "-1", in which case defaultValue also does not work
        if (maxExecutorMemory < 0) {
          maxExecutorMemory =  MRJobConfig.DEFAULT_MAP_MEMORY_MB * 1024L * 1024L;
        }
      }
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(final Configuration conf) {
    this.conf = conf;
  }

  public boolean isTez() {
    return isTez;
  }

  public boolean isLlap() {
    return isLlap;
  }

  public long getMaxExecutorMemory() {
    return maxExecutorMemory;
  }

  public long getMapJoinMemoryThreshold() {
    return mapJoinMemoryThreshold;
  }

  public long getDynPartJoinMemoryThreshold() {
    return dynPartJoinMemoryThreshold;
  }

  @Override
  public String toString() {
    return "MEMORY INFO - { isTez: " + isTez() +
        ", isLlap: " + isLlap() +
        ", maxExecutorMemory: " + LlapUtil.humanReadableByteCount(getMaxExecutorMemory()) +
        ", mapJoinMemoryThreshold: "+ LlapUtil.humanReadableByteCount(getMapJoinMemoryThreshold()) +
        ", dynPartJoinMemoryThreshold: " + LlapUtil.humanReadableByteCount(getDynPartJoinMemoryThreshold()) +
        " }";
  }
}