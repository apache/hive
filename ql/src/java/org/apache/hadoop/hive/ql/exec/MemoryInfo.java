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
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains information about executor memory, various memory thresholds used for join conversions etc. based on
 * execution engine.
 **/

public class MemoryInfo {

  private final boolean isTez;
  private final boolean isLlap;
  private final long maxExecutorMemory;
  private final Logger LOG = LoggerFactory.getLogger(MemoryInfo.class);

  public MemoryInfo(Configuration conf) {
    this.isTez = "tez".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE));
    this.isLlap = "llap".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_MODE));
    if (isLlap) {
      LlapClusterStateForCompile llapInfo = LlapClusterStateForCompile.getClusterInfo(conf);
      llapInfo.initClusterInfo();
      if (llapInfo.hasClusterInfo()) {
        this.maxExecutorMemory = llapInfo.getMemoryPerExecutor();
        LOG.info("Using LLAP registry executor MB {}", maxExecutorMemory / (1024L * 1024L));
      } else {
        long memPerInstanceMb =
            HiveConf.getLongVar(conf, HiveConf.ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB);
        LOG.info("Using LLAP default executor MB {}", memPerInstanceMb);
        long numExecutors = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
        this.maxExecutorMemory = (memPerInstanceMb * 1024L * 1024L) / numExecutors;
      }
    } else if (isTez) {
        long containerSizeMb = DagUtils.getContainerResource(conf).getMemorySize();
        float heapFraction = HiveConf.getFloatVar(conf, HiveConf.ConfVars.TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION);
        this.maxExecutorMemory = (long) ((containerSizeMb * 1024L * 1024L) * heapFraction);
    } else {
      long executorMemoryFromConf =
          conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB) * 1024L * 1024L;
      // this can happen when config is explicitly set to "-1", in which case defaultValue also does not work
      if (executorMemoryFromConf < 0L) {
        LOG.warn("Falling back to default container MB {}", MRJobConfig.DEFAULT_MAP_MEMORY_MB);
        this.maxExecutorMemory = MRJobConfig.DEFAULT_MAP_MEMORY_MB * 1024L * 1024L;
      } else {
        this.maxExecutorMemory = executorMemoryFromConf;
      }
    }
  }

  /**
   * Returns True when in TEZ execution mode.
   * @return boolean
   */
  public boolean isTez() {
    return isTez;
  }

  /**
   * Returns True when in LLAP execution mode.
   * @return boolean
   */
  public boolean isLlap() {
    return isLlap;
  }

  /**
   * Get the Container max Memory value in bytes.
   * @return bytes value as long
   */
  public long getMaxExecutorMemory() {
    return maxExecutorMemory;
  }

  @Override
  public String toString() {
    return "MEMORY INFO - { isTez: " + isTez() +
        ", isLlap: " + isLlap() +
        ", maxExecutorMemory: " + LlapUtil.humanReadableByteCount(getMaxExecutorMemory()) +
        " }";
  }
}