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
import org.apache.hadoop.hive.common.GcTimeMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MemoryExhaustionChecker} specific to Hive-on-Spark. Unlike the
 * {@link DefaultMemoryExhaustionChecker} it uses a {@link GcTimeMonitor}
 * to monitor how much time (what percentage of run time within the last
 * minute or so) is spent in GC. If this value exceeds the configured value
 * in {@link HiveConf.ConfVars#HIVEHASHTABLEMAXGCTIMEPERCENTAGE}, a
 * {@link MapJoinMemoryExhaustionError} is thrown.
 */
class SparkMemoryExhaustionChecker implements MemoryExhaustionChecker {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMemoryExhaustionChecker.class);

  private static SparkMemoryExhaustionChecker INSTANCE;

  // The GC time alert functionality below is used by the checkGcOverhead() method.
  // This method may be called very frequently, and if it called
  // GcTimeMonitor.getLatestGcData() every time, it could result in unnecessary
  // overhead due to synchronization and new object creation. So instead,
  // GcTimeMonitor itself sets the "red flag" in lastAlertGcTimePercentage,
  // and checkGcOverhead() may check it as frequently as needed.
  private volatile int lastAlertGcTimePercentage;
  private final int criticalGcTimePercentage;

  private SparkMemoryExhaustionChecker(Configuration conf) {
    super();
    criticalGcTimePercentage = (int) (HiveConf.getFloatVar(
        conf, HiveConf.ConfVars.HIVEHASHTABLEMAXGCTIMEPERCENTAGE) * 100);
    GcTimeMonitor hiveGcTimeMonitor = new HiveGcTimeMonitor(criticalGcTimePercentage);
    hiveGcTimeMonitor.start();
  }

  static synchronized SparkMemoryExhaustionChecker get(Configuration conf) {
    if (INSTANCE == null) {
      INSTANCE = new SparkMemoryExhaustionChecker(conf);
    }
    return INSTANCE;
  }

  @Override
  public void checkMemoryOverhead(long rowNumber, long hashTableScale, int tableContainerSize) {
    if (lastAlertGcTimePercentage >= criticalGcTimePercentage) {
      String msg = "GC time percentage = " + lastAlertGcTimePercentage + "% exceeded threshold "
              + criticalGcTimePercentage + "%";
      throw new MapJoinMemoryExhaustionError(msg);
    }
  }

  // GC time monitoring
  private class HiveGcTimeMonitor extends GcTimeMonitor {

    HiveGcTimeMonitor(int criticalGcTimePercentage) {
      super(45 * 1000, 200, criticalGcTimePercentage, new HiveGcTimeAlertHandler());
    }
  }

  private class HiveGcTimeAlertHandler implements GcTimeMonitor.GcTimeAlertHandler {

    @Override
    public void alert(GcTimeMonitor.GcData gcData) {
      lastAlertGcTimePercentage = gcData.getGcTimePercentage();
      LOG.warn("GcTimeMonitor alert called. Current GC time = " + lastAlertGcTimePercentage + "%");
    }
  }
}
