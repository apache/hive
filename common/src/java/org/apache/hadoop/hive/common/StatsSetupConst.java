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
package org.apache.hadoop.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;



/**
 * A class that defines the constant strings used by the statistics implementation.
 */

public class StatsSetupConst {

  public enum StatDB {
    hbase {
      @Override
      public String getPublisher(Configuration conf) {
        return "org.apache.hadoop.hive.hbase.HBaseStatsPublisher"; }
      @Override
      public String getAggregator(Configuration conf) {
        return "org.apache.hadoop.hive.hbase.HBaseStatsAggregator"; }
    },
    jdbc {
      @Override
      public String getPublisher(Configuration conf) {
        return "org.apache.hadoop.hive.ql.stats.jdbc.JDBCStatsPublisher"; }
      @Override
      public String getAggregator(Configuration conf) {
        return "org.apache.hadoop.hive.ql.stats.jdbc.JDBCStatsAggregator"; }
    },
    counter {
      @Override
      public String getPublisher(Configuration conf) {
        return "org.apache.hadoop.hive.ql.stats.CounterStatsPublisher"; }
      @Override
      public String getAggregator(Configuration conf) {
        if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
          return "org.apache.hadoop.hive.ql.stats.CounterStatsAggregatorTez";
        } else if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
          return "org.apache.hadoop.hive.ql.stats.CounterStatsAggregatorSpark";
        }
        return "org.apache.hadoop.hive.ql.stats.CounterStatsAggregator"; }
    },
    fs {
      @Override
      public String getPublisher(Configuration conf) {
        return "org.apache.hadoop.hive.ql.stats.fs.FSStatsPublisher";
      }

      @Override
      public String getAggregator(Configuration conf) {
        return "org.apache.hadoop.hive.ql.stats.fs.FSStatsAggregator";
      }
    },
    custom {
      @Override
      public String getPublisher(Configuration conf) {
        return HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_STATS_DEFAULT_PUBLISHER); }
      @Override
      public String getAggregator(Configuration conf) {
        return HiveConf.getVar(conf,  HiveConf.ConfVars.HIVE_STATS_DEFAULT_AGGREGATOR); }
    };
    public abstract String getPublisher(Configuration conf);
    public abstract String getAggregator(Configuration conf);
  }

  // statistics stored in metastore
  /**
   * The name of the statistic Num Files to be published or gathered.
   */
  public static final String NUM_FILES = "numFiles";

  /**
   * The name of the statistic Num Partitions to be published or gathered.
   */
  public static final String NUM_PARTITIONS = "numPartitions";

  /**
   * The name of the statistic Total Size to be published or gathered.
   */
  public static final String TOTAL_SIZE = "totalSize";

  /**
   * The name of the statistic Row Count to be published or gathered.
   */
  public static final String ROW_COUNT = "numRows";

  /**
   * The name of the statistic Raw Data Size to be published or gathered.
   */
  public static final String RAW_DATA_SIZE = "rawDataSize";

  /**
   * Temp dir for writing stats from tasks.
   */
  public static final String STATS_TMP_LOC = "hive.stats.tmp.loc";

  public static final String STATS_FILE_PREFIX = "tmpstats-";
  /**
   * @return List of all supported statistics
   */
  public static final String[] supportedStats = {NUM_FILES,ROW_COUNT,TOTAL_SIZE,RAW_DATA_SIZE};

  /**
   * @return List of all statistics that need to be collected during query execution. These are
   * statistics that inherently require a scan of the data.
   */
  public static final String[] statsRequireCompute = new String[] {ROW_COUNT,RAW_DATA_SIZE};

  /**
   * @return List of statistics that can be collected quickly without requiring a scan of the data.
   */
  public static final String[] fastStats = new String[] {NUM_FILES,TOTAL_SIZE};

  // This string constant is used by stats task to indicate to AlterHandler that
  // alterPartition/alterTable is happening via statsTask.
  public static final String STATS_GENERATED_VIA_STATS_TASK = "STATS_GENERATED_VIA_STATS_TASK";

  // This string constant will be persisted in metastore to indicate whether corresponding
  // table or partition's statistics are accurate or not.
  public static final String COLUMN_STATS_ACCURATE = "COLUMN_STATS_ACCURATE";

  public static final String TRUE = "true";

  public static final String FALSE = "false";

  public static boolean areStatsUptoDate(Map<String, String> params) {
    String statsAcc = params.get(COLUMN_STATS_ACCURATE);
    return statsAcc == null ? false : statsAcc.equals(TRUE);
  }
}
