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

import java.util.ArrayList;
import java.util.List;


/**
 * A class that defines the constant strings used by the statistics implementation.
 */

public class StatsSetupConst {

  /**
   * The value of the user variable "hive.stats.dbclass" to use HBase implementation.
   */
  public static final String HBASE_IMPL_CLASS_VAL = "hbase";

  /**
   * The value of the user variable "hive.stats.dbclass" to use JDBC implementation.
   */
  public static final String JDBC_IMPL_CLASS_VAL = "jdbc";

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
   * @return List of all supported statistics
   */
  public static List<String> getSupportedStats() {
    List<String> supportedStats = new ArrayList<String>();
    supportedStats.add(NUM_FILES);
    supportedStats.add(ROW_COUNT);
    supportedStats.add(TOTAL_SIZE);
    supportedStats.add(RAW_DATA_SIZE);
    return supportedStats;
  }

  /**
   * @return List of all statistics that need to be collected during query execution. These are
   * statistics that inherently require a scan of the data.
   */
  public static List<String> getStatsToBeCollected() {
    List<String> collectableStats = new ArrayList<String>();
    collectableStats.add(ROW_COUNT);
    collectableStats.add(RAW_DATA_SIZE);
    return collectableStats;
  }

  /**
   * @return List of statistics that can be collected quickly without requiring a scan of the data.
   */
  public static List<String> getStatsFastCollection() {
    List<String> fastStats = new ArrayList<String>();
    fastStats.add(NUM_FILES);
    fastStats.add(TOTAL_SIZE);
    return fastStats;
  }
}
