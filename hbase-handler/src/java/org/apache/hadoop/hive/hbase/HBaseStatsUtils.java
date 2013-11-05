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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.common.StatsSetupConst;



public class HBaseStatsUtils {

  private static final List<String> supportedStats = new ArrayList<String>();
  private static final Map<String, String> columnNameMapping = new HashMap<String, String>();

  static {
    // supported statistics
    supportedStats.add(StatsSetupConst.ROW_COUNT);
    supportedStats.add(StatsSetupConst.RAW_DATA_SIZE);

    // row count statistics
    columnNameMapping.put(StatsSetupConst.ROW_COUNT,
        HBaseStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME);

    // raw data size
    columnNameMapping.put(StatsSetupConst.RAW_DATA_SIZE,
        HBaseStatsSetupConstants.PART_STAT_RAW_DATA_SIZE_COLUMN_NAME);

  }

  /**
   * Returns the set of supported statistics
   */
  public static List<String> getSupportedStatistics() {
    return supportedStats;
  }

  /**
   * Retrieves the value for a particular stat from the published map.
   *
   * @param statType
   *          - statistic type to be retrieved from the map
   * @param stats
   *          - stats map
   * @return value for the given statistic as string, "0" if the statistic is not present
   */
  public static String getStatFromMap(String statType, Map<String, String> stats) {
    String value = stats.get(statType);
    if (value == null) {
      return "0";
    }
    return value;
  }

  /**
   * Check if the set to be published is within the supported statistics.
   * It must also contain at least the basic statistics (used for comparison).
   *
   * @param stats
   *          - stats to be published
   * @return true if is a valid statistic set, false otherwise
   */

  public static boolean isValidStatisticSet(Collection<String> stats) {
    if(!stats.contains(getBasicStat())) {
      return false;
    }
    for (String stat : stats) {
      if (!supportedStats.contains(stat)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if a particular statistic type is supported
   *
   * @param statType
   *          - statistic to be published
   * @return true if statType is supported, false otherwise
   */
  public static boolean isValidStatistic(String statType) {
    return supportedStats.contains(statType);
  }

  /**
   * Returns the HBase column where the statistics for the given type are stored.
   *
   * @param statType
   *          - supported statistic.
   * @return column name for the given statistic.
   */
  public static byte[] getColumnName(String statType) {
    return Bytes.toBytes(columnNameMapping.get(statType));
  }

  /**
   * Returns the family name for stored statistics.
   */
  public static byte[] getFamilyName() {
    return Bytes.toBytes(HBaseStatsSetupConstants.PART_STAT_COLUMN_FAMILY);
  }

  /**
   * Returns the basic type of the supported statistics.
   * It is used to determine which statistics are fresher.
   */

  public static String getBasicStat() {
    return supportedStats.get(0);
  }

}
