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
package org.apache.hadoop.hive.ql.stats.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.stats.StatsSetupConst;

public class JDBCStatsUtils {

  private static final List<String> supportedStats = new ArrayList<String>();
  private static final Map<String, String> columnNameMapping = new HashMap<String, String>();
  static {
    // supported statistics
    supportedStats.add(StatsSetupConst.ROW_COUNT);
    supportedStats.add(StatsSetupConst.RAW_DATA_SIZE);

    // row count statistics
    columnNameMapping.put(StatsSetupConst.ROW_COUNT,
        JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME);

    // raw data size
    columnNameMapping.put(StatsSetupConst.RAW_DATA_SIZE,
        JDBCStatsSetupConstants.PART_STAT_RAW_DATA_SIZE_COLUMN_NAME);
  }

  /**
   * Returns the set of supported statistics
   */
  public static List<String> getSupportedStatistics() {
    return supportedStats;
  }

  /**
   * Check if the set to be published is within the supported statistics.
   * It must also contain at least the basic statistics (used for comparison)
   *
   * @param stats
   *          - stats to be published
   * @return true if is a valid statistic set, false otherwise
   */

  public static boolean isValidStatisticSet(Collection<String> stats) {
    if (!stats.contains(getBasicStat())) {
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
   * Returns the name of the column storing the key for statistics.
   */
  public static String getIdColumnName() {
    return JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME;
  }

  public static String getTimestampColumnName() {
    return JDBCStatsSetupConstants.PART_STAT_TIMESTAMP_COLUMN_NAME;
  }

  public static String getStatTableName() {
    return JDBCStatsSetupConstants.PART_STAT_TABLE_NAME;
  }

  /**
   * Returns the column where the statistics for the given type are stored.
   *
   * @param statType
   *          - supported statistic.
   * @return column name for the given statistic.
   */
  public static String getStatColumnName(String statType) {
    return columnNameMapping.get(statType);
  }

  /**
   * Returns the basic type of the supported statistics.
   * It is used to determine which statistics are fresher.
   */
  public static String getBasicStat() {
    return supportedStats.get(0);
  }





  /**
   * Prepares CREATE TABLE query
   */
  public static String getCreate(String comment) {
    String create = "CREATE TABLE /* " + comment + " */ " + JDBCStatsUtils.getStatTableName() +
          " (" + getTimestampColumnName() + " TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
          JDBCStatsUtils.getIdColumnName() + " VARCHAR(255) PRIMARY KEY ";
    for (int i = 0; i < supportedStats.size(); i++) {
      create += ", " + getStatColumnName(supportedStats.get(i)) + " BIGINT ";
    }
    create += ")";
    return create;
  }

  /**
   * Prepares UPDATE statement issued when updating existing statistics
   */
  public static String getUpdate(String comment) {
    String update = "UPDATE /* " + comment + " */ " + getStatTableName() + " SET ";
    for (int i = 0; i < supportedStats.size(); i++) {
      update += columnNameMapping.get(supportedStats.get(i)) + " = ? , ";
    }
    update += getTimestampColumnName() + " = CURRENT_TIMESTAMP";
    update += " WHERE " + JDBCStatsUtils.getIdColumnName() + " = ? AND ? > ( SELECT TEMP."
        + getStatColumnName(getBasicStat()) + " FROM ( " +
        " SELECT " + getStatColumnName(getBasicStat()) + " FROM " + getStatTableName() + " WHERE "
        + getIdColumnName() + " = ? ) TEMP )";
    return update;
  }

  /**
   * Prepares INSERT statement for statistic publishing.
   */
  public static String getInsert(String comment) {
    String columns = JDBCStatsUtils.getIdColumnName();
    String values = "?";

    for (int i = 0; i < supportedStats.size(); i++) {
      columns += ", " + getStatColumnName(supportedStats.get(i));
      values += ", ?";
    }
    String insert = "INSERT INTO /* " + comment + " */ " + getStatTableName() + "(" + columns +
        ") VALUES (" + values + ")";
    return insert;
  }

  /**
   * Prepares SELECT query for statistics aggregation.
   *
   * @param statType
   *          - statistic type to be aggregated.
   * @param comment
   * @return aggregated value for the given statistic
   */
  public static String getSelectAggr(String statType, String comment) {
    String select = "SELECT /* " + comment + " */ " + "SUM( "
        + getStatColumnName(statType) + " ) " + " FROM "
        + getStatTableName() + " WHERE " + JDBCStatsUtils.getIdColumnName() + " LIKE ? ESCAPE ?";
    return select;
  }

  /**
   * Prepares DELETE statement for cleanup.
   */
  public static String getDeleteAggr(String rowID, String comment) {
    String delete = "DELETE /* " + comment + " */ " +
        " FROM " + getStatTableName() + " WHERE " + JDBCStatsUtils.getIdColumnName() +
        " LIKE ? ESCAPE ?";
    return delete;
  }

}
