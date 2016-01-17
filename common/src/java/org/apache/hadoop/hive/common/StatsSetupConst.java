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
import org.json.JSONException;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that defines the constant strings used by the statistics implementation.
 */

public class StatsSetupConst {

  protected final static Logger LOG = LoggerFactory.getLogger(StatsSetupConst.class.getName());

  public enum StatDB {
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

  // This string constant is used by AlterHandler to figure out that it should not attempt to
  // update stats. It is set by any client-side task which wishes to signal that no stats
  // update should take place, such as with replication.
  public static final String DO_NOT_UPDATE_STATS = "DO_NOT_UPDATE_STATS";

  //This string constant will be persisted in metastore to indicate whether corresponding
  //table or partition's statistics and table or partition's column statistics are accurate or not.
  public static final String COLUMN_STATS_ACCURATE = "COLUMN_STATS_ACCURATE";

  public static final String COLUMN_STATS = "COLUMN_STATS";

  public static final String BASIC_STATS = "BASIC_STATS";

  public static final String TRUE = "true";

  public static final String FALSE = "false";

  public static boolean areBasicStatsUptoDate(Map<String, String> params) {
    String statsAcc = params.get(COLUMN_STATS_ACCURATE);
    if (statsAcc == null) {
      return false;
    } else {
      JSONObject jsonObj;
      try {
        jsonObj = new JSONObject(statsAcc);
        if (jsonObj != null && jsonObj.has(BASIC_STATS)) {
          return true;
        } else {
          return false;
        }
      } catch (JSONException e) {
        // For backward compatibility, if previous value can not be parsed to a
        // json object, it will come here.
        LOG.debug("In StatsSetupConst, JsonParser can not parse " + BASIC_STATS + ".");
        // We try to parse it as TRUE or FALSE
        if (statsAcc.equals(TRUE)) {
          setBasicStatsState(params, TRUE);
          return true;
        } else {
          setBasicStatsState(params, FALSE);
          return false;
        }
      }
    }
  }

  public static boolean areColumnStatsUptoDate(Map<String, String> params, String colName) {
    String statsAcc = params.get(COLUMN_STATS_ACCURATE);
    if (statsAcc == null) {
      return false;
    } else {
      JSONObject jsonObj;
      try {
        jsonObj = new JSONObject(statsAcc);
        if (jsonObj == null || !jsonObj.has(COLUMN_STATS)) {
          return false;
        } else {
          JSONObject columns = jsonObj.getJSONObject(COLUMN_STATS);
          if (columns != null && columns.has(colName)) {
            return true;
          } else {
            return false;
          }
        }
      } catch (JSONException e) {
        // For backward compatibility, if previous value can not be parsed to a
        // json object, it will come here.
        LOG.debug("In StatsSetupConst, JsonParser can not parse COLUMN_STATS.");
        return false;
      }
    }
  }

  // It will only throw JSONException when stats.put(BASIC_STATS, TRUE)
  // has duplicate key, which is not possible
  // note that set basic stats false will wipe out column stats too.
  public static void setBasicStatsState(Map<String, String> params, String setting) {
    if (setting.equals(FALSE)) {
      if (params.containsKey(COLUMN_STATS_ACCURATE)) {
        params.remove(COLUMN_STATS_ACCURATE);
      }
    } else {
      String statsAcc = params.get(COLUMN_STATS_ACCURATE);
      if (statsAcc == null) {
        JSONObject stats = new JSONObject(new LinkedHashMap());
        // duplicate key is not possible
        try {
          stats.put(BASIC_STATS, TRUE);
        } catch (JSONException e) {
          // impossible to throw any json exceptions.
          LOG.trace(e.getMessage());
        }
        params.put(COLUMN_STATS_ACCURATE, stats.toString());
      } else {
        // statsAcc may not be jason format, which will throw exception
        JSONObject stats;
        try {
          stats = new JSONObject(statsAcc);
        } catch (JSONException e) {
          // old format of statsAcc, e.g., TRUE or FALSE
          LOG.debug("In StatsSetupConst, JsonParser can not parse statsAcc.");
          stats = new JSONObject(new LinkedHashMap());
          try {
            if (statsAcc.equals(TRUE)) {
              stats.put(BASIC_STATS, TRUE);
            } else {
              stats.put(BASIC_STATS, FALSE);
            }
          } catch (JSONException e1) {
            // impossible to throw any json exceptions.
            LOG.trace(e1.getMessage());
          }
        }
        if (!stats.has(BASIC_STATS)) {
          // duplicate key is not possible
          try {
            stats.put(BASIC_STATS, TRUE);
          } catch (JSONException e) {
            // impossible to throw any json exceptions.
            LOG.trace(e.getMessage());
          }
        }
        params.put(COLUMN_STATS_ACCURATE, stats.toString());
      }
    }
  }

  public static void setColumnStatsState(Map<String, String> params, List<String> colNames) {
    try {
      String statsAcc = params.get(COLUMN_STATS_ACCURATE);
      JSONObject colStats = new JSONObject(new LinkedHashMap());
      // duplicate key is not possible
      for (String colName : colNames) {
        colStats.put(colName.toLowerCase(), TRUE);
      }
      if (statsAcc == null) {
        JSONObject stats = new JSONObject(new LinkedHashMap());
        // duplicate key is not possible
        stats.put(COLUMN_STATS, colStats);
        params.put(COLUMN_STATS_ACCURATE, stats.toString());
      } else {
        // statsAcc may not be jason format, which will throw exception
        JSONObject stats;
        try {
          stats = new JSONObject(statsAcc);
        } catch (JSONException e) {
          // old format of statsAcc, e.g., TRUE or FALSE
          LOG.debug("In StatsSetupConst, JsonParser can not parse statsAcc.");
          stats = new JSONObject(new LinkedHashMap());
          try {
            if (statsAcc.equals(TRUE)) {
              stats.put(BASIC_STATS, TRUE);
            } else {
              stats.put(BASIC_STATS, FALSE);
            }
          } catch (JSONException e1) {
            // impossible to throw any json exceptions.
            LOG.trace(e1.getMessage());
          }
        }
        if (!stats.has(COLUMN_STATS)) {
          // duplicate key is not possible
          stats.put(COLUMN_STATS, colStats);
        } else {
          // getJSONObject(COLUMN_STATS) should be found.
          JSONObject allColumnStats = stats.getJSONObject(COLUMN_STATS);
          for (String colName : colNames) {
            if (!allColumnStats.has(colName)) {
              // duplicate key is not possible
              allColumnStats.put(colName, TRUE);
            }
          }
          stats.remove(COLUMN_STATS);
          // duplicate key is not possible
          stats.put(COLUMN_STATS, allColumnStats);
        }
        params.put(COLUMN_STATS_ACCURATE, stats.toString());
      }
    } catch (JSONException e) {
      //impossible to throw any json exceptions.
      LOG.trace(e.getMessage());
    }
  }

  public static void clearColumnStatsState(Map<String, String> params) {
    String statsAcc = params.get(COLUMN_STATS_ACCURATE);
    if (statsAcc != null) {
      // statsAcc may not be jason format, which will throw exception
      JSONObject stats;
      try {
        stats = new JSONObject(statsAcc);
      } catch (JSONException e) {
        // old format of statsAcc, e.g., TRUE or FALSE
        LOG.debug("In StatsSetupConst, JsonParser can not parse statsAcc.");
        stats = new JSONObject(new LinkedHashMap());
        try {
          if (statsAcc.equals(TRUE)) {
            stats.put(BASIC_STATS, TRUE);
          } else {
            stats.put(BASIC_STATS, FALSE);
          }
        } catch (JSONException e1) {
          // impossible to throw any json exceptions.
          LOG.trace(e1.getMessage());
        }
      }
      if (stats.has(COLUMN_STATS)) {
        stats.remove(COLUMN_STATS);
      }
      params.put(COLUMN_STATS_ACCURATE, stats.toString());
    }
  }
}
