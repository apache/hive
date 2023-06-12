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
package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


/**
 * A class that defines the constant strings used by the statistics implementation.
 */

public class StatsSetupConst {

  protected static final Logger LOG = LoggerFactory.getLogger(StatsSetupConst.class.getName());

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
        return MetastoreConf.getVar(conf, ConfVars.STATS_DEFAULT_PUBLISHER); }
      @Override
      public String getAggregator(Configuration conf) {
        return MetastoreConf.getVar(conf,  ConfVars.STATS_DEFAULT_AGGREGATOR); }
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

  public static final String RUN_TIME_ROW_COUNT = "runTimeNumRows";

  public static final String INSERT_COUNT = "insertCount";
  public static final String UPDATE_COUNT = "updateCount";
  public static final String DELETE_COUNT = "deleteCount";

  /**
   * The name of the statistic Raw Data Size to be published or gathered.
   */
  public static final String RAW_DATA_SIZE = "rawDataSize";

  /**
   * The name of the statistic for Number of Erasure Coded Files - to be published or gathered.
   */
  public static final String NUM_ERASURE_CODED_FILES = "numFilesErasureCoded";

  /**
   * Temp dir for writing stats from tasks.
   */
  public static final String STATS_TMP_LOC = "hive.stats.tmp.loc";

  public static final String STATS_FILE_PREFIX = "tmpstats-";
  /**
   * List of all supported statistics
   */
  public static final List<String> SUPPORTED_STATS = ImmutableList.of(
      NUM_FILES, ROW_COUNT, TOTAL_SIZE, RAW_DATA_SIZE, NUM_ERASURE_CODED_FILES);

  /**
   * List of all statistics that need to be collected during query execution. These are
   * statistics that inherently require a scan of the data.
   */
  public static final List<String> STATS_REQUIRE_COMPUTE = ImmutableList.of(ROW_COUNT, RAW_DATA_SIZE);

  /**
   * List of statistics that can be collected quickly without requiring a scan of the data.
   */
  public static final List<String> FAST_STATS = ImmutableList.of(
      NUM_FILES, TOTAL_SIZE, NUM_ERASURE_CODED_FILES);

  // This string constant is used to indicate to AlterHandler that
  // alterPartition/alterTable is happening via statsTask or via user.
  public static final String STATS_GENERATED = "STATS_GENERATED";

  public static final String TASK = "TASK";

  public static final String USER = "USER";

  // This string constant is used by AlterHandler to figure out that it should not attempt to
  // update stats. It is set by any client-side task which wishes to signal that no stats
  // update should take place, such as with replication.
  public static final String DO_NOT_UPDATE_STATS = "DO_NOT_UPDATE_STATS";

  // This string constant is used by AlterHandler to figure out that it should not attempt to
  // populate quick stats based on the file listing
  public static final String DO_NOT_POPULATE_QUICK_STATS = "DO_NOT_POPULATE_QUICK_STATS";

  //This string constant will be persisted in metastore to indicate whether corresponding
  //table or partition's statistics and table or partition's column statistics are accurate or not.
  public static final String COLUMN_STATS_ACCURATE = "COLUMN_STATS_ACCURATE";

  public static final String COLUMN_STATS = "COLUMN_STATS";

  public static final String BASIC_STATS = "BASIC_STATS";

  public static final String CASCADE = "CASCADE";

  public static final String  STATS_FOR_CREATE_TABLE = "setStatsStateForCreateTable";

  public static final String TRUE = "true";

  public static final String FALSE = "false";

  // The parameter keys for the table statistics. Those keys are excluded from 'show create table' command output.
  public static final List<String> TABLE_PARAMS_STATS_KEYS = ImmutableList.of(
      COLUMN_STATS_ACCURATE, NUM_FILES, TOTAL_SIZE, ROW_COUNT, RAW_DATA_SIZE, NUM_PARTITIONS,
      NUM_ERASURE_CODED_FILES);

  @JsonPropertyOrder({"basicStats", "columnStats"})
  private static class ColumnStatsAccurate {
    private static ObjectReader objectReader;
    private static ObjectWriter objectWriter;

    static {
      ObjectMapper objectMapper = new ObjectMapper();
      objectReader = objectMapper.readerFor(ColumnStatsAccurate.class);
      objectWriter = objectMapper.writerFor(ColumnStatsAccurate.class);
    }

    static class BooleanSerializer extends JsonSerializer<Boolean> {

      @Override
      public void serialize(Boolean value, JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeString(value.toString());
      }
    }

    static class BooleanDeserializer extends JsonDeserializer<Boolean> {

      public Boolean deserialize(JsonParser jsonParser,
          DeserializationContext deserializationContext)
              throws IOException {
        return Boolean.valueOf(jsonParser.getValueAsString());
      }
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @JsonSerialize(using = BooleanSerializer.class)
    @JsonDeserialize(using = BooleanDeserializer.class)
    @JsonProperty(BASIC_STATS)
    boolean basicStats;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty(COLUMN_STATS)
    @JsonSerialize(contentUsing = BooleanSerializer.class)
    @JsonDeserialize(contentUsing = BooleanDeserializer.class)
    TreeMap<String, Boolean> columnStats = new TreeMap<>();

  }

  /**
   * Class for marking the column statistics when creating tables.
   */
  public static class ColumnStatsSetup {
    private static ObjectReader objectReader;
    private static ObjectWriter objectWriter;
    static {
      ObjectMapper objectMapper = new ObjectMapper();
      objectReader = objectMapper.readerFor(ColumnStatsSetup.class);
      objectWriter = objectMapper.writerFor(ColumnStatsSetup.class);
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean enabled;
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isIcebergTable;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> columnNames = new ArrayList<>();

    public static ColumnStatsSetup parseStatsSetup(String statsSetup) {
      if (statsSetup == null) {
        return new ColumnStatsSetup();
      }
      try {
        return objectReader.readValue(statsSetup);
      } catch (Exception e) {
         return new ColumnStatsSetup();
      }
    }

    /**
     * Get json representation of the ColumnStatsSetup
     */
    public static String getStatsSetupAsString(boolean enabled,
        boolean isIcebergTable,
        List<String> columns) {
      try {
        ColumnStatsSetup statsSetup = new ColumnStatsSetup();
        statsSetup.enabled = enabled;
        statsSetup.isIcebergTable = isIcebergTable;
        statsSetup.columnNames = new ArrayList<>(columns);
        return objectWriter.writeValueAsString(statsSetup);
      } catch (Exception e) {
        // this should not happen
        throw new RuntimeException(e);
      }
    }
  }

  public static boolean areBasicStatsUptoDate(Map<String, String> params) {
    if (params == null) {
      return false;
    }
    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));
    return stats.basicStats;
  }

  public static boolean areColumnStatsUptoDate(Map<String, String> params, String colName) {
    if (params == null) {
      return false;
    }
    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));
    return stats.columnStats.containsKey(colName);
  }

  // It will only throw JSONException when stats.put(BASIC_STATS, TRUE)
  // has duplicate key, which is not possible
  // note that set basic stats false will wipe out column stats too.
  public static void setBasicStatsState(Map<String, String> params, String setting) {
    if (setting.equals(FALSE)) {
      if (params!=null && params.containsKey(COLUMN_STATS_ACCURATE)) {
        params.remove(COLUMN_STATS_ACCURATE);
      }
      return;
    }
    if (params == null) {
      throw new RuntimeException("params are null...cant set columnstatstate!");
    }
    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));
    stats.basicStats = true;
    try {
      params.put(COLUMN_STATS_ACCURATE, ColumnStatsAccurate.objectWriter.writeValueAsString(stats));
    } catch (JsonProcessingException e) {
      throw new RuntimeException("can't serialize column stats", e);
    }
  }

  public static void setColumnStatsState(Map<String, String> params, List<String> colNames) {
    if (params == null) {
      throw new RuntimeException("params are null...cant set columnstatstate!");
    }
    if (colNames == null) {
      return;
    }
    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));

    for (String colName : colNames) {
      if (!stats.columnStats.containsKey(colName)) {
        stats.columnStats.put(colName, true);
      }
    }

    try {
      params.put(COLUMN_STATS_ACCURATE, ColumnStatsAccurate.objectWriter.writeValueAsString(stats));
    } catch (JsonProcessingException e) {
      LOG.trace(e.getMessage());
    }
  }

  /**
   * @param params table/partition parameters
   * @return the list of column names for which the stats are available.
   */
  public static List<String> getColumnsHavingStats(Map<String, String> params) {
    if (params == null) {
      // No table/partition params, no statistics available
      return null;
    }

    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));

    // No stats available.
    if (stats == null) {
      return null;
    }

    List<String> colNames = new ArrayList<String>();
    for (Map.Entry<String, Boolean> entry : stats.columnStats.entrySet()) {
      if (entry.getValue()) {
        colNames.add(entry.getKey());
      }
    }

    return colNames;
  }

  public static boolean canColumnStatsMerge(Map<String, String> params, String colName) {
    if (params == null) {
      return false;
    }
    // TODO: should this also check that the basic flag is valid?
    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));
    return stats.columnStats.containsKey(colName);
  }

  public static void clearColumnStatsState(Map<String, String> params) {
    if (params == null || params.get(COLUMN_STATS_ACCURATE) == null) {
      return;
    }

    ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));
    stats.columnStats.clear();

    try {
      params.put(COLUMN_STATS_ACCURATE, ColumnStatsAccurate.objectWriter.writeValueAsString(stats));
    } catch (JsonProcessingException e) {
      LOG.trace(e.getMessage());
    }
  }

  public static void removeColumnStatsState(Map<String, String> params, List<String> colNames) {
    if (params == null) {
      return;
    }
    try {
      ColumnStatsAccurate stats = parseStatsAcc(params.get(COLUMN_STATS_ACCURATE));
      for (String string : colNames) {
        stats.columnStats.remove(string);
      }
      params.put(COLUMN_STATS_ACCURATE, ColumnStatsAccurate.objectWriter.writeValueAsString(stats));
    } catch (JsonProcessingException e) {
      LOG.trace(e.getMessage());
    }
  }

  public static void setStatsStateForCreateTable(Map<String, String> params,
      List<String> cols, String setting) {
    if (TRUE.equals(setting)) {
      for (String stat : StatsSetupConst.SUPPORTED_STATS) {
        params.put(stat, "0");
      }
    }
    setBasicStatsState(params, setting);
    if (TRUE.equals(setting)) {
      setColumnStatsState(params, cols);
    }
  }

  private static ColumnStatsAccurate parseStatsAcc(String statsAcc) {
    if (statsAcc == null) {
      return new ColumnStatsAccurate();
    }
    try {
      return ColumnStatsAccurate.objectReader.readValue(statsAcc);
    } catch (Exception e) {
      ColumnStatsAccurate ret = new ColumnStatsAccurate();
      if (TRUE.equalsIgnoreCase(statsAcc)) {
        ret.basicStats = true;
      }
      return ret;
    }
  }
}
