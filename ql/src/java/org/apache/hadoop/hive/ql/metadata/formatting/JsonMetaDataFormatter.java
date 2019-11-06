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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.hadoop.hive.conf.Constants.MATERIALIZED_VIEW_REWRITING_TIME_WINDOW;

/**
 * Format table and index information for machine readability using
 * json.
 */
public class JsonMetaDataFormatter implements MetaDataFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonMetaDataFormatter.class);

  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_TYPE = "type";
  private static final String COLUMN_COMMENT = "comment";
  private static final String COLUMN_MIN = "min";
  private static final String COLUMN_MAX = "max";
  private static final String COLUMN_NUM_NULLS = "numNulls";
  private static final String COLUMN_NUM_TRUES = "numTrues";
  private static final String COLUMN_NUM_FALSES = "numFalses";
  private static final String COLUMN_DISTINCT_COUNT = "distinctCount";
  private static final String COLUMN_AVG_LENGTH = "avgColLen";
  private static final String COLUMN_MAX_LENGTH = "maxColLen";


  /**
   * Convert the map to a JSON string.
   */
  private void asJson(OutputStream out, Map<String, Object> data)
      throws HiveException
      {
    try {
      new ObjectMapper().writeValue(out, data);
    } catch (IOException e) {
      throw new HiveException("Unable to convert to json", e);
    }
      }

  /**
   * Write an error message.
   */
  @Override
  public void error(OutputStream out, String msg, int errorCode, String sqlState)
      throws HiveException
      {
    error(out, msg, errorCode, sqlState, null);
      }
  @Override
  public void error(OutputStream out, String errorMessage, int errorCode, String sqlState, String errorDetail) throws HiveException {
    MapBuilder mb = MapBuilder.create().put("error", errorMessage);
    if(errorDetail != null) {
      mb.put("errorDetail", errorDetail);
    }
    mb.put("errorCode", errorCode);
    if(sqlState != null) {
      mb.put("sqlState", sqlState);
    }
    asJson(out,mb.build());
  }

  /**
   * Show a list of tables.
   */
  @Override
  public void showTables(DataOutputStream out, Set<String> tables)
      throws HiveException {
    asJson(out, MapBuilder.create().put("tables", tables).build());
  }

  /**
   * Show a list of tables including table types.
   */
  @Override
  public void showTablesExtended(DataOutputStream out, List<Table> tables)
      throws HiveException {
    if (tables.isEmpty()) {
      // Nothing to do
      return;
    }

    MapBuilder builder = MapBuilder.create();
    ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
    for (Table table : tables) {
      final String tableName = table.getTableName();
      final String tableType = table.getTableType().toString();
      res.add(builder
          .put("Table Name", tableName)
          .put("Table Type", tableType)
          .build());
    }
    asJson(out, builder.put("tables", res).build());
  }

  /**
   * Show a list of materialized views.
   */
  @Override
  public void showMaterializedViews(DataOutputStream out, List<Table> materializedViews)
      throws HiveException {
    if (materializedViews.isEmpty()) {
      // Nothing to do
      return;
    }

    MapBuilder builder = MapBuilder.create();
    ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
    for (Table mv : materializedViews) {
      final String mvName = mv.getTableName();
      final String rewriteEnabled = mv.isRewriteEnabled() ? "Yes" : "No";
      // Currently, we only support manual refresh
      // TODO: Update whenever we have other modes
      final String refreshMode = "Manual refresh";
      final String timeWindowString = mv.getProperty(MATERIALIZED_VIEW_REWRITING_TIME_WINDOW);
      final String mode;
      if (!org.apache.commons.lang.StringUtils.isEmpty(timeWindowString)) {
        long time = HiveConf.toTime(timeWindowString,
            HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW),
            TimeUnit.MINUTES);
        if (time > 0L) {
          mode = refreshMode + " (Valid for " + time + "min)";
        } else if (time == 0L) {
          mode = refreshMode + " (Valid until source tables modified)";
        } else {
          mode = refreshMode + " (Valid always)";
        }
      } else {
        mode = refreshMode;
      }
      res.add(builder
          .put("MV Name", mvName)
          .put("Rewriting Enabled", rewriteEnabled)
          .put("Mode", mode)
          .build());
    }
    asJson(out, builder.put("materialized views", res).build());
  }

  /**
   * Describe table.
   */
  @Override
  public void describeTable(DataOutputStream out, String colPath, String tableName, Table tbl, Partition part,
      List<FieldSchema> cols, boolean isFormatted, boolean isExt, boolean isOutputPadded,
      List<ColumnStatisticsObj> colStats) throws HiveException {
    MapBuilder builder = MapBuilder.create();
    builder.put("columns", createColumnsInfo(cols, colStats));

    if (isExt) {
      if (part != null) {
        builder.put("partitionInfo", part.getTPartition());
      }
      else {
        builder.put("tableInfo", tbl.getTTable());
      }
      if (PrimaryKeyInfo.isPrimaryKeyInfoNotEmpty(tbl.getPrimaryKeyInfo())) {
        builder.put("primaryKeyInfo", tbl.getPrimaryKeyInfo());
      }
      if (ForeignKeyInfo.isForeignKeyInfoNotEmpty(tbl.getForeignKeyInfo())) {
        builder.put("foreignKeyInfo", tbl.getForeignKeyInfo());
      }
      if (UniqueConstraint.isUniqueConstraintNotEmpty(tbl.getUniqueKeyInfo())) {
        builder.put("uniqueConstraintInfo", tbl.getUniqueKeyInfo());
      }
      if (NotNullConstraint.isNotNullConstraintNotEmpty(tbl.getNotNullConstraint())) {
        builder.put("notNullConstraintInfo", tbl.getNotNullConstraint());
      }
      if (DefaultConstraint.isCheckConstraintNotEmpty(tbl.getDefaultConstraint())) {
        builder.put("defaultConstraintInfo", tbl.getDefaultConstraint());
      }
      if (CheckConstraint.isCheckConstraintNotEmpty(tbl.getCheckConstraint())) {
        builder.put("checkConstraintInfo", tbl.getCheckConstraint());
      }
      if (tbl.getStorageHandlerInfo() != null) {
        builder.put("storageHandlerInfo", tbl.getStorageHandlerInfo().toString());
      }
    }

    asJson(out, builder.build());
  }

  private List<Map<String, Object>> createColumnsInfo(List<FieldSchema> columns,
      List<ColumnStatisticsObj> columnStatisticsList) {
    ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
    for (FieldSchema column : columns) {
      ColumnStatisticsData statistics = getStatistics(column, columnStatisticsList);
      res.add(createColumnInfo(column, statistics));
    }
    return res;
  }

  private ColumnStatisticsData getStatistics(FieldSchema column, List<ColumnStatisticsObj> columnStatisticsList) {
    for (ColumnStatisticsObj columnStatistics : columnStatisticsList) {
      if (column.getName().equals(columnStatistics.getColName())) {
        return columnStatistics.getStatsData();
      }
    }

    return null;
  }

  private Map<String, Object> createColumnInfo(FieldSchema column, ColumnStatisticsData statistics) {
    Map<String, Object> result = MapBuilder.create()
        .put(COLUMN_NAME, column.getName())
        .put(COLUMN_TYPE, column.getType())
        .put(COLUMN_COMMENT, column.getComment())
        .build();

    if (statistics != null) {
      if (statistics.isSetBinaryStats()) {
        if (statistics.getBinaryStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getBinaryStats().getNumNulls());
        }
        if (statistics.getBinaryStats().isSetAvgColLen()) {
          result.put(COLUMN_AVG_LENGTH, statistics.getBinaryStats().getAvgColLen());
        }
        if (statistics.getBinaryStats().isSetMaxColLen()) {
          result.put(COLUMN_MAX_LENGTH, statistics.getBinaryStats().getMaxColLen());
        }
      } else if (statistics.isSetStringStats()) {
        if (statistics.getStringStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getStringStats().getNumNulls());
        }
        if (statistics.getStringStats().isSetNumDVs()) {
          result.put(COLUMN_DISTINCT_COUNT, statistics.getStringStats().getNumDVs());
        }
        if (statistics.getStringStats().isSetAvgColLen()) {
          result.put(COLUMN_AVG_LENGTH, statistics.getStringStats().getAvgColLen());
        }
        if (statistics.getStringStats().isSetMaxColLen()) {
          result.put(COLUMN_MAX_LENGTH, statistics.getStringStats().getMaxColLen());
        }
      } else if (statistics.isSetBooleanStats()) {
        if (statistics.getBooleanStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getBooleanStats().getNumNulls());
        }
        if (statistics.getBooleanStats().isSetNumTrues()) {
          result.put(COLUMN_NUM_TRUES, statistics.getBooleanStats().getNumTrues());
        }
        if (statistics.getBooleanStats().isSetNumFalses()) {
          result.put(COLUMN_NUM_FALSES, statistics.getBooleanStats().getNumFalses());
        }
      } else if (statistics.isSetDecimalStats()) {
        if (statistics.getDecimalStats().isSetLowValue()) {
          result.put(COLUMN_MIN, MetaDataFormatUtils.convertToString(statistics.getDecimalStats().getLowValue()));
        }
        if (statistics.getDecimalStats().isSetHighValue()) {
          result.put(COLUMN_MAX, MetaDataFormatUtils.convertToString(statistics.getDecimalStats().getHighValue()));
        }
        if (statistics.getDecimalStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getDecimalStats().getNumNulls());
        }
        if (statistics.getDecimalStats().isSetNumDVs()) {
          result.put(COLUMN_DISTINCT_COUNT, statistics.getDecimalStats().getNumDVs());
        }
      } else if (statistics.isSetDoubleStats()) {
        if (statistics.getDoubleStats().isSetLowValue()) {
          result.put(COLUMN_MIN, statistics.getDoubleStats().getLowValue());
        }
        if (statistics.getDoubleStats().isSetHighValue()) {
          result.put(COLUMN_MAX, statistics.getDoubleStats().getHighValue());
        }
        if (statistics.getDoubleStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getDoubleStats().getNumNulls());
        }
        if (statistics.getDoubleStats().isSetNumDVs()) {
          result.put(COLUMN_DISTINCT_COUNT, statistics.getDoubleStats().getNumDVs());
        }
      } else if (statistics.isSetLongStats()) {
        if (statistics.getLongStats().isSetLowValue()) {
          result.put(COLUMN_MIN, statistics.getLongStats().getLowValue());
        }
        if (statistics.getLongStats().isSetHighValue()) {
          result.put(COLUMN_MAX, statistics.getLongStats().getHighValue());
        }
        if (statistics.getLongStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getLongStats().getNumNulls());
        }
        if (statistics.getLongStats().isSetNumDVs()) {
          result.put(COLUMN_DISTINCT_COUNT, statistics.getLongStats().getNumDVs());
        }
      } else if (statistics.isSetDateStats()) {
        if (statistics.getDateStats().isSetLowValue()) {
          result.put(COLUMN_MIN, MetaDataFormatUtils.convertToString(statistics.getDateStats().getLowValue()));
        }
        if (statistics.getDateStats().isSetHighValue()) {
          result.put(COLUMN_MAX, MetaDataFormatUtils.convertToString(statistics.getDateStats().getHighValue()));
        }
        if (statistics.getDateStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getDateStats().getNumNulls());
        }
        if (statistics.getDateStats().isSetNumDVs()) {
          result.put(COLUMN_DISTINCT_COUNT, statistics.getDateStats().getNumDVs());
        }
      } else if (statistics.isSetTimestampStats()) {
        if (statistics.getTimestampStats().isSetLowValue()) {
          result.put(COLUMN_MIN, MetaDataFormatUtils.convertToString(statistics.getTimestampStats().getLowValue()));
        }
        if (statistics.getTimestampStats().isSetHighValue()) {
          result.put(COLUMN_MAX, MetaDataFormatUtils.convertToString(statistics.getTimestampStats().getHighValue()));
        }
        if (statistics.getTimestampStats().isSetNumNulls()) {
          result.put(COLUMN_NUM_NULLS, statistics.getTimestampStats().getNumNulls());
        }
        if (statistics.getTimestampStats().isSetNumDVs()) {
          result.put(COLUMN_DISTINCT_COUNT, statistics.getTimestampStats().getNumDVs());
        }
      }
    }

    return result;
  }

  @Override
  public void showTableStatus(DataOutputStream out, Hive db, HiveConf conf,
      List<Table> tbls, Map<String, String> part, Partition par)
          throws HiveException {
    asJson(out, MapBuilder.create().put(
        "tables", makeAllTableStatus(db, conf, tbls, part, par)).build());
  }

  private List<Map<String, Object>> makeAllTableStatus(Hive db, HiveConf conf,
      List<Table> tbls, Map<String, String> part, Partition par)
          throws HiveException {
    try {
      ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
      for (Table tbl : tbls) {
        res.add(makeOneTableStatus(tbl, db, conf, part, par));
      }
      return res;
    } catch(IOException e) {
      throw new HiveException(e);
    }
  }

  private Map<String, Object> makeOneTableStatus(Table tbl, Hive db,
      HiveConf conf, Map<String, String> part, Partition par)
          throws HiveException, IOException {
    String tblLoc = null;
    String inputFormattCls = null;
    String outputFormattCls = null;
    if (part != null) {
      if (par != null) {
        if (par.getLocation() != null) {
          tblLoc = par.getDataLocation().toString();
        }
        inputFormattCls = par.getInputFormatClass() == null ? null : par.getInputFormatClass().getName();
        outputFormattCls = par.getOutputFormatClass() == null ? null : par.getOutputFormatClass().getName();
      }
    } else {
      if (tbl.getPath() != null) {
        tblLoc = tbl.getDataLocation().toString();
      }
      inputFormattCls = tbl.getInputFormatClass() == null ? null : tbl.getInputFormatClass().getName();
      outputFormattCls = tbl.getOutputFormatClass() == null ? null : tbl.getOutputFormatClass().getName();
    }

    MapBuilder builder = MapBuilder.create();

    builder.put("tableName", tbl.getTableName());
    builder.put("ownerType", (tbl.getOwnerType() != null) ? tbl.getOwnerType().name() : "null");
    builder.put("owner", tbl.getOwner());
    builder.put("location", tblLoc);
    builder.put("inputFormat", inputFormattCls);
    builder.put("outputFormat", outputFormattCls);
    builder.put("columns", createColumnsInfo(tbl.getCols(), new ArrayList<ColumnStatisticsObj>()));

    builder.put("partitioned", tbl.isPartitioned());
    if (tbl.isPartitioned()) {
      builder.put("partitionColumns", createColumnsInfo(tbl.getPartCols(), new ArrayList<ColumnStatisticsObj>()));
    }
    if(tbl.getTableType() != TableType.VIRTUAL_VIEW) {
      //tbl.getPath() is null for views
      putFileSystemsStats(builder, makeTableStatusLocations(tbl, db, par),
        conf, tbl.getPath());
    }

    return builder.build();
  }

  private List<Path> makeTableStatusLocations(Table tbl, Hive db, Partition par)
      throws HiveException {
    // output file system information
    Path tblPath = tbl.getPath();
    List<Path> locations = new ArrayList<Path>();
    if (tbl.isPartitioned()) {
      if (par == null) {
        for (Partition curPart : db.getPartitions(tbl)) {
          if (curPart.getLocation() != null) {
            locations.add(new Path(curPart.getLocation()));
          }
        }
      } else {
        if (par.getLocation() != null) {
          locations.add(new Path(par.getLocation()));
        }
      }
    } else {
      if (tblPath != null) {
        locations.add(tblPath);
      }
    }

    return locations;
  }

  /**
   * @param tblPath not NULL
   * @throws IOException
   */
  // Duplicates logic in TextMetaDataFormatter
  private void putFileSystemsStats(MapBuilder builder, List<Path> locations,
      HiveConf conf, Path tblPath)
          throws IOException {
    long totalFileSize = 0;
    long maxFileSize = 0;
    long minFileSize = Long.MAX_VALUE;
    long lastAccessTime = 0;
    long lastUpdateTime = 0;
    int numOfFiles = 0;

    boolean unknown = false;
    FileSystem fs = tblPath.getFileSystem(conf);
    // in case all files in locations do not exist
    try {
      FileStatus tmpStatus = fs.getFileStatus(tblPath);
      lastAccessTime = tmpStatus.getAccessTime();
      lastUpdateTime = tmpStatus.getModificationTime();
    } catch (IOException e) {
      LOG.warn(
          "Cannot access File System. File System status will be unknown: ", e);
      unknown = true;
    }

    if (!unknown) {
      for (Path loc : locations) {
        try {
          FileStatus status = fs.getFileStatus(tblPath);
          FileStatus[] files = fs.listStatus(loc);
          long accessTime = status.getAccessTime();
          long updateTime = status.getModificationTime();
          // no matter loc is the table location or part location, it must be a
          // directory.
          if (!status.isDir()) {
            continue;
          }
          if (accessTime > lastAccessTime) {
            lastAccessTime = accessTime;
          }
          if (updateTime > lastUpdateTime) {
            lastUpdateTime = updateTime;
          }
          for (FileStatus currentStatus : files) {
            if (currentStatus.isDir()) {
              continue;
            }
            numOfFiles++;
            long fileLen = currentStatus.getLen();
            totalFileSize += fileLen;
            if (fileLen > maxFileSize) {
              maxFileSize = fileLen;
            }
            if (fileLen < minFileSize) {
              minFileSize = fileLen;
            }
            accessTime = currentStatus.getAccessTime();
            updateTime = currentStatus.getModificationTime();
            if (accessTime > lastAccessTime) {
              lastAccessTime = accessTime;
            }
            if (updateTime > lastUpdateTime) {
              lastUpdateTime = updateTime;
            }
          }
        } catch (IOException e) {
          // ignore
        }
      }
    }

    builder
    .put("totalNumberFiles", numOfFiles, ! unknown)
    .put("totalFileSize",    totalFileSize, ! unknown)
    .put("maxFileSize",      maxFileSize, ! unknown)
    .put("minFileSize",      numOfFiles > 0 ? minFileSize : 0, ! unknown)
    .put("lastAccessTime",   lastAccessTime, ! (unknown  || lastAccessTime < 0))
    .put("lastUpdateTime",   lastUpdateTime, ! unknown);
  }

  /**
   * Show the table partitions.
   */
  @Override
  public void showTablePartitions(DataOutputStream out, List<String> parts)
      throws HiveException {
    asJson(out, MapBuilder.create().put("partitions",
        makeTablePartions(parts)).build());
  }

  private List<Map<String, Object>> makeTablePartions(List<String> parts) {
    ArrayList<Map<String, Object>> res =
        new ArrayList<Map<String, Object>>(parts.size());
    for (String part : parts) {
      res.add(makeOneTablePartition(part));
    }
    return res;
  }

  // This seems like a very wrong implementation.
  private Map<String, Object> makeOneTablePartition(String partIdent) {
    ArrayList<Map<String, Object>> res = new ArrayList<Map<String, Object>>();

    ArrayList<String> names = new ArrayList<String>();
    for (String part : StringUtils.split(partIdent, "/")) {
      String name = part;
      String val = null;
      String[] kv = StringUtils.split(part, "=", 2);
      if (kv != null) {
        name = kv[0];
        if (kv.length > 1) {
          try {
            val = URLDecoder.decode(kv[1], StandardCharsets.UTF_8.name());
          } catch (UnsupportedEncodingException e) {
          }
        }
      }
      if (val != null) {
        names.add(name + "='" + val + "'");
      }
      else {
        names.add(name);
      }

      res.add(MapBuilder.create()
          .put("columnName", name)
          .put("columnValue", val)
          .build());
    }

    return MapBuilder.create()
        .put("name", StringUtils.join(names, ","))
        .put("values", res)
        .build();
  }

  /**
   * Show a list of databases
   */
  @Override
  public void showDatabases(DataOutputStream out, List<String> databases)
      throws HiveException {
    asJson(out, MapBuilder.create().put("databases", databases).build());
  }

  /**
   * Show the description of a database
   */
  @Override
  public void showDatabaseDescription(DataOutputStream out, String database, String comment,
      String location, String ownerName, PrincipalType ownerType, Map<String, String> params)
          throws HiveException {
    MapBuilder builder = MapBuilder.create().put("database", database).put("comment", comment)
        .put("location", location);
    if (null != ownerName) {
      builder.put("owner", ownerName);
    }
    if (null != ownerType) {
      builder.put("ownerType", ownerType.name());
    }
    if (null != params && !params.isEmpty()) {
      builder.put("params", params);
    }
    asJson(out, builder.build());
  }

  @Override
  public void showResourcePlans(DataOutputStream out, List<WMResourcePlan> resourcePlans)
      throws HiveException {
    JsonGenerator generator = null;
    try {
      generator = new ObjectMapper().getJsonFactory().createJsonGenerator(out);
      generator.writeStartArray();
      for (WMResourcePlan plan : resourcePlans) {
        generator.writeStartObject();
        generator.writeStringField("name", plan.getName());
        generator.writeStringField("status", plan.getStatus().name());
        if (plan.isSetQueryParallelism()) {
          generator.writeNumberField("queryParallelism", plan.getQueryParallelism());
        }
        if (plan.isSetDefaultPoolPath()) {
          generator.writeStringField("defaultPoolPath", plan.getDefaultPoolPath());
        }
        generator.writeEndObject();
      }
      generator.writeEndArray();
      generator.close();
    } catch (IOException e) {
      throw new HiveException(e);
    } finally {
      if (generator != null) {
        IOUtils.closeQuietly(generator);
      }
    }
  }

  /**
   * Formats a resource plan into a json object, the structure is as follows:
   * {
   *    name: "<rp_name>",
   *    parallelism: "<parallelism>",
   *    defaultQueue: "<defaultQueue>",
   *    pools : [
   *      {
   *        name: "<pool_name>",
   *        parallelism: "<parallelism>",
   *        schedulingPolicy: "<policy>",
   *        triggers: [
   *          { name: "<triggerName>", trigger: "<trigExpression>", action: "<actionExpr">}
   *          ...
   *        ]
   *      }
   *      ...
   *    ]
   * }
   */
  private static class JsonRPFormatter implements MetaDataFormatUtils.RPFormatter, Closeable {
    private final JsonGenerator generator;

    JsonRPFormatter(DataOutputStream out) throws IOException {
      generator = new ObjectMapper().getJsonFactory().createJsonGenerator(out);
    }

    private void writeNameAndFields(String name, Object ... kvPairs) throws IOException {
      if (kvPairs.length % 2 != 0) {
        throw new IllegalArgumentException("Expected pairs");
      }
      generator.writeStringField("name", name);
      for (int i = 0; i < kvPairs.length; i += 2) {
        generator.writeObjectField(kvPairs[i].toString(), kvPairs[i + 1]);
      }
    }

    @Override
    public void startRP(String rpName, Object ... kvPairs) throws IOException {
      generator.writeStartObject();
      writeNameAndFields(rpName, kvPairs);
    }

    @Override
    public void endRP() throws IOException {
      // End the root rp object.
      generator.writeEndObject();
    }

    @Override
    public void startPools() throws IOException {
      generator.writeArrayFieldStart("pools");
    }

    @Override
    public void endPools() throws IOException {
      // End the pools array.
      generator.writeEndArray();
    }

    @Override
    public void startPool(String poolName, Object ... kvPairs) throws IOException {
      generator.writeStartObject();
      writeNameAndFields(poolName, kvPairs);
    }

    @Override
    public void startTriggers() throws IOException {
      generator.writeArrayFieldStart("triggers");
    }

    @Override
    public void endTriggers() throws IOException {
      generator.writeEndArray();
    }

    @Override
    public void startMappings() throws IOException {
      generator.writeArrayFieldStart("mappings");
    }

    @Override
    public void endMappings() throws IOException {
      generator.writeEndArray();
    }

    @Override
    public void endPool() throws IOException {
      generator.writeEndObject();
    }

    @Override
    public void formatTrigger(String triggerName, String actionExpression,
        String triggerExpression) throws IOException {
      generator.writeStartObject();
      writeNameAndFields(triggerName, "action", actionExpression, "trigger", triggerExpression);
      generator.writeEndObject();
    }

    @Override
    public void formatMappingType(String type, List<String> names) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeArrayFieldStart("values");
      for (String name : names) {
        generator.writeString(name);
      }
      generator.writeEndArray();
      generator.writeEndObject();
    }

    @Override
    public void close() throws IOException {
      generator.close();
    }
  }

  public void showFullResourcePlan(DataOutputStream out, WMFullResourcePlan resourcePlan)
      throws HiveException {
    try (JsonRPFormatter formatter = new JsonRPFormatter(out)) {
      MetaDataFormatUtils.formatFullRP(formatter, resourcePlan);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void showErrors(DataOutputStream out, WMValidateResourcePlanResponse response)
      throws HiveException {
    JsonGenerator generator = null;
    try {
      generator = new ObjectMapper().getJsonFactory().createJsonGenerator(out);
      generator.writeStartObject();

      generator.writeArrayFieldStart("errors");
      for (String error : response.getErrors()) {
        generator.writeString(error);
      }
      generator.writeEndArray();

      generator.writeArrayFieldStart("warnings");
      for (String error : response.getWarnings()) {
        generator.writeString(error);
      }
      generator.writeEndArray();

      generator.writeEndObject();
    } catch (IOException e) {
      throw new HiveException(e);
    } finally {
      if (generator != null) {
        IOUtils.closeQuietly(generator);
      }
    }
  }
}
