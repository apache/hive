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

package org.apache.hadoop.hive.ql.stats;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ColumnStatsList;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.NDV;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.AnnotationUtils;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class StatsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StatsUtils.class.getName());


  /**
   * Collect table, partition and column level statistics
   * @param conf
   *          - hive configuration
   * @param partList
   *          - partition list
   * @param table
   *          - table
   * @param tableScanOperator
   *          - table scan operator
   * @return statistics object
   * @throws HiveException
   */
  public static Statistics collectStatistics(HiveConf conf, PrunedPartitionList partList, ColumnStatsList colStatsCache,
      Table table, TableScanOperator tableScanOperator) throws HiveException {

    // column level statistics are required only for the columns that are needed
    List<ColumnInfo> schema = tableScanOperator.getSchema().getSignature();
    List<String> neededColumns = tableScanOperator.getNeededColumns();
    List<String> referencedColumns = tableScanOperator.getReferencedColumns();

    return collectStatistics(conf, partList, table, schema, neededColumns, colStatsCache, referencedColumns);
  }

  private static Statistics collectStatistics(HiveConf conf, PrunedPartitionList partList,
      Table table, List<ColumnInfo> schema, List<String> neededColumns, ColumnStatsList colStatsCache,
      List<String> referencedColumns) throws HiveException {

    boolean fetchColStats =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_FETCH_COLUMN_STATS);
    boolean testMode =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST);

    return collectStatistics(conf, partList, table, schema, neededColumns, colStatsCache, referencedColumns,
        fetchColStats, testMode);
  }

  private static long getDataSize(HiveConf conf, Table table) {
    long ds = getRawDataSize(table);
    if (ds <= 0) {
      ds = getTotalSize(table);

      // if data size is still 0 then get file size
      if (ds <= 0) {
        ds = getFileSizeForTable(conf, table);
      }
      float deserFactor =
          HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR);
      ds = (long) (ds * deserFactor);
    }

    return ds;
  }

  /**
   * Returns number of rows if it exists. Otherwise it estimates number of rows
   * based on estimated data size for both partition and non-partitioned table
   * RelOptHiveTable's getRowCount uses this.
   *
   * @param conf
   * @param schema
   * @param table
   * @return
   */
  public static long getNumRows(HiveConf conf, List<ColumnInfo> schema, Table table,
                                PrunedPartitionList partitionList, AtomicInteger noColsMissingStats) {

    boolean shouldEstimateStats = HiveConf.getBoolVar(conf, ConfVars.HIVE_STATS_ESTIMATE_STATS);

    if(!table.isPartitioned()) {
      //get actual number of rows from metastore
      long nr = getNumRows(table);

      // log warning if row count is missing
      if(nr <= 0) {
        noColsMissingStats.getAndIncrement();
      }

      // if row count exists or stats aren't to be estimated return
      // whatever we have
      if(nr > 0 || !shouldEstimateStats) {
        return nr;
      }
      // go ahead with the estimation
      long ds = getDataSize(conf, table);
      return getNumRows(conf, schema, table, ds);
    }
    else { // partitioned table
      long nr = 0;
      List<Long> rowCounts = Lists.newArrayList();
      rowCounts = getBasicStatForPartitions(
          table, partitionList.getNotDeniedPartns(), StatsSetupConst.ROW_COUNT);
      nr = getSumIgnoreNegatives(rowCounts);

      // log warning if row count is missing
      if(nr <= 0) {
        noColsMissingStats.getAndIncrement();
      }

      // if row count exists or stats aren't to be estimated return
      // whatever we have
      if(nr > 0 || !shouldEstimateStats) {
        return nr;
      }

      // estimate row count
      long ds = 0;
      List<Long> dataSizes = Lists.newArrayList();

      dataSizes =  getBasicStatForPartitions(
          table, partitionList.getNotDeniedPartns(), StatsSetupConst.RAW_DATA_SIZE);

      ds = getSumIgnoreNegatives(dataSizes);

      float deserFactor = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR);

      if (ds <= 0) {
        dataSizes = getBasicStatForPartitions(
            table, partitionList.getNotDeniedPartns(), StatsSetupConst.TOTAL_SIZE);
        dataSizes = safeMult(dataSizes, deserFactor);
        ds = getSumIgnoreNegatives(dataSizes);
      }

      // if data size still could not be determined, then fall back to filesytem to get file
      // sizes
      if (ds <= 0 && shouldEstimateStats) {
        dataSizes = getFileSizeForPartitions(conf, partitionList.getNotDeniedPartns());
        dataSizes = safeMult(dataSizes, deserFactor);
        ds = getSumIgnoreNegatives(dataSizes);
      }

      int avgRowSize = estimateRowSizeFromSchema(conf, schema);
      if (avgRowSize > 0) {
        setUnknownRcDsToAverage(rowCounts, dataSizes, avgRowSize);
        nr = getSumIgnoreNegatives(rowCounts);
        ds = getSumIgnoreNegatives(dataSizes);

        // number of rows -1 means that statistics from metastore is not reliable
        if (nr <= 0) {
          nr = ds / avgRowSize;
        }
      }
      if (nr == 0) {
        nr = 1;
      }
      return nr;
    }
  }

  private static void estimateStatsForMissingCols(List<String> neededColumns, List<ColStatistics> columnStats,
                                           Table table, HiveConf conf, long nr, List<ColumnInfo> schema) {

    Set<String> neededCols = new HashSet<>(neededColumns);
    Set<String> colsWithStats = new HashSet<>();

    for (ColStatistics cstats : columnStats) {
      colsWithStats.add(cstats.getColumnName());
    }

    List<String> missingColStats = new ArrayList<String>(Sets.difference(neededCols, colsWithStats));

    if(missingColStats.size() > 0) {
      List<ColStatistics> estimatedColStats = estimateStats(table, schema, missingColStats, conf, nr);
      for (ColStatistics estColStats : estimatedColStats) {
        columnStats.add(estColStats);
      }
    }
  }

  private static long getNumRows(HiveConf conf, List<ColumnInfo> schema, Table table, long ds) {
    long nr = getNumRows(table);
    // number of rows -1 means that statistics from metastore is not reliable
    // and 0 means statistics gathering is disabled
    // estimate only if num rows is -1 since 0 could be actual number of rows
    if (nr < 0) {
      int avgRowSize = estimateRowSizeFromSchema(conf, schema);
      if (avgRowSize > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Estimated average row size: " + avgRowSize);
        }
        nr = ds / avgRowSize;
      }
    }
    if(nr == 0 || nr == -1) {
      return 1;
    }
    return nr;
  }

  public static Statistics collectStatistics(HiveConf conf, PrunedPartitionList partList,
      Table table, List<ColumnInfo> schema, List<String> neededColumns, ColumnStatsList colStatsCache,
      List<String> referencedColumns, boolean fetchColStats)
      throws HiveException {
    return collectStatistics(conf, partList, table, schema, neededColumns, colStatsCache,
        referencedColumns, fetchColStats, false);
  }

  private static Statistics collectStatistics(HiveConf conf, PrunedPartitionList partList,
      Table table, List<ColumnInfo> schema, List<String> neededColumns, ColumnStatsList colStatsCache,
      List<String> referencedColumns, boolean fetchColStats, boolean failIfCacheMiss)
      throws HiveException {

    Statistics stats = null;

    float deserFactor =
        HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR);
    boolean shouldEstimateStats = HiveConf.getBoolVar(conf, ConfVars.HIVE_STATS_ESTIMATE_STATS);

    if (!table.isPartitioned()) {

      //getDataSize tries to estimate stats if it doesn't exist using file size
      // we would like to avoid file system calls  if it too expensive
      long ds = shouldEstimateStats? getDataSize(conf, table): getRawDataSize(table);
      long nr = getNumRows(conf, schema, table, ds);
      List<ColStatistics> colStats = Lists.newArrayList();
      if (fetchColStats) {
        colStats = getTableColumnStats(table, schema, neededColumns, colStatsCache);
        if(colStats == null) {
          colStats = Lists.newArrayList();
        }
        estimateStatsForMissingCols(neededColumns, colStats, table, conf, nr, schema);

        // we should have stats for all columns (estimated or actual)
        assert(neededColumns.size() == colStats.size());
        long betterDS = getDataSizeFromColumnStats(nr, colStats);
        ds = (betterDS < 1 || colStats.isEmpty()) ? ds : betterDS;
      }
      stats = new Statistics(nr, ds);
      // infer if any column can be primary key based on column statistics
      inferAndSetPrimaryKey(stats.getNumRows(), colStats);

      stats.setColumnStatsState(deriveStatType(colStats, neededColumns));
      stats.addToColumnStats(colStats);
    } else if (partList != null) {
      // For partitioned tables, get the size of all the partitions after pruning
      // the partitions that are not required
      long nr = 0;
      long ds = 0;

      List<Long> rowCounts = Lists.newArrayList();
      List<Long> dataSizes = Lists.newArrayList();

      rowCounts = getBasicStatForPartitions(table, partList.getNotDeniedPartns(), StatsSetupConst.ROW_COUNT);
      dataSizes = getBasicStatForPartitions(table, partList.getNotDeniedPartns(), StatsSetupConst.RAW_DATA_SIZE);

      nr = getSumIgnoreNegatives(rowCounts);
      ds = getSumIgnoreNegatives(dataSizes);
      if (ds <= 0) {
        dataSizes = getBasicStatForPartitions(table, partList.getNotDeniedPartns(), StatsSetupConst.TOTAL_SIZE);
        dataSizes = safeMult(dataSizes, deserFactor);
        ds = getSumIgnoreNegatives(dataSizes);
      }

      // if data size still could not be determined, then fall back to filesytem to get file
      // sizes
      if (ds <= 0 && shouldEstimateStats) {
        dataSizes = getFileSizeForPartitions(conf, partList.getNotDeniedPartns());
        dataSizes = safeMult(dataSizes, deserFactor);
        ds = getSumIgnoreNegatives(dataSizes);
      }

      int avgRowSize = estimateRowSizeFromSchema(conf, schema);
      if (avgRowSize > 0) {
        setUnknownRcDsToAverage(rowCounts, dataSizes, avgRowSize);
        nr = getSumIgnoreNegatives(rowCounts);
        ds = getSumIgnoreNegatives(dataSizes);

        // number of rows -1 means that statistics from metastore is not reliable
        if (nr <= 0) {
          nr = ds / avgRowSize;
        }
      }

      // Minimum values
      if (nr == 0) {
        nr = 1;
      }
      stats = new Statistics(nr, ds);

      // if at least a partition does not contain row count then mark basic stats state as PARTIAL
      if (containsNonPositives(rowCounts) &&
          stats.getBasicStatsState().equals(State.COMPLETE)) {
        stats.setBasicStatsState(State.PARTIAL);
      }
      if (fetchColStats) {
        List<String> partitionCols = getPartitionColumns(
            schema, neededColumns, referencedColumns);

        // We will retrieve stats from the metastore only for columns that are not cached
        List<String> neededColsToRetrieve;
        List<String> partitionColsToRetrieve;
        List<ColStatistics> columnStats = new ArrayList<>();
        if (colStatsCache != null) {
          neededColsToRetrieve = new ArrayList<String>(neededColumns.size());
          for (String colName : neededColumns) {
            ColStatistics colStats = colStatsCache.getColStats().get(colName);
            if (colStats == null) {
              neededColsToRetrieve.add(colName);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Stats for column " + colName +
                    " in table " + table.getCompleteName() + " could not be retrieved from cache");
              }
            } else {
              columnStats.add(colStats);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Stats for column " + colName +
                    " in table " + table.getCompleteName() + " retrieved from cache");
              }
            }
          }
          partitionColsToRetrieve = new ArrayList<>(partitionCols.size());
          for (String colName : partitionCols) {
            ColStatistics colStats = colStatsCache.getColStats().get(colName);
            if (colStats == null) {
              partitionColsToRetrieve.add(colName);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Stats for column " + colName +
                    " in table " + table.getCompleteName() + " could not be retrieved from cache");
              }
            } else {
              columnStats.add(colStats);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Stats for column " + colName +
                    " in table " + table.getCompleteName() + " retrieved from cache");
              }
            }
          }
        } else {
          neededColsToRetrieve = neededColumns;
          partitionColsToRetrieve = partitionCols;
        }

        // List of partitions
        List<String> partNames = new ArrayList<String>(partList.getNotDeniedPartns().size());
        for (Partition part : partList.getNotDeniedPartns()) {
          partNames.add(part.getName());
        }

        AggrStats aggrStats = null;
        // We check the sizes of neededColumns and partNames here. If either
        // size is 0, aggrStats is null after several retries. Thus, we can
        // skip the step to connect to the metastore.
        if (neededColsToRetrieve.size() > 0 && partNames.size() > 0) {
          aggrStats = Hive.get().getAggrColStatsFor(table.getDbName(), table.getTableName(),
              neededColsToRetrieve, partNames);
        }

        boolean statsRetrieved = aggrStats != null &&
            aggrStats.getColStats() != null && aggrStats.getColStatsSize() != 0;
        if (neededColumns.size() == 0 ||
            (neededColsToRetrieve.size() > 0 && !statsRetrieved)) {
          estimateStatsForMissingCols(neededColsToRetrieve, columnStats, table, conf, nr, schema);
          // There are some partitions with no state (or we didn't fetch any state).
          // Update the stats with empty list to reflect that in the
          // state/initialize structures.

          // add partition column stats
          addPartitionColumnStats(conf, partitionColsToRetrieve, schema, table, partList, columnStats);

          // FIXME: this add seems suspicious...10 lines below the value returned by this method used as betterDS
          stats.addToDataSize(getDataSizeFromColumnStats(nr, columnStats));
          stats.updateColumnStatsState(deriveStatType(columnStats, referencedColumns));

          stats.addToColumnStats(columnStats);
        } else {
          if (statsRetrieved) {
            columnStats.addAll(convertColStats(aggrStats.getColStats(), table.getTableName()));
          }
          int colStatsAvailable = neededColumns.size() + partitionCols.size() - partitionColsToRetrieve.size();
          if (columnStats.size() != colStatsAvailable) {
            LOG.debug("Column stats requested for : {} columns. Able to retrieve for {} columns",
                    columnStats.size(), colStatsAvailable);
          }

          addPartitionColumnStats(conf, partitionColsToRetrieve, schema, table, partList, columnStats);
          long betterDS = getDataSizeFromColumnStats(nr, columnStats);
          stats.setDataSize((betterDS < 1 || columnStats.isEmpty()) ? ds : betterDS);
          // infer if any column can be primary key based on column statistics
          inferAndSetPrimaryKey(stats.getNumRows(), columnStats);

          stats.addToColumnStats(columnStats);

          // Infer column stats state
          stats.setColumnStatsState(deriveStatType(columnStats, referencedColumns));
          if (neededColumns.size() != neededColsToRetrieve.size() ||
              partitionCols.size() != partitionColsToRetrieve.size()) {
            // Include state for cached columns
            stats.updateColumnStatsState(colStatsCache.getState());
          }
          // Change if we could not retrieve for all partitions
          if (aggrStats != null && aggrStats.getPartsFound() != partNames.size() && stats.getColumnStatsState() != State.NONE) {
            stats.updateColumnStatsState(State.PARTIAL);
            LOG.debug("Column stats requested for : {} partitions. Able to retrieve for {} partitions",
                    partNames.size(), aggrStats.getPartsFound());
          }
        }

        if(rowCounts.size() == 0 ) {
          // all partitions are filtered by partition pruning
          stats.setBasicStatsState(State.COMPLETE);
        }

        // This block exists for debugging purposes: we want to check whether
        // the col stats cache is working properly and we are retrieving the
        // stats from metastore only once.
        if (colStatsCache != null && failIfCacheMiss &&
            stats.getColumnStatsState().equals(State.COMPLETE) &&
            (!neededColsToRetrieve.isEmpty() || !partitionColsToRetrieve.isEmpty())) {
          throw new HiveException("Cache has been loaded in logical planning phase for all columns; "
              + "however, stats for column some columns could not be retrieved from it "
              + "(see messages above)");
        }
      }
    }
    return stats;
  }


  /**
   * Based on the provided column statistics and number of rows, this method infers if the column
   * can be primary key. It checks if the difference between the min and max value is equal to
   * number of rows specified.
   * @param numRows - number of rows
   * @param colStats - column statistics
   */
  public static void inferAndSetPrimaryKey(long numRows, List<ColStatistics> colStats) {
    if (colStats != null) {
      for (ColStatistics cs : colStats) {
        if (cs != null && cs.getCountDistint() >= numRows) {
          cs.setPrimaryKey(true);
        }
        else if (cs != null && cs.getRange() != null && cs.getRange().minValue != null &&
            cs.getRange().maxValue != null) {
          if (numRows ==
              ((cs.getRange().maxValue.longValue() - cs.getRange().minValue.longValue()) + 1)) {
            cs.setPrimaryKey(true);
          }
        }
      }
    }
  }

  /**
   * Infer foreign key relationship from given column statistics.
   * @param csPK - column statistics of primary key
   * @param csFK - column statistics of potential foreign key
   * @return
   */
  public static boolean inferForeignKey(ColStatistics csPK, ColStatistics csFK) {
    if (csPK != null && csFK != null) {
      if (csPK.isPrimaryKey()) {
        if (csPK.getRange() != null && csFK.getRange() != null) {
          ColStatistics.Range pkRange = csPK.getRange();
          ColStatistics.Range fkRange = csFK.getRange();
          return isWithin(fkRange, pkRange);
        }
      }
    }
    return false;
  }

  /**
   * Scale selectivity based on key range ratio.
   * @param csPK - column statistics of primary key
   * @param csFK - column statistics of potential foreign key
   * @return
   */
  public static float getScaledSelectivity(ColStatistics csPK, ColStatistics csFK) {
    float scaledSelectivity = 1.0f;
    if (csPK != null && csFK != null) {
      if (csPK.isPrimaryKey()) {
        // Use Max-Min Range as NDV gets scaled by selectivity.
        if (csPK.getRange() != null && csFK.getRange() != null) {
          long pkRangeDelta = getRangeDelta(csPK.getRange());
          long fkRangeDelta = getRangeDelta(csFK.getRange());
          if (fkRangeDelta > 0 && pkRangeDelta > 0 && fkRangeDelta < pkRangeDelta) {
            scaledSelectivity = (float) pkRangeDelta / (float) fkRangeDelta;
          }
        }
      }
    }
    return scaledSelectivity;
  }

  public static long getRangeDelta(ColStatistics.Range range) {
    if (range.minValue != null && range.maxValue != null) {
      return (range.maxValue.longValue() - range.minValue.longValue());
    }
    return 0;
  }

  private static boolean isWithin(ColStatistics.Range range1, ColStatistics.Range range2) {
    if (range1.minValue != null && range2.minValue != null && range1.maxValue != null &&
        range2.maxValue != null) {
      if (range1.minValue.longValue() >= range2.minValue.longValue() &&
          range1.maxValue.longValue() <= range2.maxValue.longValue()) {
        return true;
      }
    }
    return false;
  }

  private static void addPartitionColumnStats(HiveConf conf, List<String> partitionCols,
      List<ColumnInfo> schema, Table table, PrunedPartitionList partList, List<ColStatistics> colStats)
          throws HiveException {
    for (String col : partitionCols) {
      for (ColumnInfo ci : schema) {
        // conditions for being partition column
        if (col.equals(ci.getInternalName())) {
          colStats.add(getColStatsForPartCol(ci, new PartitionIterable(partList.getPartitions()), conf));
        }
      }
    }
  }

  private static List<String> getPartitionColumns(List<ColumnInfo> schema,
      List<String> neededColumns,
      List<String> referencedColumns) {
    // extra columns is difference between referenced columns vs needed
    // columns. The difference could be partition columns.
    List<String> partitionCols = new ArrayList<>(referencedColumns.size());
    List<String> extraCols = Lists.newArrayList(referencedColumns);
    if (referencedColumns.size() > neededColumns.size()) {
      extraCols.removeAll(neededColumns);
      for (String col : extraCols) {
        for (ColumnInfo ci : schema) {
          // conditions for being partition column
          if (col.equals(ci.getInternalName()) && ci.getIsVirtualCol() &&
              !ci.isHiddenVirtualCol()) {
            partitionCols.add(col);
          }
        }
      }
    }
    return partitionCols;
  }

  public static ColStatistics getColStatsForPartCol(ColumnInfo ci,PartitionIterable partList, HiveConf conf) {
    // currently metastore does not store column stats for
    // partition column, so we calculate the NDV from partition list
    ColStatistics partCS = new ColStatistics(ci.getInternalName(), ci.getType()
        .getTypeName());
    long numPartitions = getNDVPartitionColumn(partList,
        ci.getInternalName());
    partCS.setCountDistint(numPartitions);
    partCS.setAvgColLen(StatsUtils.getAvgColLenOf(conf,
        ci.getObjectInspector(), partCS.getColumnType()));
    partCS.setRange(getRangePartitionColumn(partList, ci.getInternalName(),
        ci.getType().getTypeName(), conf.getVar(ConfVars.DEFAULTPARTITIONNAME)));
    return partCS;
  }

  public static int getNDVPartitionColumn(PartitionIterable partitions, String partColName) {
    Set<String> distinctVals = new HashSet<String>();
    for (Partition partition : partitions) {
      distinctVals.add(partition.getSpec().get(partColName));
    }
    return distinctVals.size();
  }

  private static Range getRangePartitionColumn(PartitionIterable partitions, String partColName,
      String colType, String defaultPartName) {
    Range range = null;
    String partVal;
    String colTypeLowerCase = colType.toLowerCase();
    if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (Partition partition : partitions) {
        partVal = partition.getSpec().get(partColName);
        if (partVal.equals(defaultPartName)) {
          // partition column value is null.
          continue;
        } else {
          long value = Long.parseLong(partVal);
          min = Math.min(min, value);
          max = Math.max(max, value);
        }
      }
      range = new Range(min, max);
    } else if (colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
      for (Partition partition : partitions) {
        partVal = partition.getSpec().get(partColName);
        if (partVal.equals(defaultPartName)) {
          // partition column value is null.
          continue;
        } else {
          double value = Double.parseDouble(partVal);
          min = Math.min(min, value);
          max = Math.max(max, value);
        }
      }
      range = new Range(min, max);
    } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
      for (Partition partition : partitions) {
        partVal = partition.getSpec().get(partColName);
        if (partVal.equals(defaultPartName)) {
          // partition column value is null.
          continue;
        } else {
          double value = new BigDecimal(partVal).doubleValue();
          min = Math.min(min, value);
          max = Math.max(max, value);
        }
      }
      range = new Range(min, max);
    } else {
      // Columns statistics for complex datatypes are not supported yet
      return null;
    }
    return range;
  }

  private static void setUnknownRcDsToAverage(
      List<Long> rowCounts, List<Long> dataSizes, int avgRowSize) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Estimated average row size: " + avgRowSize);
    }
    for (int i = 0; i < rowCounts.size(); i++) {
      long rc = rowCounts.get(i);
      long s = dataSizes.get(i);
      if (rc <= 0 && s > 0) {
        rc = s / avgRowSize;
        rowCounts.set(i, rc);
      }

      if (s <= 0 && rc > 0) {
        s = safeMult(rc, avgRowSize);
        dataSizes.set(i, s);
      }
    }
  }

  public static int estimateRowSizeFromSchema(HiveConf conf, List<ColumnInfo> schema) {
    List<String> neededColumns = new ArrayList<>();
    for (ColumnInfo ci : schema) {
      neededColumns.add(ci.getInternalName());
    }
    return estimateRowSizeFromSchema(conf, schema, neededColumns);
  }

  public static int estimateRowSizeFromSchema(HiveConf conf, List<ColumnInfo> schema,
      List<String> neededColumns) {
    int avgRowSize = 0;
    for (String neededCol : neededColumns) {
      ColumnInfo ci = getColumnInfoForColumn(neededCol, schema);
      if (ci == null) {
        // No need to collect statistics of index columns
        continue;
      }
      ObjectInspector oi = ci.getObjectInspector();
      String colTypeLowerCase = ci.getTypeName().toLowerCase();
      if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
          || colTypeLowerCase.equals(serdeConstants.BINARY_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.LIST_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.MAP_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.STRUCT_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.UNION_TYPE_NAME)) {
        avgRowSize += getAvgColLenOf(conf, oi, colTypeLowerCase);
      } else {
        avgRowSize += getAvgColLenOfFixedLengthTypes(colTypeLowerCase);
      }
    }
    return avgRowSize;
  }

  private static ColumnInfo getColumnInfoForColumn(String neededCol, List<ColumnInfo> schema) {
    for (ColumnInfo ci : schema) {
      if (ci.getInternalName().equalsIgnoreCase(neededCol)) {
        return ci;
      }
    }
    return null;
  }

  /**
   * Find the bytes on disk occupied by a table
   * @param conf
   *          - hive conf
   * @param table
   *          - table
   * @return size on disk
   */
  public static long getFileSizeForTable(HiveConf conf, Table table) {
    Path path = table.getPath();
    long size = 0;
    try {
      FileSystem fs = path.getFileSystem(conf);
      size = fs.getContentSummary(path).getLength();
    } catch (Exception e) {
      size = 0;
    }
    return size;
  }

  /**
   * Find the bytes on disks occupied by list of partitions
   * @param conf
   *          - hive conf
   * @param parts
   *          - partition list
   * @return sizes of partitions
   */
  public static List<Long> getFileSizeForPartitions(final HiveConf conf, List<Partition> parts) {
    LOG.info("Number of partitions : " + parts.size());
    ArrayList<Future<Long>> futures = new ArrayList<>();

    int threads = Math.max(1, conf.getIntVar(ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT));
    final ExecutorService pool = Executors.newFixedThreadPool(threads,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("Get-Partitions-Size-%d")
                    .build());

    final ArrayList<Long> sizes = new ArrayList<>(parts.size());
    for (final Partition part : parts) {
      final Path path = part.getDataLocation();
      futures.add(pool.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          try {
            LOG.debug("Partition path : " + path);
            FileSystem fs = path.getFileSystem(conf);
            return fs.getContentSummary(path).getLength();
          } catch (IOException e) {
            return 0L;
          }
        }
      }));
    }

    try {
      for(int i = 0; i < futures.size(); i++) {
        sizes.add(i, futures.get(i).get());
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.warn("Exception in processing files ", e);
    } finally {
      pool.shutdownNow();
    }
    return sizes;
  }

  private static boolean containsNonPositives(List<Long> vals) {
    for (Long val : vals) {
      if (val <= 0L) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get sum of all values in the list that are >0
   * @param vals
   *          - list of values
   * @return sum
   */
  public static long getSumIgnoreNegatives(List<Long> vals) {
    long result = 0;
    for (Long l : vals) {
      if (l > 0) {
        result = safeAdd(result, l);
      }
    }
    return result;
  }

  private static Statistics.State deriveStatType(
      List<ColStatistics> colStats, List<String> neededColumns) {
    boolean hasStats = false,
        hasNull = (colStats == null) || (colStats.size() < neededColumns.size());
    if (colStats != null) {
      for (ColStatistics cs : colStats) {
        // either colstats is null or is estimated
        boolean isNull = (cs == null) ? true: (cs.isEstimated());
        hasStats |= !isNull;
        hasNull |= isNull;
        if (hasNull && hasStats) {
          break;
        }
      }
    }
    State result = (hasStats
        ? (hasNull ? Statistics.State.PARTIAL : Statistics.State.COMPLETE)
        : (neededColumns.isEmpty() ? Statistics.State.COMPLETE : Statistics.State.NONE));
    return result;
  }

  /**
   * Convert ColumnStatisticsObj to ColStatistics
   * @param cso
   *          - ColumnStatisticsObj
   * @param tabName
   *          - table name
   * @param colName
   *          - column name
   * @return ColStatistics
   */
  public static ColStatistics getColStatistics(ColumnStatisticsObj cso, String tabName,
      String colName) {
    String colTypeLowerCase = cso.getColType().toLowerCase();
    ColStatistics cs = new ColStatistics(colName, colTypeLowerCase);
    ColumnStatisticsData csd = cso.getStatsData();
    if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME)) {
      cs.setCountDistint(csd.getLongStats().getNumDVs());
      cs.setNumNulls(csd.getLongStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive1());
      cs.setRange(csd.getLongStats().getLowValue(), csd.getLongStats().getHighValue());
    } else if (colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      cs.setCountDistint(csd.getLongStats().getNumDVs());
      cs.setNumNulls(csd.getLongStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive2());
      cs.setRange(csd.getLongStats().getLowValue(), csd.getLongStats().getHighValue());
    } else if (colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)) {
      cs.setCountDistint(csd.getDoubleStats().getNumDVs());
      cs.setNumNulls(csd.getDoubleStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive1());
      cs.setRange(csd.getDoubleStats().getLowValue(), csd.getDoubleStats().getHighValue());
    } else if (colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      cs.setCountDistint(csd.getDoubleStats().getNumDVs());
      cs.setNumNulls(csd.getDoubleStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive2());
      cs.setRange(csd.getDoubleStats().getLowValue(), csd.getDoubleStats().getHighValue());
    } else if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
      cs.setCountDistint(csd.getStringStats().getNumDVs());
      cs.setNumNulls(csd.getStringStats().getNumNulls());
      cs.setAvgColLen(csd.getStringStats().getAvgColLen());
    } else if (colTypeLowerCase.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      if (csd.getBooleanStats().getNumFalses() > 0 && csd.getBooleanStats().getNumTrues() > 0) {
        cs.setCountDistint(2);
      } else {
        cs.setCountDistint(1);
      }
      cs.setNumTrues(csd.getBooleanStats().getNumTrues());
      cs.setNumFalses(csd.getBooleanStats().getNumFalses());
      cs.setNumNulls(csd.getBooleanStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive1());
    } else if (colTypeLowerCase.equals(serdeConstants.BINARY_TYPE_NAME)) {
      cs.setAvgColLen(csd.getBinaryStats().getAvgColLen());
      cs.setNumNulls(csd.getBinaryStats().getNumNulls());
    } else if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfTimestamp());
    } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfDecimal());
      cs.setCountDistint(csd.getDecimalStats().getNumDVs());
      cs.setNumNulls(csd.getDecimalStats().getNumNulls());
      Decimal highValue = csd.getDecimalStats().getHighValue();
      Decimal lowValue = csd.getDecimalStats().getLowValue();
      if (highValue != null && highValue.getUnscaled() != null
          && lowValue != null && lowValue.getUnscaled() != null) {
        HiveDecimal maxHiveDec = HiveDecimal.create(new BigInteger(highValue.getUnscaled()), highValue.getScale());
        BigDecimal maxVal = maxHiveDec == null ? null : maxHiveDec.bigDecimalValue();
        HiveDecimal minHiveDec = HiveDecimal.create(new BigInteger(lowValue.getUnscaled()), lowValue.getScale());
        BigDecimal minVal = minHiveDec == null ? null : minHiveDec.bigDecimalValue();

        if (minVal != null && maxVal != null) {
          cs.setRange(minVal, maxVal);
        }
      }
    } else if (colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfDate());
      cs.setNumNulls(csd.getDateStats().getNumNulls());
      Long lowVal = (csd.getDateStats().getLowValue() != null) ? csd.getDateStats().getLowValue()
          .getDaysSinceEpoch() : null;
      Long highVal = (csd.getDateStats().getHighValue() != null) ? csd.getDateStats().getHighValue()
          .getDaysSinceEpoch() : null;
      cs.setRange(lowVal, highVal);
    } else {
      // Columns statistics for complex datatypes are not supported yet
      return null;
    }

    return cs;
  }

  private static ColStatistics estimateColStats(long numRows, String colName, HiveConf conf,
      List<ColumnInfo> schema) {
    ColumnInfo cinfo = getColumnInfoForColumn(colName, schema);
    ColStatistics cs = new ColStatistics(colName, cinfo.getTypeName());
    cs.setIsEstimated(true);

    String colTypeLowerCase = cinfo.getTypeName().toLowerCase();

    float ndvPercent = Math.min(100L, HiveConf.getFloatVar(conf, ConfVars.HIVE_STATS_NDV_ESTIMATE_PERC));
    float nullPercent = Math.min(100L, HiveConf.getFloatVar(conf, ConfVars.HIVE_STATS_NUM_NULLS_ESTIMATE_PERC));

    cs.setCountDistint(Math.max(1, (long)(numRows * ndvPercent/100.00)));
    cs.setNumNulls(Math.min(numRows, (long)(numRows * nullPercent/100.00)));

    if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)){
      cs.setAvgColLen(JavaDataModel.get().primitive1());
      cs.setRange(-128,127);
    }
    else if(colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)){
      cs.setAvgColLen(JavaDataModel.get().primitive1());
      cs.setRange(-32768, 32767);
    } else if(colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().primitive1());
      cs.setRange(Long.MIN_VALUE, Long.MAX_VALUE);
    } else if (colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().primitive2());
      cs.setRange(Integer.MIN_VALUE, Integer.MAX_VALUE);
    } else if (colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().primitive1());
      cs.setRange(Float.MIN_VALUE, Float.MAX_VALUE);
    } else if (colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().primitive2());
      cs.setRange(Double.MIN_VALUE, Double.MAX_VALUE);
    } else if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.BINARY_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
      cs.setAvgColLen(getAvgColLenOf(conf,cinfo.getObjectInspector(), cinfo.getTypeName()));
    } else if (colTypeLowerCase.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        cs.setCountDistint(2);
        cs.setNumTrues(Math.max(1, (long)numRows/2));
        cs.setNumFalses(Math.max(1, (long)numRows/2));
        cs.setAvgColLen(JavaDataModel.get().primitive1());
    } else if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfTimestamp());
    } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfDecimal());
      cs.setRange(Float.MIN_VALUE, Float.MAX_VALUE);
    } else if (colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfDate());
      // epoch, days since epoch
      cs.setRange(0, 25201);
    } else {
      cs.setAvgColLen(getSizeOfComplexTypes(conf, cinfo.getObjectInspector()));
    }
    return cs;
  }

  private static List<ColStatistics> estimateStats(Table table, List<ColumnInfo> schema,
      List<String> neededColumns, HiveConf conf, long nr) {

    List<ColStatistics> stats = new ArrayList<ColStatistics>(neededColumns.size());

    for (int i = 0; i < neededColumns.size(); i++) {
      ColStatistics cs = estimateColStats(nr, neededColumns.get(i), conf, schema);
      stats.add(cs);
    }
    return stats;
  }

  /**
   * Get table level column statistics from metastore for needed columns
   * @param table
   *          - table
   * @param schema
   *          - output schema
   * @param neededColumns
   *          - list of needed columns
   * @return column statistics
   */
  public static List<ColStatistics> getTableColumnStats(
      Table table, List<ColumnInfo> schema, List<String> neededColumns,
      ColumnStatsList colStatsCache) {
    if (table.isMaterializedTable()) {
      LOG.debug("Materialized table does not contain table statistics");
      return null;
    }
    // We will retrieve stats from the metastore only for columns that are not cached
    List<String> colStatsToRetrieve;
    if (colStatsCache != null) {
      colStatsToRetrieve = new ArrayList<>(neededColumns.size());
      for (String colName : neededColumns) {
        if (!colStatsCache.getColStats().containsKey(colName)) {
          colStatsToRetrieve.add(colName);
        }
      }
    } else {
      colStatsToRetrieve = neededColumns;
    }
    // Retrieve stats from metastore
    String dbName = table.getDbName();
    String tabName = table.getTableName();
    List<ColStatistics> stats = null;
    try {
      List<ColumnStatisticsObj> colStat = Hive.get().getTableColumnStatistics(
          dbName, tabName, colStatsToRetrieve);
      stats = convertColStats(colStat, tabName);
    } catch (HiveException e) {
      LOG.error("Failed to retrieve table statistics: ", e);
      stats = new ArrayList<ColStatistics>();
    }
    // Merge stats from cache with metastore cache
    if (colStatsCache != null) {
      for(String col:neededColumns) {
        ColStatistics cs = colStatsCache.getColStats().get(col);
        if (cs != null) {
          stats.add(cs);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Stats for column " + cs.getColumnName() +
                " in table " + table.getCompleteName() + " retrieved from cache");
          }
        }
      }
    }
    return stats;
  }

  private static List<ColStatistics> convertColStats(List<ColumnStatisticsObj> colStats, String tabName) {
    if (colStats==null) {
      return new ArrayList<ColStatistics>();
    }
    List<ColStatistics> stats = new ArrayList<ColStatistics>(colStats.size());
    for (ColumnStatisticsObj statObj : colStats) {
      ColStatistics cs = getColStatistics(statObj, tabName, statObj.getColName());
      if (cs != null) {
        stats.add(cs);
      }
    }
    return stats;
  }

  /**
   * Get the raw data size of variable length data types
   * @param conf
   *          - hive conf
   * @param oi
   *          - object inspector
   * @param colType
   *          - column type
   * @return raw data size
   */
  public static long getAvgColLenOf(HiveConf conf, ObjectInspector oi,
      String colType) {

    long configVarLen = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAX_VARIABLE_LENGTH);
    String colTypeLowCase = colType.toLowerCase();
    if (colTypeLowCase.equals(serdeConstants.STRING_TYPE_NAME)) {

      // constant string projection Ex: select "hello" from table
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        Object constantValue = coi.getWritableConstantValue();
        return constantValue == null ? 0 : constantValue.toString().length();
      } else if (oi instanceof StringObjectInspector) {

        // some UDFs may emit strings of variable length. like pattern matching
        // UDFs. it's hard to find the length of such UDFs.
        // return the variable length from config
        return configVarLen;
      }
    } else if (colTypeLowCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {

      // constant varchar projection
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        Object constantValue = coi.getWritableConstantValue();
        return constantValue == null ? 0 : constantValue.toString().length();
      } else if (oi instanceof HiveVarcharObjectInspector) {
        VarcharTypeInfo type = (VarcharTypeInfo) ((HiveVarcharObjectInspector) oi).getTypeInfo();
        return type.getLength();
      }
    } else if (colTypeLowCase.startsWith(serdeConstants.CHAR_TYPE_NAME)) {

      // constant char projection
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        Object constantValue = coi.getWritableConstantValue();
        return constantValue == null ? 0 : constantValue.toString().length();
      } else if (oi instanceof HiveCharObjectInspector) {
        CharTypeInfo type = (CharTypeInfo) ((HiveCharObjectInspector) oi).getTypeInfo();
        return type.getLength();
      }
    } else if (colTypeLowCase.equals(serdeConstants.BINARY_TYPE_NAME)) {

      // constant byte arrays
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        BytesWritable constantValue = (BytesWritable)coi.getWritableConstantValue();
        return constantValue == null ? 0 : constantValue.getLength();
      } else if (oi instanceof BinaryObjectInspector) {

        // return the variable length from config
        return configVarLen;
      }
    } else {

      // complex types (map, list, struct, union)
      return getSizeOfComplexTypes(conf, oi);
    }

    throw new IllegalArgumentException("Size requested for unknown type: " + colType + " OI: " + oi.getTypeName());
  }

  /**
   * Get the size of complex data types
   * @return raw data size
   */
  public static long getSizeOfComplexTypes(HiveConf conf, ObjectInspector oi) {
    long result = 0;
    int length = 0;
    int listEntries = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_LIST_NUM_ENTRIES);
    int mapEntries = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAP_NUM_ENTRIES);

    switch (oi.getCategory()) {
    case PRIMITIVE:
      String colTypeLowerCase = oi.getTypeName().toLowerCase();
      if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
          || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)) {
        int avgColLen = (int) getAvgColLenOf(conf, oi, colTypeLowerCase);
        result += JavaDataModel.get().lengthForStringOfLength(avgColLen);
      } else if (colTypeLowerCase.equals(serdeConstants.BINARY_TYPE_NAME)) {
        int avgColLen = (int) getAvgColLenOf(conf, oi, colTypeLowerCase);
        result += JavaDataModel.get().lengthForByteArrayOfSize(avgColLen);
      } else {
        result += getAvgColLenOfFixedLengthTypes(colTypeLowerCase);
      }
      break;
    case LIST:
      if (oi instanceof StandardConstantListObjectInspector) {

        // constant list projection of known length
        StandardConstantListObjectInspector scloi = (StandardConstantListObjectInspector) oi;
        length = scloi.getWritableConstantValue().size();

        // check if list elements are primitive or Objects
        ObjectInspector leoi = scloi.getListElementObjectInspector();
        if (leoi.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
          result += getSizeOfPrimitiveTypeArraysFromType(leoi.getTypeName(), length, conf);
        } else {
          result += JavaDataModel.get().lengthForObjectArrayOfSize(length);
        }
      } else {
        StandardListObjectInspector sloi = (StandardListObjectInspector) oi;

        // list overhead + (configured number of element in list * size of element)
        long elemSize = getSizeOfComplexTypes(conf, sloi.getListElementObjectInspector());
        result += JavaDataModel.get().arrayList() + (listEntries * elemSize);
      }
      break;
    case MAP:
      if (oi instanceof StandardConstantMapObjectInspector) {

        // constant map projection of known length
        StandardConstantMapObjectInspector scmoi = (StandardConstantMapObjectInspector) oi;
        result += getSizeOfMap(scmoi);
      } else {
        StandardMapObjectInspector smoi = (StandardMapObjectInspector) oi;
        result += getSizeOfComplexTypes(conf, smoi.getMapKeyObjectInspector());
        result += getSizeOfComplexTypes(conf, smoi.getMapValueObjectInspector());

        // hash map overhead
        result += JavaDataModel.get().hashMap(mapEntries);
      }
      break;
    case STRUCT:
      if (oi instanceof StandardConstantStructObjectInspector) {
        // constant map projection of known length
        StandardConstantStructObjectInspector scsoi = (StandardConstantStructObjectInspector) oi;
        result += getSizeOfStruct(scsoi);
      }  else {
        StructObjectInspector soi = (StructObjectInspector) oi;

        // add constant object overhead for struct
        result += JavaDataModel.get().object();

        // add constant struct field names references overhead
        result += soi.getAllStructFieldRefs().size() * JavaDataModel.get().ref();
        for (StructField field : soi.getAllStructFieldRefs()) {
          result += getSizeOfComplexTypes(conf, field.getFieldObjectInspector());
        }
      }
      break;
    case UNION:
      UnionObjectInspector uoi = (UnionObjectInspector) oi;

      // add constant object overhead for union
      result += JavaDataModel.get().object();

      // add constant size for unions tags
      result += uoi.getObjectInspectors().size() * JavaDataModel.get().primitive1();
      for (ObjectInspector foi : uoi.getObjectInspectors()) {
        result += getSizeOfComplexTypes(conf, foi);
      }
      break;
    default:
      break;
    }

    return result;
  }

  /**
   * Get size of fixed length primitives
   * @param colType
   *          - column type
   * @return raw data size
   */
  public static long getAvgColLenOfFixedLengthTypes(String colType) {
    String colTypeLowerCase = colType.toLowerCase();
    if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.VOID_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.BOOLEAN_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)) {
      return JavaDataModel.get().primitive1();
    } else if (colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME)
        || colTypeLowerCase.equals("long")) {
      return JavaDataModel.get().primitive2();
    } else if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME)) {
      return JavaDataModel.get().lengthOfTimestamp();
    } else if (colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
      return JavaDataModel.get().lengthOfDate();
    } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      return JavaDataModel.get().lengthOfDecimal();
    } else if (colTypeLowerCase.equals(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME)) {
      return JavaDataModel.JAVA32_META;
    } else {
      //TODO: support complex types
      // for complex type we simply return 0
      return 0;
    }
  }

  /**
   * Get the size of arrays of primitive types
   * @return raw data size
   */
  public static long getSizeOfPrimitiveTypeArraysFromType(String colType, int length, HiveConf conf) {
    String colTypeLowerCase = colType.toLowerCase();
    if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME)
        || colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)) {
      return JavaDataModel.get().lengthForIntArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      return JavaDataModel.get().lengthForDoubleArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME)
        || colTypeLowerCase.equals("long")) {
      return JavaDataModel.get().lengthForLongArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.BINARY_TYPE_NAME)) {
      return JavaDataModel.get().lengthForByteArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      return JavaDataModel.get().lengthForBooleanArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.DATETIME_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME) ||
        colTypeLowerCase.equals(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME)) {
      return JavaDataModel.get().lengthForTimestampArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
      return JavaDataModel.get().lengthForDateArrayOfSize(length);
    } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      return JavaDataModel.get().lengthForDecimalArrayOfSize(length);
    } else if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
        || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)) {
      int configVarLen = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAX_VARIABLE_LENGTH);
      int siz = JavaDataModel.get().lengthForStringOfLength(configVarLen);
      return JavaDataModel.get().lengthForPrimitiveArrayOfSize(siz, length);
    } else {
      return 0;
    }
  }

  /**
   * Estimate the size of map object
   * @param scmoi
   *          - object inspector
   * @return size of map
   */
  public static long getSizeOfMap(StandardConstantMapObjectInspector scmoi) {
    Map<?, ?> map = scmoi.getWritableConstantValue();
    ObjectInspector koi = scmoi.getMapKeyObjectInspector();
    ObjectInspector voi = scmoi.getMapValueObjectInspector();
    long result = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      result += getWritableSize(koi, entry.getKey());
      result += getWritableSize(voi, entry.getValue());
    }

    // add additional overhead of each map entries
    result += JavaDataModel.get().hashMap(map.entrySet().size());
    return result;
  }

  public static long getSizeOfStruct(StandardConstantStructObjectInspector soi) {
	long result = 0;
    // add constant object overhead for struct
    result += JavaDataModel.get().object();

    // add constant struct field names references overhead
    result += soi.getAllStructFieldRefs().size() * JavaDataModel.get().ref();
    List<?> value = soi.getWritableConstantValue();
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    if (value == null || value.size() != fields.size()) {
      return result;
    }
    for (int i = 0; i < fields.size(); i++) {
      result += getWritableSize(fields.get(i).getFieldObjectInspector(), value.get(i));
    }
	return result;
  }

  /**
   * Get size of primitive data types based on their respective writable object inspector
   * @param oi
   *          - object inspector
   * @param value
   *          - value
   * @return raw data size
   */
  public static long getWritableSize(ObjectInspector oi, Object value) {
    if (oi instanceof WritableStringObjectInspector) {
      WritableStringObjectInspector woi = (WritableStringObjectInspector) oi;
      return JavaDataModel.get().lengthForStringOfLength(
          woi.getPrimitiveWritableObject(value).getLength());
    } else if (oi instanceof WritableBinaryObjectInspector) {
      WritableBinaryObjectInspector woi = (WritableBinaryObjectInspector) oi;
      return JavaDataModel.get().lengthForByteArrayOfSize(
          woi.getPrimitiveWritableObject(value).getLength());
    } else if (oi instanceof WritableBooleanObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableByteObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableDateObjectInspector) {
      return JavaDataModel.get().lengthOfDate();
    } else if (oi instanceof WritableDoubleObjectInspector) {
      return JavaDataModel.get().primitive2();
    } else if (oi instanceof WritableFloatObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableHiveDecimalObjectInspector) {
      return JavaDataModel.get().lengthOfDecimal();
    } else if (oi instanceof WritableIntObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableLongObjectInspector) {
      return JavaDataModel.get().primitive2();
    } else if (oi instanceof WritableShortObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableTimestampObjectInspector ||
        oi instanceof WritableTimestampLocalTZObjectInspector) {
      return JavaDataModel.get().lengthOfTimestamp();
    }

    return 0;
  }

  /**
   * Get column statistics from parent statistics.
   * @param conf
   *          - hive conf
   * @param parentStats
   *          - parent statistics
   * @param colExprMap
   *          - column expression map
   * @param rowSchema
   *          - row schema
   * @return column statistics
   */
  public static List<ColStatistics> getColStatisticsFromExprMap(HiveConf conf,
      Statistics parentStats, Map<String, ExprNodeDesc> colExprMap, RowSchema rowSchema) {

    List<ColStatistics> cs = Lists.newArrayList();
    if (colExprMap != null  && rowSchema != null) {
      for (ColumnInfo ci : rowSchema.getSignature()) {
        String outColName = ci.getInternalName();
        ExprNodeDesc end = colExprMap.get(outColName);
        ColStatistics colStat = getColStatisticsFromExpression(conf, parentStats, end);
        if (colStat != null) {
          colStat.setColumnName(outColName);
          cs.add(colStat);
        }
      }
      // sometimes RowSchema is empty, so fetch stats of columns in exprMap
      for (Entry<String, ExprNodeDesc> pair : colExprMap.entrySet()) {
        if (rowSchema.getColumnInfo(pair.getKey()) == null) {
          ColStatistics colStat = getColStatisticsFromExpression(conf, parentStats, pair.getValue());
          if (colStat != null) {
            colStat.setColumnName(pair.getKey());
            cs.add(colStat);
          }
        }
      }

      return cs;
    }

    // In cases where column expression map or row schema is missing, just pass on the parent column
    // stats. This could happen in cases like TS -> FIL where FIL does not map input column names to
    // internal names.
    if (colExprMap == null || rowSchema == null) {
      if (parentStats.getColumnStats() != null) {
        cs.addAll(parentStats.getColumnStats());
      }
    }
    return cs;
  }

  /**
   * Get column statistics from parent statistics given the
   * row schema of its child.
   * @param parentStats
   *          - parent statistics
   * @param rowSchema
   *          - row schema
   * @return column statistics
   */
  public static List<ColStatistics> getColStatisticsUpdatingTableAlias(
          Statistics parentStats, RowSchema rowSchema) {

    List<ColStatistics> cs = Lists.newArrayList();

    for (ColStatistics parentColStat : parentStats.getColumnStats()) {
      ColStatistics colStat;
      colStat = parentColStat.clone();
      if (colStat != null) {
        cs.add(colStat);
      }
    }

    return cs;
  }

  /**
   * Get column statistics expression nodes
   * @param conf
   *          - hive conf
   * @param parentStats
   *          - parent statistics
   * @param end
   *          - expression nodes
   * @return column statistics
   */
  public static ColStatistics getColStatisticsFromExpression(HiveConf conf, Statistics parentStats,
      ExprNodeDesc end) {

    if (end == null) {
      return null;
    }

    String colName = null;
    String colType = null;
    double avgColSize = 0;
    long countDistincts = 0;
    long numNulls = 0;
    ObjectInspector oi = end.getWritableObjectInspector();
    long numRows = parentStats.getNumRows();

    if (end instanceof ExprNodeColumnDesc) {
      // column projection
      ExprNodeColumnDesc encd = (ExprNodeColumnDesc) end;
      colName = encd.getColumn();

      if (encd.getIsPartitionColOrVirtualCol()) {

        ColStatistics colStats = parentStats.getColumnStatisticsFromColName(colName);
        if (colStats != null) {
          /* If statistics for the column already exist use it. */
            return colStats.clone();
        }

        // virtual columns
        colType = encd.getTypeInfo().getTypeName();
        countDistincts = numRows;
      } else {

        // clone the column stats and return
        ColStatistics result = parentStats.getColumnStatisticsFromColName(colName);
        if (result != null) {
            return result.clone();
        }
        return null;
      }
    } else if (end instanceof ExprNodeConstantDesc) {

      // constant projection
      ExprNodeConstantDesc encd = (ExprNodeConstantDesc) end;

      colName = encd.getName();
      colType = encd.getTypeString();
      if (encd.getValue() == null) {
        // null projection
        numNulls = numRows;
      } else {
        countDistincts = 1;
      }
    } else if (end instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc engfd = (ExprNodeGenericFuncDesc) end;
      colName = engfd.getName();
      colType = engfd.getTypeString();

      // If it is a widening cast, we do not change NDV, min, max
      if (isWideningCast(engfd) && engfd.getChildren().get(0) instanceof ExprNodeColumnDesc) {
        // cast on single column
        ColStatistics stats = parentStats.getColumnStatisticsFromColName(engfd.getCols().get(0));
        if (stats != null) {
          ColStatistics newStats;
          newStats = stats.clone();
          newStats.setColumnName(colName);
          colType = colType.toLowerCase();
          newStats.setColumnType(colType);
          newStats.setAvgColLen(getAvgColLenOf(conf, oi, colType));
          return newStats;
        }
      }

      // fallback to default
      countDistincts = getNDVFor(engfd, numRows, parentStats);
    } else if (end instanceof ExprNodeColumnListDesc) {

      // column list
      ExprNodeColumnListDesc encd = (ExprNodeColumnListDesc) end;
      colName = Joiner.on(",").join(encd.getCols());
      colType = serdeConstants.LIST_TYPE_NAME;
      countDistincts = numRows;
    } else if (end instanceof ExprNodeFieldDesc) {

      // field within complex type
      ExprNodeFieldDesc enfd = (ExprNodeFieldDesc) end;
      colName = enfd.getFieldName();
      colType = enfd.getTypeString();
      countDistincts = numRows;
    } else {
      throw new IllegalArgumentException("not supported expr type " + end.getClass());
    }

    colType = colType.toLowerCase();
    avgColSize = getAvgColLenOf(conf, oi, colType);
    ColStatistics colStats = new ColStatistics(colName, colType);
    colStats.setAvgColLen(avgColSize);
    colStats.setCountDistint(countDistincts);
    colStats.setNumNulls(numNulls);

    return colStats;
  }

  private static boolean isWideningCast(ExprNodeGenericFuncDesc engfd) {
    GenericUDF udf = engfd.getGenericUDF();
    if (!FunctionRegistry.isOpCast(udf)) {
      // It is not a cast
      return false;
    }
    return TypeInfoUtils.implicitConvertible(engfd.getChildren().get(0).getTypeInfo(),
            engfd.getTypeInfo());
  }

  public static Long addWithExpDecay (List<Long> distinctVals) {
    // Exponential back-off for NDVs.
    // 1) Descending order sort of NDVs
    // 2) denominator = NDV1 * (NDV2 ^ (1/2)) * (NDV3 ^ (1/4))) * ....
    Collections.sort(distinctVals, Collections.reverseOrder());

    long denom = distinctVals.get(0);
    for (int i = 1; i < distinctVals.size(); i++) {
      denom = (long) (denom * Math.pow(distinctVals.get(i), 1.0 / (1 << i)));
    }

    return denom;
  }

  private static long getNDVFor(ExprNodeGenericFuncDesc engfd, long numRows, Statistics parentStats) {

    GenericUDF udf = engfd.getGenericUDF();
    if (!FunctionRegistry.isDeterministic(udf) && !FunctionRegistry.isRuntimeConstant(udf)){
      return numRows;
    }
    List<Long> ndvs = Lists.newArrayList();
    Class<?> udfClass = udf instanceof GenericUDFBridge ? ((GenericUDFBridge) udf).getUdfClass() : udf.getClass();
    NDV ndv = AnnotationUtils.getAnnotation(udfClass, NDV.class);
    long udfNDV = Long.MAX_VALUE;
    if (ndv != null) {
      udfNDV = ndv.maxNdv();
    } else {
      for (String col : engfd.getCols()) {
        ColStatistics stats = parentStats.getColumnStatisticsFromColName(col);
        if (stats != null) {
          ndvs.add(stats.getCountDistint());
        }
      }
    }
    long countDistincts = ndvs.isEmpty() ? numRows : addWithExpDecay(ndvs);
    return Collections.min(Lists.newArrayList(countDistincts, udfNDV, numRows));
    }

  /**
   * Get number of rows of a give table
   * @return number of rows
   */
  public static long getNumRows(Table table) {
    return getBasicStatForTable(table, StatsSetupConst.ROW_COUNT);
  }

  /**
   * Get raw data size of a give table
   * @return raw data size
   */
  public static long getRawDataSize(Table table) {
    return getBasicStatForTable(table, StatsSetupConst.RAW_DATA_SIZE);
  }

  /**
   * Get total size of a give table
   * @return total size
   */
  public static long getTotalSize(Table table) {
    return getBasicStatForTable(table, StatsSetupConst.TOTAL_SIZE);
  }

  /**
   * Get basic stats of table
   * @param table
   *          - table
   * @param statType
   *          - type of stats
   * @return value of stats
   */
  public static long getBasicStatForTable(Table table, String statType) {
    Map<String, String> params = table.getParameters();
    long result = -1;

    if (params != null) {
      try {
        result = Long.parseLong(params.get(statType));
      } catch (NumberFormatException e) {
        result = -1;
      }
    }
    return result;
  }

  /**
   * Get basic stats of partitions
   * @param table
   *          - table
   * @param parts
   *          - partitions
   * @param statType
   *          - type of stats
   * @return value of stats
   */
  public static List<Long> getBasicStatForPartitions(Table table, List<Partition> parts,
      String statType) {

    List<Long> stats = Lists.newArrayList();
    for (Partition part : parts) {
      Map<String, String> params = part.getParameters();
      long result = 0;
      if (params != null) {
        try {
          result = Long.parseLong(params.get(statType));
        } catch (NumberFormatException e) {
          result = 0;
        }
        stats.add(result);
      }
    }
    return stats;
  }

  /**
   * Compute raw data size from column statistics
   * @param numRows
   *          - number of rows
   * @param colStats
   *          - column statistics
   * @return raw data size
   */
  public static long getDataSizeFromColumnStats(long numRows, List<ColStatistics> colStats) {
    long result = 0;

    if (numRows <= 0 || colStats == null) {
      return result;
    }

    if (colStats.isEmpty()) {
      // this may happen if we are not projecting any column from current operator
      // think count(*) where we are projecting rows without any columns
      // in such a case we estimate empty row to be of size of empty java object.
      return numRows * JavaDataModel.JAVA64_REF;
    }

    for (ColStatistics cs : colStats) {
      if (cs != null) {
        String colTypeLowerCase = cs.getColumnType().toLowerCase();
        long nonNullCount = cs.getNumNulls() > 0 ? numRows - cs.getNumNulls() + 1 : numRows;
        double sizeOf = 0;
        if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)
            || colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)
            || colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME)
            || colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME)
            || colTypeLowerCase.equals(serdeConstants.BOOLEAN_TYPE_NAME)
            || colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)
            || colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
          sizeOf = cs.getAvgColLen();
        } else if (colTypeLowerCase.equals(serdeConstants.STRING_TYPE_NAME)
            || colTypeLowerCase.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
            || colTypeLowerCase.startsWith(serdeConstants.CHAR_TYPE_NAME)) {
          int acl = (int) Math.round(cs.getAvgColLen());
          sizeOf = JavaDataModel.get().lengthForStringOfLength(acl);
        } else if (colTypeLowerCase.equals(serdeConstants.BINARY_TYPE_NAME)) {
          int acl = (int) Math.round(cs.getAvgColLen());
          sizeOf = JavaDataModel.get().lengthForByteArrayOfSize(acl);
        } else if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME) ||
            colTypeLowerCase.equals(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME)) {
          sizeOf = JavaDataModel.get().lengthOfTimestamp();
        } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
          sizeOf = JavaDataModel.get().lengthOfDecimal();
        } else if (colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
          sizeOf = JavaDataModel.get().lengthOfDate();
        } else {
          sizeOf = cs.getAvgColLen();
        }
        result = safeAdd(result, safeMult(nonNullCount, sizeOf));
      }
    }

    return result;
  }

  public static String getFullyQualifiedTableName(String dbName, String tabName) {
    return getFullyQualifiedName(dbName, tabName);
  }

  private static String getFullyQualifiedName(String... names) {
    List<String> nonNullAndEmptyNames = Lists.newArrayList();
    for (String name : names) {
      if (name != null && !name.isEmpty()) {
        nonNullAndEmptyNames.add(name);
      }
    }
    return Joiner.on(".").join(nonNullAndEmptyNames);
  }

  /**
   * Get qualified column name from output key column names
   * @param keyExprs
   *          - output key names
   * @return list of qualified names
   */
  public static List<String> getQualifedReducerKeyNames(List<String> keyExprs) {
    List<String> result = Lists.newArrayList();
    if (keyExprs != null) {
      for (String key : keyExprs) {
        result.add(Utilities.ReduceField.KEY.toString() + "." + key);
      }
    }
    return result;
  }

  public static long getAvailableMemory(Configuration conf) {
    int memory = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVETEZCONTAINERSIZE);
    if (memory <= 0) {
      memory = conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);
      if (memory <= 0) {
        memory = 1024;
      }
    }
    return memory;
  }

  /**
   * negative number of rows or data sizes are invalid. It could be because of
   * long overflow in which case return Long.MAX_VALUE
   * @param val - input value
   * @return Long.MAX_VALUE if val is negative else val
   */
  public static long getMaxIfOverflow(long val) {
    return val < 0 ? Long.MAX_VALUE : val;
  }

  /** Bounded multiplication - overflows become MAX_VALUE */
  public static long safeMult(long a, double b) {
    double result = a * b;
    return (result > Long.MAX_VALUE) ? Long.MAX_VALUE : (long)result;
  }

  /** Bounded addition - overflows become MAX_VALUE */
  public static long safeAdd(long a, long b) {
    try {
      return LongMath.checkedAdd(a, b);
    } catch (ArithmeticException ex) {
      return Long.MAX_VALUE;
    }
  }

  /** Bounded multiplication - overflows become MAX_VALUE */
  public static long safeMult(long a, long b) {
    try {
      return LongMath.checkedMultiply(a, b);
    } catch (ArithmeticException ex) {
      return Long.MAX_VALUE;
    }
  }

  public static List<Long> safeMult(List<Long> l, float b) {
    List<Long> ret = new ArrayList<>();
    for (Long a : l) {
      ret.add(safeMult(a, b));
    }
    return ret;
  }

  public static boolean hasDiscreteRange(ColStatistics colStat) {
    if (colStat.getRange() != null) {
      TypeInfo colType = TypeInfoUtils.getTypeInfoFromTypeString(colStat.getColumnType());
      if (colType.getCategory() == Category.PRIMITIVE) {
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) colType;
        switch (pti.getPrimitiveCategory()) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            return true;
          default:
            break;
        }
      }
    }
    return false;
  }

  public static Range combineRange(Range range1, Range range2) {
    if (   range1.minValue != null && range1.maxValue != null
        && range2.minValue != null && range2.maxValue != null) {
      long min1 = range1.minValue.longValue();
      long max1 = range1.maxValue.longValue();
      long min2 = range2.minValue.longValue();
      long max2 = range2.maxValue.longValue();

      if (max1 < min2 || max2 < min1) {
        // No overlap between the two ranges
        return null;
      } else {
        // There is an overlap of ranges - create combined range.
        return new ColStatistics.Range(
            Math.min(min1, min2),
            Math.max(max1, max2));
      }
    }
    return null;
  }
}
