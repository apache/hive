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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class RelOptHiveTable extends RelOptAbstractTable {
  private final Table                             hiveTblMetadata;
  private final String                            tblAlias;
  private final ImmutableList<ColumnInfo>         hiveNonPartitionCols;
  private final ImmutableMap<Integer, ColumnInfo> hiveNonPartitionColsMap;
  private final ImmutableMap<Integer, ColumnInfo> hivePartitionColsMap;
  private final int                               noOfProjs;
  final HiveConf                                  hiveConf;

  private double                                  rowCount        = -1;
  Map<Integer, ColStatistics>                     hiveColStatsMap = new HashMap<Integer, ColStatistics>();
  PrunedPartitionList                             partitionList;
  Map<String, PrunedPartitionList>                partitionCache;
  AtomicInteger                                   noColsMissingStats;

  protected static final Log                      LOG               = LogFactory
                                                                        .getLog(RelOptHiveTable.class
                                                                            .getName());

  public RelOptHiveTable(RelOptSchema calciteSchema, String qualifiedTblName, String tblAlias, RelDataType rowType,
      Table hiveTblMetadata, List<ColumnInfo> hiveNonPartitionCols,
      List<ColumnInfo> hivePartitionCols, HiveConf hconf, Map<String, PrunedPartitionList> partitionCache, AtomicInteger noColsMissingStats) {
    super(calciteSchema, qualifiedTblName, rowType);
    this.hiveTblMetadata = hiveTblMetadata;
    this.tblAlias = tblAlias;
    this.hiveNonPartitionCols = ImmutableList.copyOf(hiveNonPartitionCols);
    this.hiveNonPartitionColsMap = getColInfoMap(hiveNonPartitionCols, 0);
    this.hivePartitionColsMap = getColInfoMap(hivePartitionCols, hiveNonPartitionColsMap.size());
    this.noOfProjs = hiveNonPartitionCols.size() + hivePartitionCols.size();
    this.hiveConf = hconf;
    this.partitionCache = partitionCache;
    this.noColsMissingStats = noColsMissingStats;
  }

  private static ImmutableMap<Integer, ColumnInfo> getColInfoMap(List<ColumnInfo> hiveCols,
      int startIndx) {
    Builder<Integer, ColumnInfo> bldr = ImmutableMap.<Integer, ColumnInfo> builder();

    int indx = startIndx;
    for (ColumnInfo ci : hiveCols) {
      bldr.put(indx, ci);
      indx++;
    }

    return bldr.build();
  }

  @Override
  public boolean isKey(ImmutableBitSet arg0) {
    return false;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return new LogicalTableScan(context.getCluster(), this);
  }

  @Override
  public <T> T unwrap(Class<T> arg0) {
    return arg0.isInstance(this) ? arg0.cast(this) : null;
  }

  @Override
  public double getRowCount() {
    if (rowCount == -1) {
      if (null == partitionList) {
        // we are here either unpartitioned table or partitioned table with no predicates
        computePartitionList(hiveConf, null);
      }
      if (hiveTblMetadata.isPartitioned()) {
        List<Long> rowCounts = StatsUtils.getBasicStatForPartitions(
            hiveTblMetadata, partitionList.getNotDeniedPartns(),
            StatsSetupConst.ROW_COUNT);
        rowCount = StatsUtils.getSumIgnoreNegatives(rowCounts);

      } else {
        rowCount = StatsUtils.getNumRows(hiveTblMetadata);
      }
    }

    if (rowCount == -1)
      noColsMissingStats.getAndIncrement();

    return rowCount;
  }

  public Table getHiveTableMD() {
    return hiveTblMetadata;
  }

  public String getTableAlias() {
    // NOTE: Calcite considers tbls to be equal if their names are the same. Hence
    // we need to provide Calcite the fully qualified table name (dbname.tblname)
    // and not the user provided aliases.
    // However in HIVE DB name can not appear in select list; in case of join
    // where table names differ only in DB name, Hive would require user
    // introducing explicit aliases for tbl.
    if (tblAlias == null)
      return hiveTblMetadata.getTableName();
    else
      return tblAlias;
  }

  private String getColNamesForLogging(Set<String> colLst) {
    StringBuffer sb = new StringBuffer();
    boolean firstEntry = true;
    for (String colName : colLst) {
      if (firstEntry) {
        sb.append(colName);
        firstEntry = false;
      } else {
        sb.append(", " + colName);
      }
    }
    return sb.toString();
  }

  public void computePartitionList(HiveConf conf, RexNode pruneNode) {

    try {
      if (!hiveTblMetadata.isPartitioned() || pruneNode == null || InputFinder.bits(pruneNode).length() == 0 ) {
        // there is no predicate on partitioning column, we need all partitions in this case.
        partitionList = PartitionPruner.prune(hiveTblMetadata, null, conf, getName(), partitionCache);
        return;
      }

      // We have valid pruning expressions, only retrieve qualifying partitions
      ExprNodeDesc pruneExpr = pruneNode.accept(new ExprNodeConverter(getName(), getRowType(), true, getRelOptSchema().getTypeFactory()));

      partitionList = PartitionPruner.prune(hiveTblMetadata, pruneExpr, conf, getName(), partitionCache);
    } catch (HiveException he) {
      throw new RuntimeException(he);
    }
  }

  private void updateColStats(Set<Integer> projIndxLst) {
    List<String> nonPartColNamesThatRqrStats = new ArrayList<String>();
    List<Integer> nonPartColIndxsThatRqrStats = new ArrayList<Integer>();
    List<String> partColNamesThatRqrStats = new ArrayList<String>();
    List<Integer> partColIndxsThatRqrStats = new ArrayList<Integer>();
    Set<String> colNamesFailedStats = new HashSet<String>();

    // 1. Separate required columns to Non Partition and Partition Cols
    ColumnInfo tmp;
    for (Integer pi : projIndxLst) {
      if (hiveColStatsMap.get(pi) == null) {
        if ((tmp = hiveNonPartitionColsMap.get(pi)) != null) {
          nonPartColNamesThatRqrStats.add(tmp.getInternalName());
          nonPartColIndxsThatRqrStats.add(pi);
        } else if ((tmp = hivePartitionColsMap.get(pi)) != null) {
          partColNamesThatRqrStats.add(tmp.getInternalName());
          partColIndxsThatRqrStats.add(pi);
        } else {
          noColsMissingStats.getAndIncrement();
          String logMsg = "Unable to find Column Index: " + pi + ", in "
              + hiveTblMetadata.getCompleteName();
          LOG.error(logMsg);
          throw new RuntimeException(logMsg);
        }
      }
    }

    if (null == partitionList) {
      // We could be here either because its an unpartitioned table or because
      // there are no pruning predicates on a partitioned table.
      computePartitionList(hiveConf, null);
    }

    // 2. Obtain Col Stats for Non Partition Cols
    if (nonPartColNamesThatRqrStats.size() > 0) {
      List<ColStatistics> hiveColStats;

      if (!hiveTblMetadata.isPartitioned()) {
        // 2.1 Handle the case for unpartitioned table.
        hiveColStats = StatsUtils.getTableColumnStats(hiveTblMetadata, hiveNonPartitionCols,
            nonPartColNamesThatRqrStats);

        // 2.1.1 Record Column Names that we needed stats for but couldn't
        if (hiveColStats == null) {
          colNamesFailedStats.addAll(nonPartColNamesThatRqrStats);
        } else if (hiveColStats.size() != nonPartColNamesThatRqrStats.size()) {
          Set<String> setOfFiledCols = new HashSet<String>(nonPartColNamesThatRqrStats);

          Set<String> setOfObtainedColStats = new HashSet<String>();
          for (ColStatistics cs : hiveColStats) {
            setOfObtainedColStats.add(cs.getColumnName());
          }
          setOfFiledCols.removeAll(setOfObtainedColStats);

          colNamesFailedStats.addAll(setOfFiledCols);
        }
      } else {
        // 2.2 Obtain col stats for partitioned table.
        try {
          if (partitionList.getNotDeniedPartns().isEmpty()) {
            // no need to make a metastore call
            rowCount = 0;
            hiveColStats = new ArrayList<ColStatistics>();
            for (String c : nonPartColNamesThatRqrStats) {
              // add empty stats object for each column
              hiveColStats.add(new ColStatistics(hiveTblMetadata.getTableName(), c, null));
            }
            colNamesFailedStats.clear();
          } else {
            Statistics stats = StatsUtils.collectStatistics(hiveConf, partitionList,
                hiveTblMetadata, hiveNonPartitionCols, nonPartColNamesThatRqrStats,
                nonPartColNamesThatRqrStats, true, true);
            rowCount = stats.getNumRows();
            hiveColStats = new ArrayList<ColStatistics>();
            for (String c : nonPartColNamesThatRqrStats) {
              ColStatistics cs = stats.getColumnStatisticsFromColName(c);
              if (cs != null) {
                hiveColStats.add(cs);
              } else {
                colNamesFailedStats.add(c);
              }
            }
          }
        } catch (HiveException e) {
          String logMsg = "Collecting stats failed.";
          LOG.error(logMsg, e);
          throw new RuntimeException(logMsg, e);
        }
      }

      if (hiveColStats != null && hiveColStats.size() == nonPartColNamesThatRqrStats.size()) {
        for (int i = 0; i < hiveColStats.size(); i++) {
          hiveColStatsMap.put(nonPartColIndxsThatRqrStats.get(i), hiveColStats.get(i));
        }
      }
    }

    // 3. Obtain Stats for Partition Cols
    if (colNamesFailedStats.isEmpty() && !partColNamesThatRqrStats.isEmpty()) {
      ColStatistics cStats = null;
      for (int i = 0; i < partColNamesThatRqrStats.size(); i++) {
        cStats = new ColStatistics(hiveTblMetadata.getTableName(),
            partColNamesThatRqrStats.get(i), hivePartitionColsMap.get(
                partColIndxsThatRqrStats.get(i)).getTypeName());
        cStats.setCountDistint(getDistinctCount(partitionList.getPartitions(),partColNamesThatRqrStats.get(i)));
        hiveColStatsMap.put(partColIndxsThatRqrStats.get(i), cStats);
      }
    }

    // 4. Warn user if we could get stats for required columns
    if (!colNamesFailedStats.isEmpty()) {
      String logMsg = "No Stats for " + hiveTblMetadata.getCompleteName() + ", Columns: "
          + getColNamesForLogging(colNamesFailedStats);
      LOG.error(logMsg);
      noColsMissingStats.getAndAdd(colNamesFailedStats.size());
      throw new RuntimeException(logMsg);
    }
  }

  private int getDistinctCount(Set<Partition> partitions, String partColName) {
    Set<String> distinctVals = new HashSet<String>(partitions.size());
    for (Partition partition : partitions) {
      distinctVals.add(partition.getSpec().get(partColName));
    }
    return distinctVals.size();
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    ImmutableList.Builder<ColStatistics> colStatsBldr = ImmutableList.<ColStatistics> builder();

    if (projIndxLst != null) {
      updateColStats(new HashSet<Integer>(projIndxLst));
      for (Integer i : projIndxLst) {
        colStatsBldr.add(hiveColStatsMap.get(i));
      }
    } else {
      List<Integer> pILst = new ArrayList<Integer>();
      for (Integer i = 0; i < noOfProjs; i++) {
        pILst.add(i);
      }
      updateColStats(new HashSet<Integer>(pILst));
      for (Integer pi : pILst) {
        colStatsBldr.add(hiveColStatsMap.get(pi));
      }
    }

    return colStatsBldr.build();
  }

  /*
   * use to check if a set of columns are all partition columns.
   * true only if:
   * - all columns in BitSet are partition
   * columns.
   */
  public boolean containsPartitionColumnsOnly(ImmutableBitSet cols) {

    for (int i = cols.nextSetBit(0); i >= 0; i++, i = cols.nextSetBit(i + 1)) {
      if (!hivePartitionColsMap.containsKey(i)) {
        return false;
      }
    }
    return true;
  }
}
