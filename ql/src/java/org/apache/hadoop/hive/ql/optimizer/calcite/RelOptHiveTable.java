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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelReferentialConstraintImpl;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo.ForeignKeyCol;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.PartitionIterable;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint.UniqueConstraintCol;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ColumnStatsList;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class RelOptHiveTable implements RelOptTable {

  //~ Instance fields --------------------------------------------------------

  private final RelOptSchema schema;
  private final RelDataType rowType;
  private final List<String> qualifiedTblName;
  private final String name;
  private final Table                             hiveTblMetadata;
  private final ImmutableList<ColumnInfo>         hiveNonPartitionCols;
  private final ImmutableList<ColumnInfo>         hivePartitionCols;
  private final ImmutableMap<Integer, ColumnInfo> hiveNonPartitionColsMap;
  private final ImmutableMap<Integer, ColumnInfo> hivePartitionColsMap;
  private final ImmutableList<VirtualColumn>      hiveVirtualCols;
  private final int                               noOfNonVirtualCols;
  private final List<ImmutableBitSet>             keys;
  private final List<RelReferentialConstraint>    referentialConstraints;
  final HiveConf                                  hiveConf;

  private double                                  rowCount        = -1;
  Map<Integer, ColStatistics>                     hiveColStatsMap = new HashMap<>();
  PrunedPartitionList                             partitionList;
  Map<String, PrunedPartitionList>                partitionCache;
  Map<String, ColumnStatsList>                    colStatsCache;
  AtomicInteger                                   noColsMissingStats;

  protected static final Logger LOG = LoggerFactory.getLogger(RelOptHiveTable.class.getName());

  public RelOptHiveTable(RelOptSchema calciteSchema, List<String> qualifiedTblName,
      RelDataType rowType, Table hiveTblMetadata, List<ColumnInfo> hiveNonPartitionCols,
      List<ColumnInfo> hivePartitionCols, List<VirtualColumn> hiveVirtualCols, HiveConf hconf,
      Map<String, PrunedPartitionList> partitionCache, Map<String, ColumnStatsList> colStatsCache,
      AtomicInteger noColsMissingStats) {
    this.schema = calciteSchema;
    this.qualifiedTblName = ImmutableList.copyOf(qualifiedTblName);
    this.name = this.qualifiedTblName.stream().collect(Collectors.joining("."));
    this.rowType = rowType;
    this.hiveTblMetadata = hiveTblMetadata;
    this.hiveNonPartitionCols = ImmutableList.copyOf(hiveNonPartitionCols);
    this.hiveNonPartitionColsMap = HiveCalciteUtil.getColInfoMap(hiveNonPartitionCols, 0);
    this.hivePartitionCols = ImmutableList.copyOf(hivePartitionCols);
    this.hivePartitionColsMap = HiveCalciteUtil.getColInfoMap(hivePartitionCols, hiveNonPartitionColsMap.size());
    this.noOfNonVirtualCols = hiveNonPartitionCols.size() + hivePartitionCols.size();
    this.hiveVirtualCols = ImmutableList.copyOf(hiveVirtualCols);
    this.hiveConf = hconf;
    this.partitionCache = partitionCache;
    this.colStatsCache = colStatsCache;
    this.noColsMissingStats = noColsMissingStats;
    this.keys = generateKeys();
    this.referentialConstraints = generateReferentialConstraints();
  }

  //~ Methods ----------------------------------------------------------------

  public String getName() {
    return name;
  }

  @Override
  public List<String> getQualifiedName() {
    return qualifiedTblName;
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return schema;
  }

  @Override
  public Expression getExpression(Class clazz) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelOptTable extend(List<RelDataTypeField> extendedFields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ColumnStrategy> getColumnStrategies() {
    return RelOptTableImpl.columnStrategies(this);
  }

  public RelOptHiveTable copy(RelDataType newRowType) {
    // 1. Build map of column name to col index of original schema
    // Assumption: Hive Table can not contain duplicate column names
    Map<String, Integer> nameToColIndxMap = new HashMap<String, Integer>();
    for (RelDataTypeField f : this.rowType.getFieldList()) {
      nameToColIndxMap.put(f.getName(), f.getIndex());
    }

    // 2. Build nonPart/Part/Virtual column info for new RowSchema
    List<ColumnInfo> newHiveNonPartitionCols = new ArrayList<ColumnInfo>();
    List<ColumnInfo> newHivePartitionCols = new ArrayList<ColumnInfo>();
    List<VirtualColumn> newHiveVirtualCols = new ArrayList<VirtualColumn>();
    Map<Integer, VirtualColumn> virtualColInfoMap = HiveCalciteUtil.getVColsMap(this.hiveVirtualCols,
        this.noOfNonVirtualCols);
    Integer originalColIndx;
    ColumnInfo cInfo;
    VirtualColumn vc;
    for (RelDataTypeField f : newRowType.getFieldList()) {
      originalColIndx = nameToColIndxMap.get(f.getName());
      if ((cInfo = hiveNonPartitionColsMap.get(originalColIndx)) != null) {
        newHiveNonPartitionCols.add(new ColumnInfo(cInfo));
      } else if ((cInfo = hivePartitionColsMap.get(originalColIndx)) != null) {
        newHivePartitionCols.add(new ColumnInfo(cInfo));
      } else if ((vc = virtualColInfoMap.get(originalColIndx)) != null) {
        newHiveVirtualCols.add(vc);
      } else {
        throw new RuntimeException("Copy encountered a column not seen in original TS");
      }
    }

    // 3. Build new Table
    return new RelOptHiveTable(this.schema, this.qualifiedTblName, newRowType,
        this.hiveTblMetadata, newHiveNonPartitionCols, newHivePartitionCols, newHiveVirtualCols,
        this.hiveConf, this.partitionCache, this.colStatsCache, this.noColsMissingStats);
  }

  @Override
  public boolean isKey(ImmutableBitSet columns) {
    for (ImmutableBitSet key : keys) {
      if (columns.contains(key)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return referentialConstraints;
  }

  private List<ImmutableBitSet> generateKeys() {
    ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
    // First PK
    final PrimaryKeyInfo pki;
    try {
      pki = Hive.get().getReliablePrimaryKeys(
          hiveTblMetadata.getDbName(), hiveTblMetadata.getTableName());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    if (!pki.getColNames().isEmpty()) {
      ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
      for (String pkColName : pki.getColNames().values()) {
        int pkPos;
        for (pkPos = 0; pkPos < rowType.getFieldNames().size(); pkPos++) {
          String colName = rowType.getFieldNames().get(pkPos);
          if (pkColName.equals(colName)) {
            break;
          }
        }
        if (pkPos == rowType.getFieldNames().size()) {
          LOG.error("Column for primary key definition " + pkColName + " not found");
          return ImmutableList.of();
        }
        keys.set(pkPos);
      }
      builder.add(keys.build());
    }
    // Then UKs
    final UniqueConstraint uki;
    try {
      uki = Hive.get().getReliableUniqueConstraints(
          hiveTblMetadata.getDbName(), hiveTblMetadata.getTableName());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    for (List<UniqueConstraintCol> ukCols : uki.getUniqueConstraints().values()) {
      ImmutableBitSet.Builder keys = ImmutableBitSet.builder();
      for (UniqueConstraintCol ukCol : ukCols) {
        int ukPos;
        for (ukPos = 0; ukPos < rowType.getFieldNames().size(); ukPos++) {
          String colName = rowType.getFieldNames().get(ukPos);
          if (ukCol.colName.equals(colName)) {
            break;
          }
        }
        if (ukPos == rowType.getFieldNames().size()) {
          LOG.error("Column for unique constraint definition " + ukCol.colName + " not found");
          return ImmutableList.of();
        }
        keys.set(ukPos);
      }
      builder.add(keys.build());
    }
    return builder.build();
  }

  private List<RelReferentialConstraint> generateReferentialConstraints() {
    final ForeignKeyInfo fki;
    try {
      fki = Hive.get().getReliableForeignKeys(
          hiveTblMetadata.getDbName(), hiveTblMetadata.getTableName());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    ImmutableList.Builder<RelReferentialConstraint> builder = ImmutableList.builder();
    for (List<ForeignKeyCol> fkCols : fki.getForeignKeys().values()) {
      List<String> foreignKeyTableQualifiedName = qualifiedTblName;
      String parentDatabaseName = fkCols.get(0).parentDatabaseName;
      String parentTableName = fkCols.get(0).parentTableName;
      List<String> parentTableQualifiedName = new ArrayList<>();
      if (parentDatabaseName != null && !parentDatabaseName.isEmpty()) {
        parentTableQualifiedName.add(parentDatabaseName);
      }
      parentTableQualifiedName.add(parentTableName);
      Table parentTab = null;
      try {
        // TODO: We have a cache for Table objects in SemanticAnalyzer::getTableObjectByName()
        // We need to move that cache elsewhere and use it from places like this.
        parentTab = Hive.get().getTable(parentDatabaseName, parentTableName);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      if (parentTab == null) {
        LOG.error("Table for primary key not found: "
              + "databaseName: " + parentDatabaseName+ ", "
              + "tableName: " + parentTableName);
        return ImmutableList.of();
      }
      ImmutableList.Builder<IntPair> keys = ImmutableList.builder();
      for (ForeignKeyCol fkCol : fkCols) {
        int fkPos;
        for (fkPos = 0; fkPos < rowType.getFieldNames().size(); fkPos++) {
          String fkColName = rowType.getFieldNames().get(fkPos);
          if (fkColName.equals(fkCol.childColName)) {
            break;
          }
        }
        int pkPos;
        for (pkPos = 0; pkPos < parentTab.getAllCols().size(); pkPos++) {
          String pkColName = parentTab.getAllCols().get(pkPos).getName();
          if (pkColName.equals(fkCol.parentColName)) {
            break;
          }
        }
        if (fkPos == rowType.getFieldNames().size()
            || pkPos == parentTab.getAllCols().size()) {
          LOG.error("Column for foreign key definition " + fkCol + " not found");
          return ImmutableList.of();
        }
        keys.add(IntPair.of(fkPos, pkPos));
      }
      builder.add(RelReferentialConstraintImpl.of(foreignKeyTableQualifiedName,
              parentTableQualifiedName, keys.build()));
    }
    return builder.build();
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
  public List<RelCollation> getCollationList() {
    ImmutableList.Builder<RelFieldCollation> collationList = new ImmutableList.Builder<RelFieldCollation>();
    for (Order sortColumn : this.hiveTblMetadata.getSortCols()) {
      for (int i=0; i<this.hiveTblMetadata.getSd().getCols().size(); i++) {
        FieldSchema field = this.hiveTblMetadata.getSd().getCols().get(i);
        if (field.getName().equals(sortColumn.getCol())) {
          Direction direction;
          NullDirection nullDirection;
          if (sortColumn.getOrder() == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC) {
            direction = Direction.ASCENDING;
            nullDirection = NullDirection.FIRST;
          }
          else {
            direction = Direction.DESCENDING;
            nullDirection = NullDirection.LAST;
          }
          collationList.add(new RelFieldCollation(i,direction,nullDirection));
          break;
        }
      }
    }
    return new ImmutableList.Builder<RelCollation>()
            .add(RelCollationTraitDef.INSTANCE.canonize(
                    new HiveRelCollation(collationList.build())))
            .build();
  }

  @Override
  public RelDistribution getDistribution() {
    ImmutableList.Builder<Integer> columnPositions = new ImmutableList.Builder<Integer>();
    for (String bucketColumn : this.hiveTblMetadata.getBucketCols()) {
      for (int i=0; i<this.hiveTblMetadata.getSd().getCols().size(); i++) {
        FieldSchema field = this.hiveTblMetadata.getSd().getCols().get(i);
        if (field.getName().equals(bucketColumn)) {
          columnPositions.add(i);
          break;
        }
      }
    }
    return new HiveRelDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
            columnPositions.build());
  }

  @Override
  public double getRowCount() {
    if (rowCount == -1) {
      if (null == partitionList) {
        // we are here either unpartitioned table or partitioned table with no
        // predicates
        computePartitionList(hiveConf, null, new HashSet<Integer>());
      }
      rowCount = StatsUtils.getNumRows(hiveConf, getNonPartColumns(), hiveTblMetadata,
          partitionList, noColsMissingStats);
    }

    return rowCount;
  }

  public Table getHiveTableMD() {
    return hiveTblMetadata;
  }

  private String getColNamesForLogging(Set<String> colLst) {
    StringBuilder sb = new StringBuilder();
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

  public void computePartitionList(HiveConf conf, RexNode pruneNode, Set<Integer> partOrVirtualCols) {
    try {
      if (!hiveTblMetadata.isPartitioned() || pruneNode == null
          || InputFinder.bits(pruneNode).length() == 0) {
        // there is no predicate on partitioning column, we need all partitions
        // in this case.
        partitionList = PartitionPruner.prune(hiveTblMetadata, null, conf, getName(),
            partitionCache);
        return;
      }

      // We have valid pruning expressions, only retrieve qualifying partitions
      ExprNodeDesc pruneExpr = pruneNode.accept(new ExprNodeConverter(getName(), getRowType(),
          partOrVirtualCols, this.getRelOptSchema().getTypeFactory()));

      partitionList = PartitionPruner.prune(hiveTblMetadata, pruneExpr, conf, getName(),
          partitionCache);
    } catch (HiveException he) {
      throw new RuntimeException(he);
    }
  }

  private void updateColStats(Set<Integer> projIndxLst, boolean allowMissingStats) {
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
      computePartitionList(hiveConf, null, new HashSet<Integer>());
    }

    ColumnStatsList colStatsCached = colStatsCache.get(partitionList.getKey());
    if (colStatsCached == null) {
      colStatsCached = new ColumnStatsList();
      colStatsCache.put(partitionList.getKey(), colStatsCached);
    }

    // 2. Obtain Col Stats for Non Partition Cols
    if (nonPartColNamesThatRqrStats.size() > 0) {
      List<ColStatistics> hiveColStats = new ArrayList<ColStatistics>();

      if (!hiveTblMetadata.isPartitioned()) {
        // 2.1 Handle the case for unpartitioned table.
        try {
          Statistics stats = StatsUtils.collectStatistics(hiveConf, null,
              hiveTblMetadata, hiveNonPartitionCols, nonPartColNamesThatRqrStats,
              colStatsCached, nonPartColNamesThatRqrStats, true);
          rowCount = stats.getNumRows();
          for (String c : nonPartColNamesThatRqrStats) {
            ColStatistics cs = stats.getColumnStatisticsFromColName(c);
            if (cs != null) {
              hiveColStats.add(cs);
            }
          }
          colStatsCached.updateState(stats.getColumnStatsState());

          // 2.1.1 Record Column Names that we needed stats for but couldn't
          if (hiveColStats.isEmpty()) {
            colNamesFailedStats.addAll(nonPartColNamesThatRqrStats);
          } else if (hiveColStats.size() != nonPartColNamesThatRqrStats.size()) {
            Set<String> setOfFiledCols = new HashSet<String>(nonPartColNamesThatRqrStats);

            Set<String> setOfObtainedColStats = new HashSet<String>();
            for (ColStatistics cs : hiveColStats) {
              setOfObtainedColStats.add(cs.getColumnName());
            }
            setOfFiledCols.removeAll(setOfObtainedColStats);

            colNamesFailedStats.addAll(setOfFiledCols);
          } else {
            // Column stats in hiveColStats might not be in the same order as the columns in
            // nonPartColNamesThatRqrStats. reorder hiveColStats so we can build hiveColStatsMap
            // using nonPartColIndxsThatRqrStats as below
            Map<String, ColStatistics> columnStatsMap =
                new HashMap<String, ColStatistics>(hiveColStats.size());
            for (ColStatistics cs : hiveColStats) {
              columnStatsMap.put(cs.getColumnName(), cs);
              // even though the stats were estimated we need to warn user that
              // stats are not available
              if(cs.isEstimated()) {
                colNamesFailedStats.add(cs.getColumnName());
              }
            }
            hiveColStats.clear();
            for (String colName : nonPartColNamesThatRqrStats) {
              hiveColStats.add(columnStatsMap.get(colName));
            }
          }
        } catch (HiveException e) {
          String logMsg = "Collecting stats for table: " + hiveTblMetadata.getTableName() + " failed.";
          LOG.error(logMsg, e);
          throw new RuntimeException(logMsg, e);
        }
      } else {
        // 2.2 Obtain col stats for partitioned table.
        try {
          if (partitionList.getNotDeniedPartns().isEmpty()) {
            // no need to make a metastore call
            rowCount = 0;
            hiveColStats = new ArrayList<ColStatistics>();
            for (int i = 0; i < nonPartColNamesThatRqrStats.size(); i++) {
              // add empty stats object for each column
              hiveColStats.add(
                  new ColStatistics(
                      nonPartColNamesThatRqrStats.get(i),
                      hiveNonPartitionColsMap.get(nonPartColIndxsThatRqrStats.get(i)).getTypeName()));
            }
            colNamesFailedStats.clear();
            colStatsCached.updateState(State.COMPLETE);
          } else {
            Statistics stats = StatsUtils.collectStatistics(hiveConf, partitionList,
                hiveTblMetadata, hiveNonPartitionCols, nonPartColNamesThatRqrStats, colStatsCached,
                nonPartColNamesThatRqrStats, true);
            rowCount = stats.getNumRows();
            hiveColStats = new ArrayList<ColStatistics>();
            for (String c : nonPartColNamesThatRqrStats) {
              ColStatistics cs = stats.getColumnStatisticsFromColName(c);
              if (cs != null) {
                hiveColStats.add(cs);
                if(cs.isEstimated()) {
                  colNamesFailedStats.add(c);
                }
              } else {
                colNamesFailedStats.add(c);
              }
            }
            colStatsCached.updateState(stats.getColumnStatsState());
          }
        } catch (HiveException e) {
          String logMsg = "Collecting stats failed.";
          LOG.error(logMsg, e);
          throw new RuntimeException(logMsg, e);
        }
      }

      if (hiveColStats != null && hiveColStats.size() == nonPartColNamesThatRqrStats.size()) {
        for (int i = 0; i < hiveColStats.size(); i++) {
          // the columns in nonPartColIndxsThatRqrStats/nonPartColNamesThatRqrStats/hiveColStats
          // are in same order
          hiveColStatsMap.put(nonPartColIndxsThatRqrStats.get(i), hiveColStats.get(i));
          colStatsCached.put(hiveColStats.get(i).getColumnName(), hiveColStats.get(i));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Stats for column " + hiveColStats.get(i).getColumnName() +
                    " in table " + hiveTblMetadata.getTableName() + " stored in cache");
            LOG.debug(hiveColStats.get(i).toString());
          }
        }
      }
    }

    // 3. Obtain Stats for Partition Cols
    if (colNamesFailedStats.isEmpty() && !partColNamesThatRqrStats.isEmpty()) {
      ColStatistics cStats = null;
      for (int i = 0; i < partColNamesThatRqrStats.size(); i++) {
        cStats = StatsUtils.getColStatsForPartCol(hivePartitionColsMap.get(partColIndxsThatRqrStats.get(i)),
            new PartitionIterable(partitionList.getNotDeniedPartns()), hiveConf);
        hiveColStatsMap.put(partColIndxsThatRqrStats.get(i), cStats);
        colStatsCached.put(cStats.getColumnName(), cStats);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stats for column " + cStats.getColumnName() +
                  " in table " + hiveTblMetadata.getTableName() + " stored in cache");
          LOG.debug(cStats.toString());
        }
      }
    }

    // 4. Warn user if we could get stats for required columns
    if (!colNamesFailedStats.isEmpty()) {
      String logMsg = "No Stats for " + hiveTblMetadata.getCompleteName() + ", Columns: "
          + getColNamesForLogging(colNamesFailedStats);
      noColsMissingStats.getAndAdd(colNamesFailedStats.size());
      if (allowMissingStats) {
        LOG.warn(logMsg);
        HiveConf conf = SessionState.getSessionConf();
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_SHOW_WARNINGS)) {
          LogHelper console = SessionState.getConsole();
          console.printInfo(logMsg);
        }
      } else {
        LOG.error(logMsg);
        throw new RuntimeException(logMsg);
      }
    }
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    // If we allow estimated stats for the columns, then we shall set the boolean to true,
    // since otherwise we will throw an exception because columns with estimated stats are
    // actually added to the list of columns that do not contain stats.
    return getColStat(projIndxLst, HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_STATS_ESTIMATE_STATS));
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst, boolean allowMissingStats) {
    List<ColStatistics> colStatsBldr = Lists.newArrayList();
    Set<Integer> projIndxSet = new HashSet<Integer>(projIndxLst);
    if (projIndxLst != null) {
      for (Integer i : projIndxLst) {
        if (hiveColStatsMap.get(i) != null) {
          colStatsBldr.add(hiveColStatsMap.get(i));
          projIndxSet.remove(i);
        }
      }
      if (!projIndxSet.isEmpty()) {
        updateColStats(projIndxSet, allowMissingStats);
        for (Integer i : projIndxSet) {
          colStatsBldr.add(hiveColStatsMap.get(i));
        }
      }
    } else {
      List<Integer> pILst = new ArrayList<Integer>();
      for (Integer i = 0; i < noOfNonVirtualCols; i++) {
        if (hiveColStatsMap.get(i) == null) {
          pILst.add(i);
        }
      }
      if (!pILst.isEmpty()) {
        updateColStats(new HashSet<Integer>(pILst), allowMissingStats);
        for (Integer pi : pILst) {
          colStatsBldr.add(hiveColStatsMap.get(pi));
        }
      }
    }

    return colStatsBldr;
  }

  /*
   * use to check if a set of columns are all partition columns. true only if: -
   * all columns in BitSet are partition columns.
   */
  public boolean containsPartitionColumnsOnly(ImmutableBitSet cols) {

    for (int i = cols.nextSetBit(0); i >= 0; i++, i = cols.nextSetBit(i + 1)) {
      if (!hivePartitionColsMap.containsKey(i)) {
        return false;
      }
    }
    return true;
  }

  public List<VirtualColumn> getVirtualCols() {
    return this.hiveVirtualCols;
  }

  public List<ColumnInfo> getPartColumns() {
    return this.hivePartitionCols;
  }

  public List<ColumnInfo> getNonPartColumns() {
    return this.hiveNonPartitionCols;
  }

  public int getNoOfNonVirtualCols() {
    return noOfNonVirtualCols;
  }

  public Map<Integer, ColumnInfo> getPartColInfoMap() {
    return hivePartitionColsMap;
  }

  public Map<Integer, ColumnInfo> getNonPartColInfoMap() {
    return hiveNonPartitionColsMap;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof RelOptHiveTable
        && this.rowType.equals(((RelOptHiveTable) obj).getRowType())
        && this.getHiveTableMD().equals(((RelOptHiveTable) obj).getHiveTableMD());
  }

  @Override
  public int hashCode() {
    return (this.getHiveTableMD() == null)
        ? super.hashCode() : this.getHiveTableMD().hashCode();
  }

}
