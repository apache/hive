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

package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaBasicAnalyzer;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.catalog.ImpalaHdfsTable;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaConjuncts;
import org.apache.hadoop.hive.ql.plan.impala.prune.ImpalaPrunedPartitionList;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.List;
import java.util.Map;

public class ImpalaHdfsScanRel extends ImpalaPlanRel {


  private ImpalaHdfsScanNode hdfsScanNode = null;
  private final HiveTableScan scan;
  private final HiveFilter filter;
  private final Hive db;

  public ImpalaHdfsScanRel(HiveTableScan scan, Hive db) {
    this(scan, null, db);
  }

  public ImpalaHdfsScanRel(HiveTableScan scan, HiveFilter filter, Hive db) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getInputs(),
        filter != null ? filter.getRowType() : scan.getRowType());
    this.scan = scan;
    this.filter = filter;
    this.db = db;
  }

  public Hive getDb() {
    return db;
  }

  public HiveTableScan getScan() {
    return scan;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException,
      MetaException {
    if (hdfsScanNode != null) {
      return hdfsScanNode;
    }

    List<FeFsPartition> feFsPartitions = Lists.newArrayList();

    ImpalaPrunedPartitionList prunedPartList =
        (ImpalaPrunedPartitionList) ((RelOptHiveTable) scan.getTable()).getPrunedPartitionList();
    Preconditions.checkNotNull(prunedPartList);

    // TODO: CDPD-8315
    // Impala passes an AggregateInfo to HdfsScanNode when there is a
    // potential for a perf optimization to be applied where a single table
    // count(*) could be satisfied by the scan directly. In the new planner
    // this could be done through a rule (not implemented yet), so for now
    // we will pass a null and re-visit this later.
    AggregateInfo aggInfo = null;

    String tableName = scan.getTable().getQualifiedName().get(1);
    TableName impalaTblName = TableName.parse(tableName);
    String alias = null;
    // Impala assumes that if an alias was supplied it is an explicit alias.
    // Since Hive scan's getTableAlias() always returns a non-null value (it returns
    // the table name if there was no alias), we add an additional check here before
    // setting it in Impala. Comparing with the table name is still not a foolproof way.
    // TODO: find a better way to identify explicit vs implicit alias
    if (!scan.getTableAlias().equalsIgnoreCase(tableName)) {
      alias = scan.getTableAlias();
    }
    TableRef tblRef = new TableRef(impalaTblName.toPath(), alias);
    ImpalaBasicAnalyzer basicAnalyzer = (ImpalaBasicAnalyzer) ctx.getRootAnalyzer();
    // save the mapping from table name to impala Table
    ImpalaHdfsTable impalaHdfsTable =
        ctx.getTableLoader().getHdfsTable(prunedPartList.getTable().getMetaStoreTable());
    basicAnalyzer.setTable(impalaTblName, impalaHdfsTable);
    Path resolvedPath = ctx.getRootAnalyzer().resolvePath(tblRef.getPath(), Path.PathType.TABLE_REF);

    ImpalaBaseTableRef baseTblRef = new ImpalaBaseTableRef(tblRef, resolvedPath, basicAnalyzer);

    List<FeFsPartition> impalaPartitions =
        impalaHdfsTable.getPartitions(prunedPartList.getBasicPartitions());

    TupleDescriptor tupleDesc =
        createTupleAndSlotDesc(baseTblRef, scan.getPrunedRowType(), basicAnalyzer);

    this.outputExprs =
        createScanOutputExprs(tupleDesc.getSlots(), (RelOptHiveTable) scan.getTable());

    List<RexNode> partitionConjuncts = prunedPartList.getPartitionConjuncts();
    // get the conjuncts from the filter. It is possible that new conditions were added
    // after partition pruning.  We pass in partition conjuncts because we don't want
    // the partition conjuncts to be used in the filter condition. This will also validate
    // that the existing partition conjuncts found at pruning time are still present in the 
    // filter as a check to make sure that the partiton conjunct wasn't stripped out of
    // the filter. Since partition pruning has already been done, we do not add any new
    // partition conjuncts, even if it is on a partitioned column.  It will be added to
    // the nonpartitioned conjuncts.
    ImpalaConjuncts assignedConjuncts = ImpalaConjuncts.create(filter,
        partitionConjuncts, basicAnalyzer, this, getCluster().getRexBuilder());
    List<Expr> impalaAssignedConjuncts = assignedConjuncts.getImpalaNonPartitionConjuncts();
    List<Expr> impalaPartitionConjuncts = assignedConjuncts.getImpalaPartitionConjuncts();

    this.nodeInfo = new ImpalaNodeInfo(impalaAssignedConjuncts, tupleDesc);
    PlanNodeId nodeId = ctx.getNextNodeId();

    hdfsScanNode = new ImpalaHdfsScanNode(nodeId, impalaPartitions, baseTblRef, aggInfo,
        impalaPartitionConjuncts, nodeInfo);
    hdfsScanNode.init(ctx.getRootAnalyzer());

    return hdfsScanNode;
  }

  // Create tuple and slot descriptors for this base table
  public static TupleDescriptor createTupleAndSlotDesc(BaseTableRef baseTblRef,
      RelDataType relDataType, Analyzer analyzer) throws ImpalaException {
    // create the tuple descriptor via the analyzer
    baseTblRef.analyze(analyzer);

    // create the slot descriptors corresponding to this tuple descriptor
    // by supplying the field names from Calcite's output schema for this node
    List<String> fieldNames = relDataType.getFieldNames();
    for (int i = 0; i < fieldNames.size(); i++) {
      SlotRef slotref = new SlotRef(Path.createRawPath(baseTblRef.getUniqueAlias(), fieldNames.get(i)));
      slotref.analyze(analyzer);
      SlotDescriptor slotDesc = slotref.getDesc();
      slotDesc.setIsMaterialized(true);
    }
    TupleDescriptor tupleDesc = baseTblRef.getDesc();
    return tupleDesc;
  }

  /**
   * Create the output SlotRefs using the supplied slot descriptors. The key for the map
   * is the position in the Hdfs table, with partitioned columns coming after non-partitioned
   * columns.
   */
  public static ImmutableMap<Integer, Expr> createScanOutputExprs(List<SlotDescriptor> slotDescs,
      RelOptHiveTable scanTable) {
    Map<Integer, Expr> exprs = Maps.newLinkedHashMap();
    int totalColumnsInTbl = scanTable.getNoOfNonVirtualCols();
    int nonPartitionedCols = scanTable.getNonPartColumns().size();
    for (SlotDescriptor slotDesc : slotDescs) {
      slotDesc.setIsMaterialized(true);
      // We need to determine the position of the column in the table. However, Impala places
      // the partitioned columns as the first columns whereas Calcite places them at the end.
      // We do modulo arithmetic to get the right position of the column.
      int position = (slotDesc.getColumn().getPosition() + nonPartitionedCols) % totalColumnsInTbl;
      exprs.put(position, new SlotRef(slotDesc));
    }
    return ImmutableMap.copyOf(exprs);
  }


  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw)
        .item("table", scan.getTable().getQualifiedName());

    if (filter != null) {
      rw = rw.item("condition", filter.getCondition());
    }
    return rw;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return filter != null ?
        mq.getNonCumulativeCost(filter) : mq.getNonCumulativeCost(scan);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return filter != null ?
        mq.getRowCount(filter) : mq.getRowCount(scan);
  }
}
