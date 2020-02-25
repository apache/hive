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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import org.apache.hadoop.hive.ql.plan.impala.ImpalaBasicAnalyzer;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

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

  private List<Expr> getConjuncts(HiveFilter filter, Analyzer analyzer) {
    List<Expr> conjuncts = Lists.newArrayList();
    if (filter == null) {
      return conjuncts;
    }
    ImpalaRexVisitor visitor = new ImpalaRexVisitor(analyzer, this);
    List<RexNode> andOperands = getAndOperands(filter.getCondition());
    for (RexNode andOperand : andOperands) {
      conjuncts.add(andOperand.accept(visitor));
    }
    return conjuncts;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException,
      MetaException {
    if (hdfsScanNode != null) {
      return hdfsScanNode;
    }

    String tableName = scan.getTable().getQualifiedName().get(1);
    PlanNodeId nodeId = ctx.getNextNodeId();
    Table table = ((RelOptHiveTable) scan.getTable()).getHiveTableMD();

    // get the corresponding metastore Table object
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getTTable();
    // TODO: CDPD-8324: ideally we should cache the metastore DB object at query level
    org.apache.hadoop.hive.metastore.api.Database msDb = db.getDatabase(table.getDbName());
    StorageDescriptor sd = msTbl.getSd();
    org.apache.impala.catalog.Db impalaDb = new Db(table.getDbName(), msDb);
    HdfsTable hdfsTable = new HdfsTable(msTbl, impalaDb, tableName, table.getOwner());
    hdfsTable.load(false, db.getMSC(), msTbl, "");

    List<FeFsPartition> feFsPartitions = Lists.newArrayList();
    // TODO: CDPD-8313: supply the actual pruned partition list
    for (PrunablePartition p : hdfsTable.getPartitions()) {
      feFsPartitions.add((FeFsPartition) p);
    }

    // TODO: CDPD-8315
    // Impala passes an AggregateInfo to HdfsScanNode when there is a
    // potential for a perf optimization to be applied where a single table
    // count(*) could be satisfied by the scan directly. In the new planner
    // this could be done through a rule (not implemented yet), so for now
    // we will pass a null and re-visit this later.
    AggregateInfo aggInfo = null;
    // TODO: populate the partition conjuncts
    List<Expr> partitionConjuncts = new ArrayList<>();

    TableName impalaTblName = TableName.parse(tableName);
    String alias = null;
    TableRef tblRef = new TableRef(impalaTblName.toPath(), alias);
    ImpalaBasicAnalyzer basicAnalyzer = (ImpalaBasicAnalyzer) ctx.getRootAnalyzer();
    // save the mapping from table name to hdfsTable
    basicAnalyzer.setTable(impalaTblName, hdfsTable);
    Path resolvedPath = ctx.getRootAnalyzer().resolvePath(tblRef.getPath(), Path.PathType.TABLE_REF);

    BaseTableRef baseTblRef = new BaseTableRef(tblRef, resolvedPath);

    TupleDescriptor tupleDesc = createTupleAndSlotDesc(baseTblRef, ctx);

    this.outputExprs = createOutputExprs(tupleDesc.getSlots());

    // get the list of conjuncts from the filter
    List<Expr> assignedConjuncts = getConjuncts(filter, ctx.getRootAnalyzer());
    this.nodeInfo = new ImpalaNodeInfo(assignedConjuncts, tupleDesc);

    hdfsScanNode = new ImpalaHdfsScanNode(nodeId, feFsPartitions, baseTblRef, aggInfo, partitionConjuncts, nodeInfo);
    hdfsScanNode.init(ctx.getRootAnalyzer());

    return hdfsScanNode;
  }

  // Create tuple and slot descriptors for this base table
  private TupleDescriptor createTupleAndSlotDesc(BaseTableRef baseTblRef,
      ImpalaPlannerContext ctx) throws ImpalaException {
    // create the tuple descriptor via the analyzer
    baseTblRef.analyze(ctx.getRootAnalyzer());
    TupleDescriptor tupleDesc = baseTblRef.getDesc();

    // create the slot descriptors corresponding to this tuple descriptor
    // by supplying the field names from Calcite's output schema for this node
    RelDataType relDataType = scan.getPrunedRowType();
    List<String> fieldNames = relDataType.getFieldNames();
    for (int i = 0; i < fieldNames.size(); i++) {
      List<String> pathList = new ArrayList<>();
      pathList.add(tupleDesc.getPath().toString());
      pathList.add(fieldNames.get(i));
      SlotRef slotref = new SlotRef(pathList);
      slotref.analyze(ctx.getRootAnalyzer());
      SlotDescriptor slotDesc = slotref.getDesc();
      slotDesc.setIsMaterialized(true);
    }

    return tupleDesc;
  }

  private List<RexNode> getAndOperands(RexNode conjuncts) {
    if (conjuncts == null) {
      return ImmutableList.of();
    }

    if (!(conjuncts instanceof RexCall)) {
      return ImmutableList.of(conjuncts);
    }
    RexCall rexCallConjuncts = (RexCall) conjuncts;
    if (rexCallConjuncts.getKind() != SqlKind.AND) {
      return ImmutableList.of(conjuncts);
    }
    return rexCallConjuncts.getOperands();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = scan.explainTerms(pw);
    if (filter != null) {
      rw = filter.explainTerms(rw);
    }
    return rw;
  }
}
