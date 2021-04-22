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
package org.apache.hadoop.hive.impala.prune;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.RulePartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.impala.plan.ImpalaBasicAnalyzer;
import org.apache.hadoop.hive.impala.plan.ImpalaQueryContext;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;
import org.apache.hadoop.hive.impala.node.ImpalaBaseTableRef;
import org.apache.hadoop.hive.impala.node.ImpalaHdfsScanRel;
import org.apache.hadoop.hive.impala.rex.ReferrableNode;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.TableName;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ImpalaRulePartitionPruner.
 *
 * Class to do the partition pruning for Impala.
 */
public class ImpalaRulePartitionPruner implements RulePartitionPruner {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaRulePartitionPruner.class);

  private final RelOptHiveTable table;
  private final ImpalaQueryContext queryContext;
  private final ImpalaBasicHdfsTable impalaTable;
  private final TupleDescriptor tupleDesc;
  private final ImpalaBasicAnalyzer analyzer;
  private final ImpalaConjuncts impalaConjuncts;
  private final HiveFilter filter;

  public ImpalaRulePartitionPruner(RelOptHiveTable table, HiveFilter filter,
      ImpalaQueryContext queryContext, RexBuilder rexBuilder)
      throws HiveException, ImpalaException, MetaException {
    this.table = table;
    this.queryContext = queryContext;
    this.filter = filter;

    Preconditions.checkNotNull(queryContext);
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getHiveTableMD().getTTable();
    ImpalaBasicHdfsTable tmpTable = queryContext.getBasicTable(msTbl);
    this.impalaTable = (tmpTable != null)
        ? tmpTable
        : ImpalaBasicTableCreator.createPartitionedTable(table, queryContext);
    if (tmpTable == null) {
      queryContext.cacheBasicTable(msTbl, impalaTable);
    }

    // Need to use a temp analyzer. The Impala PrunedPartition class requires a tableRef
    // to be registered with the analyzer. However, we don't want to pollute the analyzer
    // space with a TableRef that is going to be thrown away.
    this.analyzer = queryContext.getPruneAnalyzer();

    // The TupleDescriptor is also needed for the PrunedPartition. We can create the tuple
    // descriptor using the same method as the Scan RelNode
    this.tupleDesc = createTupleDesc();

    this.impalaConjuncts = createImpalaConjuncts(rexBuilder);
  }

  /**
   * Method to do the pruning.
   *
   * The main pruning is done in the Impala jar file inthe HdfsPartitionPruner.prune
   * method.  The partitions are then returned and placed into the ImpalaPrunedPartitionList
   * which will be accessed at translation time. The pruner takes an Impala TupleDescriptor
   * as an argument which holds access to an ImpalaBasicHdfsTable. The Pruner calls this
   * table via a callback "ImpalaBasicHdfsTable.loadPartitions()" to retrieve the partitions.
   * These partitions will also be "Basic" in that the actual metadata will not be loaded
   * until translation time. The names of the partitions that need to be loaded are stored
   * in the ImpalaBasicHdfsTable.
   */
  @Override
  public PrunedPartitionList getPartitionPruneList(HiveConf conf,
      Map<String, PrunedPartitionList> partitionCache) throws HiveException {

    List<Expr> partitionConjuncts =
        Lists.newArrayList(impalaConjuncts.getNormalizedPartitionConjuncts());
    String partitionConjunctsKey = PrunerUtils.getTableKey(table.getHiveTableMD()) +
        impalaConjuncts.createPartitionKey();
    LOG.info("Partition pruned key is : " + partitionConjunctsKey);
    PrunedPartitionList cachedValue = partitionCache.get(partitionConjunctsKey);
    if (cachedValue != null) {
      return cachedValue;
    }

    try {
      boolean doImpalaPruning = (filter != null && partitionConjuncts.size() >= 0);
      HdfsPartitionPruner pruner = doImpalaPruning ? new HdfsPartitionPruner(tupleDesc) : null;
      org.apache.impala.common.Pair<List<? extends FeFsPartition>, List<Expr>> impalaPair =
          doImpalaPruning
          ? (new HdfsPartitionPruner(tupleDesc)).prunePartitions(analyzer, partitionConjuncts, true, getTmpTableRef(queryContext, table, impalaTable, analyzer))
          : null;
      List<Expr> prunedExprs = doImpalaPruning ? impalaPair.second : new ArrayList<>();
      List<ImpalaBasicPartition> prunedPartitions = doImpalaPruning
          ? getPrunedImpalaBasicPartitions(impalaPair.first)
          : impalaTable.getAllPartitions();

      LOG.info("Number of Partition pruned expressions is " + prunedExprs.size());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Partition pruned expressions: " + ImpalaConjuncts.toString(prunedExprs));
      }

      queryContext.addBasicTableNewNames(table.getHiveTableMD().getTTable(), impalaTable,
          prunedPartitions);

      Set<Partition> msPartitions = impalaTable.fetchPartitions(
          table.getHiveTableMD(), prunedPartitions, conf);

      if (prunedExprs.size() < impalaConjuncts.getImpalaPartitionConjuncts().size()) {
        // This situation shouldn't happen. We calculated the expressions used for
        // partition pruning via the ImpalaConjuncts object, but the HdfsPartitionPruner
        // found that one of the expressions didn't meet pruning standards.
        String keptString = ImpalaConjuncts.toString(prunedExprs);
        throw new RuntimeException("Error while partition pruning: Only the following " +
            "expressions could be parsed for pruning: " + keptString);
      }
      Table t = new Table(table.getHiveTableMD().getTTable());
      PrunedPartitionList ppl = new ImpalaPrunedPartitionList(t,
          partitionConjunctsKey, msPartitions, false, prunedPartitions, impalaTable,
          impalaConjuncts);

      partitionCache.put(partitionConjunctsKey, ppl);
      partitionCache.put(PrunerUtils.getConditionKey(table.getHiveTableMD(), filter), ppl);
      return ppl;
    } catch (ImpalaException e) {
      throw new HiveException(e);
    }
  }

  /**
   * convert the pruned FeFsPartition List into an ImpalaBasicPartition List
   */
  private List<ImpalaBasicPartition> getPrunedImpalaBasicPartitions(
      List<? extends FeFsPartition> partitions) {
    List<ImpalaBasicPartition> prunedBasicPartitions = new ArrayList<>();
    for (FeFsPartition p : partitions) {
      prunedBasicPartitions.add((ImpalaBasicPartition) p);
    }
    return prunedBasicPartitions;
  }

  private ImpalaBaseTableRef getTmpTableRef(ImpalaQueryContext queryContext, RelOptHiveTable table,
      ImpalaBasicHdfsTable hdfsTable, ImpalaBasicAnalyzer analyzer)
          throws ImpalaException, HiveException {
    String tableName = table.getName();
    TableName impalaTableName = TableName.parse(tableName);
    TableRef tblRef = new TableRef(impalaTableName.toPath(), table.getName());
    analyzer.setTable(impalaTableName, (HdfsTable) hdfsTable);
    Path resolvedPath = analyzer.resolvePath(tblRef.getPath(), Path.PathType.TABLE_REF);
    return new ImpalaBaseTableRef(tblRef, resolvedPath, analyzer);
  }

  private TupleDescriptor createTupleDesc() throws HiveException, ImpalaException {
    if (filter == null) {
      return null;
    }
    BaseTableRef baseTblRef = getTmpTableRef(queryContext, table, impalaTable, analyzer);
    return ImpalaHdfsScanRel.createTupleAndSlotDesc(baseTblRef, table.getRowType(), analyzer);
  }

  private ImpalaConjuncts createImpalaConjuncts(RexBuilder rexBuilder) throws HiveException {
    if (filter == null) {
      return ImpalaConjuncts.create();
    }

    HdfsTableNode hdfsTableNode = new HdfsTableNode(tupleDesc, table);
    Set<Integer> partitionColsIndexes =
        (table.getPartColInfoMap() != null) ? table.getPartColInfoMap().keySet() : null;
    // retrieve the ImpalaConjuncts, which break up the pruneNode into "and" partition
    // conjuncts and non-partition conjuncts.
    return ImpalaConjuncts.create(filter.getCondition(), analyzer, hdfsTableNode,
          rexBuilder, partitionColsIndexes);
  }

  /**
   * ReferrableNode dummy class that will hold the outputExprs for the TableScan.
   */
  private static class HdfsTableNode implements ReferrableNode {

    private final TupleDescriptor tupleDesc;
    private final ImmutableMap<Integer, Expr> outputExprs;
    private Pair<Integer, Integer> maxIndexInfo;

    public HdfsTableNode(TupleDescriptor tupleDesc, RelOptHiveTable table) {
      this.tupleDesc = tupleDesc;
      this.outputExprs = ImpalaHdfsScanRel.createScanOutputExprs(tupleDesc.getSlots(), table);
    }

    @Override
    public Expr getExpr(int index) {
      Preconditions.checkState(this.outputExprs.containsKey(index));
      return this.outputExprs.get(index);
    }

    @Override
    public int numOutputExprs() {
      Preconditions.checkNotNull(this.outputExprs);
      return this.outputExprs.size();
    }

    @Override
    public Pair<Integer, Integer> getMaxIndexInfo() {
      return Pair.of(numOutputExprs(), numOutputExprs());
    }
  }
}
