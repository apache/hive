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
package org.apache.hadoop.hive.ql.plan.impala.prune;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.base.Preconditions;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulePartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.RulePartitionPruner;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaBasicAnalyzer;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaQueryContext;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaConjuncts;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaBaseTableRef;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaHdfsScanRel;
import org.apache.hadoop.hive.ql.plan.impala.rex.ReferrableNode;
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
  private final BaseTableRef baseTblRef;
  private final TupleDescriptor tupleDesc;
  private final ImpalaBasicAnalyzer analyzer;
  private final ImpalaConjuncts impalaConjuncts;

  public ImpalaRulePartitionPruner(RelOptHiveTable table, HiveFilter filter,
      ImpalaQueryContext queryContext, RexBuilder rexBuilder)
      throws HiveException, ImpalaException, MetaException {
    this.table = table;
    this.queryContext = queryContext;

    RexNode pruneNode = (filter == null) ? null : filter.getCondition();
    Preconditions.checkNotNull(queryContext);
    this.impalaTable =
        ImpalaBasicTableCreator.createAndCache(table, queryContext, table.getMSC());

    // Need to use a temp analyzer. The Impala PrunedPartition class requires a tableRef
    // to be registered with the analyzer. However, we don't want to pollute the analyzer
    // space with a TableRef that is going to be thrown away.
    this.analyzer = queryContext.getPruneAnalyzer();
    // Create temporary table ref that references our "Prune" HdfsTable (not the real
    // hdfs table).
    this.baseTblRef = getTmpTableRef(queryContext, table, impalaTable, analyzer);
    // The TupleDescriptor is also needed for the PrunedPartition. We can create the tuple
    // descriptor using the same method as the Scan RelNode
    this.tupleDesc =
          ImpalaHdfsScanRel.createTupleAndSlotDesc(baseTblRef, table.getRowType(), analyzer);
    HdfsTableNode hdfsTableNode = new HdfsTableNode(tupleDesc, table);
    Set<Integer> partitionColsIndexes =
        (table.getPartColInfoMap() != null) ? table.getPartColInfoMap().keySet() : null;
    // retrieve the ImpalaConjuncts, which break up the pruneNode into "and" partition
    // conjuncts and non-partition conjuncts.
    this.impalaConjuncts = ImpalaConjuncts.create(pruneNode, analyzer, hdfsTableNode,
          rexBuilder, partitionColsIndexes);
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
  public PrunedPartitionList prune(HiveConf conf, Map<String, PrunedPartitionList> partitionCache)
      throws HiveException {

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
      HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);

      Table t = new Table(table.getHiveTableMD().getTTable());

      List<ImpalaBasicPartition> impalaPartitions = Lists.newArrayList();
      org.apache.impala.common.Pair<List<? extends FeFsPartition>, List<Expr>> pair =
          pruner.prunePartitions(analyzer, partitionConjuncts, true);
      // The partitions retrieved go through Impala's code which is of type "FeFsPartition".
      // However, we need it to be of type ImpalaBasicPartition, so we need to iterate and
      // cast.
      for (FeFsPartition p : pair.first) {
        impalaPartitions.add((ImpalaBasicPartition) p);
      }

      LOG.info("Partition pruned expressions: " + ImpalaConjuncts.toString(pair.second));

      Set<Partition> msPartitions = impalaTable.fetchPartitions(table.getMSC(),
          table.getHiveTableMD(), pair.first);

      if (pair.second.size() < impalaConjuncts.getImpalaPartitionConjuncts().size()) {
        // This situation shouldn't happen. We calculated the expressions used for
        // partition pruning via the ImpalaConjuncts object, but the HdfsPartitionPruner
        // found that one of the expressions didn't meet pruning standards.
        String keptString = ImpalaConjuncts.toString(pair.second);
        throw new RuntimeException("Error while partition pruning: Only the following " +
            "expressions could be parsed for pruning: " + keptString);
      }
      PrunedPartitionList ppl = new ImpalaPrunedPartitionList(t, partitionConjunctsKey, msPartitions,
          false, impalaPartitions, impalaTable, impalaConjuncts);
      partitionCache.put(partitionConjunctsKey, ppl);
      return ppl;
    } catch (ImpalaException|MetaException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public PrunedPartitionList getNonPruneList(HiveConf conf,
      Map<String, PrunedPartitionList> partitionCache) throws HiveException {
    Preconditions.checkNotNull(table);
    String tableKey = PrunerUtils.getTableKey(table.getHiveTableMD());
    PrunedPartitionList cachedValue = partitionCache.get(tableKey);
    if (cachedValue != null) {
      return cachedValue;
    }
    Table t = new Table(table.getHiveTableMD().getTTable());
    PrunedPartitionList ppl = new ImpalaPrunedPartitionList(t, tableKey,
        Sets.newHashSet(new Partition(table.getHiveTableMD())), false, Lists.newArrayList(),
        impalaTable, impalaConjuncts);
    partitionCache.put(tableKey, ppl);
    return ppl;
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
