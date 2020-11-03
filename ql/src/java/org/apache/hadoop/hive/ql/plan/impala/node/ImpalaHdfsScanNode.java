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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.HdfsScanNode;
import org.apache.impala.planner.PlanNodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ImpalaHdfsScanNode extends HdfsScanNode {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaHdfsScanNode.class);

  private ImpalaNodeInfo nodeInfo;

  public ImpalaHdfsScanNode(PlanNodeId id, List<? extends FeFsPartition> partitions,
      TableRef hdfsTblRef, AggregateInfo aggInfo, List<Expr> partConjuncts,
      ImpalaNodeInfo nodeInfo) {
    super(id, nodeInfo.getTupleDesc(), nodeInfo.getAssignedConjuncts(),
        partitions, hdfsTblRef, aggInfo, partConjuncts);
    this.nodeInfo = nodeInfo;
    if (LOG.isDebugEnabled()) {
      if (partitions != null && partitions.size() > 1) {
        String names = "";
        for (int i = 0; i < partitions.size() && i < 5; i++) {
          names += partitions.get(i).getPartitionName() + " ";
        }
        LOG.debug("Table: " + hdfsTblRef.getTable().getName() + " First 5 partitions: " + names);
      }
    }
  }

  @Override
  public void assignConjuncts(Analyzer analyzer) {
    // ignore analyzer and retrieve the conjuncts from Hive
    this.conjuncts_ = nodeInfo.getAssignedConjuncts();
  }

  public List<FunctionCallExpr> checkAndApplyCountStarOptimization(
      MultiAggregateInfo multiAggInfo, Analyzer analyzer,
      FunctionCallExpr countStarExpr) throws ImpalaException, HiveException {
    // Only optimize scan/agg plan if there is a single aggregation class.
    // (This mirrors the check done by Impala's single node planner)
    AggregateInfo scanAggInfo = null;
    if (multiAggInfo != null && multiAggInfo.getMaterializedAggClasses().size() == 1) {
      scanAggInfo = multiAggInfo.getMaterializedAggClass(0);
    }
    aggInfo_ = scanAggInfo;
    if (canApplyCountStarOptimization(analyzer, getFileFormats())) {
      TupleDescriptor desc = getTupleDesc();
      Preconditions.checkNotNull(desc.getPath().destTable());
      Preconditions.checkState(getCollectionConjuncts().isEmpty());
      // before applying the count(*) reset the mem layout because a slot cannot
      // be added to a tuple if it already has a mem layout.. this is recomputed
      // down below
      desc.resetMemLayout();
      // Supply our own countStarExpr instead of letting Impala create one. This
      // allows equality checks of the count expr to succeed within Impala.
      countStarSlot_ = applyCountStarOptimization(analyzer, countStarExpr);
      scanAggInfo.substitute(getOptimizedAggSmap(), analyzer);
      scanAggInfo.getMergeAggInfo().substitute(getOptimizedAggSmap(), analyzer);
      // re-compute the mem layout
      desc.computeMemLayout();
      return scanAggInfo.getAggregateExprs();
    }
    return null;
  }

}
