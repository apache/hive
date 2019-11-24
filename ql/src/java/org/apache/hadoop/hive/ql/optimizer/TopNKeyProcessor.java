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
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * TopNKeyProcessor is a processor for TopNKeyOperator. A TopNKeyOperator will be placed between
 * a GroupByOperator and its following ReduceSinkOperator. If there already is a TopNKeyOperator,
 * then it will be skipped.
 */
public class TopNKeyProcessor implements NodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyProcessor.class);

  public TopNKeyProcessor() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {

    // Get ReduceSinkOperator
    ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) nd;
    ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

    // Get GroupByOperator
    GroupByOperator groupByOperator = (GroupByOperator) reduceSinkOperator.getParentOperators().get(0);
    GroupByDesc groupByDesc = groupByOperator.getConf();

    // Check whether the reduce sink operator contains top n
    if (!reduceSinkDesc.isOrdering() || reduceSinkDesc.getTopN() < 0) {
      return null;
    }

    // Currently, per partitioning top n key is not supported
    // in TopNKey operator
    if (reduceSinkDesc.isPTFReduceSink()) {
      return null;
    }

    // Check whether the group by operator is in hash mode
    if (groupByDesc.getMode() != GroupByDesc.Mode.HASH) {
      return null;
    }

    // Check whether the group by operator has distinct aggregations
    if (groupByDesc.isDistinct()) {
      return null;
    }

    // Check whether RS keys are same as GBY keys
    List<ExprNodeDesc> groupByKeyColumns = groupByDesc.getKeys();
    List<ExprNodeDesc> mappedColumns = new ArrayList<>();
    for (ExprNodeDesc columns : reduceSinkDesc.getKeyCols()) {
      mappedColumns.add(groupByDesc.getColumnExprMap().get(columns.getExprString()));
    }
    if (!ExprNodeDescUtils.isSame(mappedColumns, groupByKeyColumns)) {
      return null;
    }

    // Check whether there already is a top n key operator
    Operator<? extends OperatorDesc> parentOperator = groupByOperator.getParentOperators().get(0);
    if (parentOperator instanceof TopNKeyOperator) {
      return null;
    }

    // Insert a new top n key operator between the group by operator and its parent
    TopNKeyDesc topNKeyDesc = new TopNKeyDesc(
            reduceSinkDesc.getTopN(), reduceSinkDesc.getOrder(), reduceSinkDesc.getNullOrder(), groupByKeyColumns);
    Operator<? extends OperatorDesc> newOperator = OperatorFactory.getAndMakeChild(
            groupByOperator.getCompilationOpContext(), (OperatorDesc) topNKeyDesc,
            new RowSchema(groupByOperator.getSchema()), groupByOperator.getParentOperators());
    newOperator.getChildOperators().add(groupByOperator);
    groupByOperator.getParentOperators().add(newOperator);
    parentOperator.removeChild(groupByOperator);

    return null;
  }
}
