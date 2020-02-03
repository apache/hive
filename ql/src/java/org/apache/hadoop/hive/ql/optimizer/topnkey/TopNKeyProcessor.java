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
package org.apache.hadoop.hive.ql.optimizer.topnkey;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Stack;

/**
 * TopNKeyProcessor is a processor for TopNKeyOperator.
 * A TopNKeyOperator will be placed before any ReduceSinkOperator which has a topN property >= 0.
 */
public class TopNKeyProcessor implements SemanticNodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyProcessor.class);
  private final int maxTopNAllowed;

  public TopNKeyProcessor(int maxTopNAllowed) {
    this.maxTopNAllowed = maxTopNAllowed;
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {

    // Get ReduceSinkOperator
    ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) nd;
    ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

    // Check whether the reduce sink operator contains top n
    if (reduceSinkDesc.getTopN() < 0 || !reduceSinkDesc.isOrdering()) {
      return null;
    }

    if (reduceSinkDesc.getTopN() > maxTopNAllowed) {
      return null;
    }

    // Check whether there already is a top n key operator
    Operator<? extends OperatorDesc> parentOperator = reduceSinkOperator.getParentOperators().get(0);
    if (parentOperator instanceof TopNKeyOperator) {
      return null;
    }

    List<ExprNodeDesc> partitionCols = Collections.emptyList();
    if (reduceSinkDesc.isPTFReduceSink()) {
      // All keys are partition keys or no keys at all
      // Note: partition cols are prefix of key cols
      if (reduceSinkDesc.getPartitionCols().size() >= reduceSinkDesc.getKeyCols().size()) {
        return null;
      }

      partitionCols = reduceSinkDesc.getPartitionCols();
    }

    TopNKeyDesc topNKeyDesc = new TopNKeyDesc(reduceSinkDesc.getTopN(), reduceSinkDesc.getOrder(),
            reduceSinkDesc.getNullOrder(), reduceSinkDesc.getKeyCols(), partitionCols);

    copyDown(reduceSinkOperator, topNKeyDesc);
    return null;
  }

  static TopNKeyOperator copyDown(Operator<? extends OperatorDesc> child, OperatorDesc operatorDesc) {
    final List<Operator<? extends OperatorDesc>> parents = child.getParentOperators();

    final Operator<? extends OperatorDesc> newOperator =
        OperatorFactory.getAndMakeChild(
            child.getCompilationOpContext(), operatorDesc,
            new RowSchema(parents.get(0).getSchema()), child.getParentOperators());

    newOperator.getChildOperators().add(child);

    for (Operator<? extends OperatorDesc> parent : parents) {
      parent.removeChild(child);
    }
    child.getParentOperators().clear();
    child.getParentOperators().add(newOperator);

    return (TopNKeyOperator) newOperator;
  }
}
