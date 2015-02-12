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

package org.apache.hadoop.hive.ql.optimizer.index;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Factory of methods used by {@link RewriteGBUsingIndex}
 * to determine if the rewrite optimization can be applied to the input query.
 *
 */
public final class RewriteCanApplyProcFactory {
  public static CheckTableScanProc canApplyOnTableScanOperator(TableScanOperator topOp) {
    return new CheckTableScanProc();
  }

  private static class CheckTableScanProc implements NodeProcessor {
    public CheckTableScanProc() {
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      RewriteCanApplyCtx canApplyCtx = (RewriteCanApplyCtx) ctx;
      for (Node node : stack) {
        // For table scan operator,
        // check ReferencedColumns to make sure that only the index column is
        // selected for the following operators.
        if (node instanceof TableScanOperator) {
          TableScanOperator ts = (TableScanOperator) node;
          canApplyCtx.setTableScanOperator(ts);
          List<String> selectColumns = ts.getConf().getReferencedColumns();
          if (selectColumns == null || selectColumns.size() != 1) {
            canApplyCtx.setSelClauseColsFetchException(true);
            return null;
          } else {
            canApplyCtx.setIndexKey(selectColumns.get(0));
          }
        } else if (node instanceof SelectOperator) {
          // For select operators in the stack, we just add them
          if (canApplyCtx.getSelectOperators() == null) {
            canApplyCtx.setSelectOperators(new ArrayList<SelectOperator>());
          }
          canApplyCtx.getSelectOperators().add((SelectOperator) node);
        } else if (node instanceof GroupByOperator) {
          if (canApplyCtx.getGroupByOperators() == null) {
            canApplyCtx.setGroupByOperators(new ArrayList<GroupByOperator>());
          }
          // According to the pre-order,
          // the first GroupbyOperator is the one before RS
          // and the second one is the one after RS
          GroupByOperator operator = (GroupByOperator) node;
          canApplyCtx.getGroupByOperators().add(operator);
          if (!canApplyCtx.isQueryHasGroupBy()) {
            canApplyCtx.setQueryHasGroupBy(true);
            GroupByDesc conf = operator.getConf();
            List<AggregationDesc> aggrList = conf.getAggregators();
            if (aggrList == null || aggrList.size() != 1
                || !("count".equals(aggrList.get(0).getGenericUDAFName()))) {
              // In the current implementation, we make sure that only count is
              // in the function
              canApplyCtx.setAggFuncIsNotCount(true);
              return null;
            } else {
              List<ExprNodeDesc> para = aggrList.get(0).getParameters();
              if (para == null || para.size() == 0 || para.size() > 1) {
                canApplyCtx.setAggParameterException(true);
                return null;
              } else {
                ExprNodeDesc expr = ExprNodeDescUtils.backtrack(para.get(0), operator,
                    (Operator<OperatorDesc>) stack.get(0));
                if (!(expr instanceof ExprNodeColumnDesc)) {
                  canApplyCtx.setAggParameterException(true);
                  return null;
                }
              }
            }
          }
        }
      }
      return null;
    }
  }
}
