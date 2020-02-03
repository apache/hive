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
package org.apache.hadoop.hive.ql.optimizer.listbucketingpruner;

import java.util.Map;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.PrunerExpressionOperatorFactory;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Expression processor factory for list bucketing pruning. Each processor tries to
 * convert the expression subtree into a list bucketing pruning expression. This
 * expression is then used to figure out which skewed value to be used
 */
public class LBExprProcFactory extends PrunerExpressionOperatorFactory {

  private LBExprProcFactory() {
    // prevent instantiation
  }

  /**
   * Processor for lbpr column expressions.
   */
  public static class LBPRColumnExprProcessor extends ColumnExprProcessor {

    @Override
    protected ExprNodeDesc processColumnDesc(NodeProcessorCtx procCtx, ExprNodeColumnDesc cd) {
      ExprNodeDesc newcd;
      LBExprProcCtx ctx = (LBExprProcCtx) procCtx;
      Partition part = ctx.getPart();
      if (cd.getTabAlias().equalsIgnoreCase(ctx.getTabAlias())
          && isPruneForListBucketing(part, cd.getColumn())) {
        newcd = cd.clone();
      } else {
        newcd = new ExprNodeConstantDesc(cd.getTypeInfo(), null);
      }
      return newcd;
    }

    /**
     * Check if we prune it for list bucketing
     * 1. column name is part of skewed column
     * 2. partition has skewed value to location map
     * @param part
     * @param columnName
     * @return
     */
    private boolean isPruneForListBucketing(Partition part, String columnName) {
      return ListBucketingPrunerUtils.isListBucketingPart(part)
          && (part.getSkewedColNames().contains(columnName));
    }
  }

  /**
   * Generates the list bucketing pruner for the expression tree.
   *
   * @param tabAlias
   *          The table alias of the partition table that is being considered
   *          for pruning
   * @param pred
   *          The predicate from which the list bucketing pruner needs to be
   *          generated
   * @param part
   *          The partition this walker is walking
   * @throws SemanticException
   */
  public static ExprNodeDesc genPruner(String tabAlias, ExprNodeDesc pred, Partition part)
      throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    NodeProcessorCtx lbprCtx = new LBExprProcCtx(tabAlias, part);

    Map<Node, Object> outputMap = PrunerUtils.walkExprTree(pred, lbprCtx, getColumnProcessor(),
        getFieldProcessor(), getGenericFuncProcessor(), getDefaultExprProcessor());

    // Get the exprNodeDesc corresponding to the first start node;
    return (ExprNodeDesc) outputMap.get(pred);
  }

  /**
   * Instantiate column processor.
   *
   * @return
   */
  public static SemanticNodeProcessor getColumnProcessor() {
    return new LBPRColumnExprProcessor();
  }
}
