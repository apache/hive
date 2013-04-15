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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.Map;

import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.PrunerExpressionOperatorFactory;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Expression processor factory for partition pruning. Each processor tries to
 * convert the expression subtree into a partition pruning expression. This
 * expression is then used to figure out whether a particular partition should
 * be scanned or not.
 *
 *  * Refactor:
 * Move main logic to PrunerExpressionOperatorFactory. ExprProcFactory extends it to reuse logic.
 *
 * Any other pruner can reuse it by creating a class extending from PrunerExpressionOperatorFactory.
 *
 * Only specific logic is in genPruner(..) which is in its own class like ExprProcFactory.
 *
 */
public final class ExprProcFactory extends PrunerExpressionOperatorFactory {

  private ExprProcFactory() {
    // prevent instantiation
  }

  /**
   * Processor for ppr column expressions.
   */
  public static class PPRColumnExprProcessor extends ColumnExprProcessor {

    @Override
    protected ExprNodeDesc processColumnDesc(NodeProcessorCtx procCtx, ExprNodeColumnDesc cd) {
      ExprNodeDesc newcd;
      ExprProcCtx epc = (ExprProcCtx) procCtx;
      if (cd.getTabAlias().equalsIgnoreCase(epc.getTabAlias())
          && cd.getIsPartitionColOrVirtualCol()) {
        newcd = cd.clone();
      } else {
        newcd = new ExprNodeConstantDesc(cd.getTypeInfo(), null);
        epc.setHasNonPartCols(true);
      }
      return newcd;
    }
  }

  /**
   * Instantiate column processor.
   *
   * @return
   */
  public static NodeProcessor getColumnProcessor() {
    return new PPRColumnExprProcessor();
  }

  /**
   * Generates the partition pruner for the expression tree.
   *
   * @param tabAlias
   *          The table alias of the partition table that is being considered
   *          for pruning
   * @param pred
   *          The predicate from which the partition pruner needs to be
   *          generated
   * @return hasNonPartCols returns true/false depending upon whether this pred
   *         has a non partition column
   * @throws SemanticException
   */
  public static ExprNodeDesc genPruner(String tabAlias, ExprNodeDesc pred,
      boolean hasNonPartCols) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    ExprProcCtx pprCtx = new ExprProcCtx(tabAlias);

    /* Move common logic to PrunerUtils.walkExprTree(...) so that it can be reused. */
    Map<Node, Object> outputMap = PrunerUtils.walkExprTree(pred, pprCtx, getColumnProcessor(),
        getFieldProcessor(), getGenericFuncProcessor(), getDefaultExprProcessor());

    hasNonPartCols = pprCtx.getHasNonPartCols();

    // Get the exprNodeDesc corresponding to the first start node;
    return (ExprNodeDesc) outputMap.get(pred);
  }

}
