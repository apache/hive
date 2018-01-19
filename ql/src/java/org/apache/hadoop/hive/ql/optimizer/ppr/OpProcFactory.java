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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.PrunerOperatorFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Operator factory for partition pruning processing of operator graph We find
 * all the filter operators that appear just beneath the table scan operators.
 * We then pass the filter to the partition pruner to construct a pruner for
 * that table alias and store a mapping from the table scan operator to that
 * pruner. We call that pruner later during plan generation.
 *
 *
 * Refactor:
 * Move main logic to PrunerOperatorFactory. OpProcFactory extends it to reuse logic.
 *
 * Any other pruner can reuse it by creating a class extending from PrunerOperatorFactory.
 *
 * Only specific logic is in generatePredicate(..) which is in its own class like OpProcFactory.
 */
public final class OpProcFactory extends PrunerOperatorFactory {

  /**
   * Determines the partition pruner for the filter. This is called only when
   * the filter follows a table scan operator.
   */
  public static class FilterPPR extends FilterPruner {

    @Override
    protected void generatePredicate(NodeProcessorCtx procCtx, FilterOperator fop,
        TableScanOperator top) throws SemanticException, UDFArgumentException {
      OpWalkerCtx owc = (OpWalkerCtx) procCtx;
      // Otherwise this is not a sampling predicate and we need to
      ExprNodeDesc predicate = fop.getConf().getPredicate();
      String alias = top.getConf().getAlias();

      // Generate the partition pruning predicate
      ExprNodeDesc ppr_pred = ExprProcFactory.genPruner(alias, predicate);

      // Add the pruning predicate to the table scan operator
      addPruningPred(owc.getOpToPartPruner(), top, ppr_pred);
    }

  }

  public static NodeProcessor getFilterProc() {
    return new FilterPPR();
  }

  private OpProcFactory() {
    // prevent instantiation
  }
}
