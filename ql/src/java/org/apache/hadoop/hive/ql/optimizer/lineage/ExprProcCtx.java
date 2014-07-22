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

package org.apache.hadoop.hive.ql.optimizer.lineage;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * The processor context for the lineage information. This contains the
 * lineage context and the column info and operator information that is
 * being used for the current expression.
 */
public class ExprProcCtx implements NodeProcessorCtx {

  /**
   * The lineage context that is being populated.
   */
  private final LineageCtx lctx;

  /**
   * The input operator in case the current operator is not a leaf.
   */
  private final Operator<? extends OperatorDesc> inpOp;

  /**
   * Constructor.
   *
   * @param lctx The lineage context thatcontains the dependencies for the inputs.
   * @param inpOp The input operator to the current operator.
   */
  public ExprProcCtx(LineageCtx lctx,
    Operator<? extends OperatorDesc> inpOp) {
    this.lctx = lctx;
    this.inpOp = inpOp;
  }

  /**
   * Gets the lineage context.
   *
   * @return LineageCtx The lineage context.
   */
  public LineageCtx getLineageCtx() {
    return lctx;
  }

  /**
   * Gets the input operator.
   *
   * @return Operator The input operator - this is null in case the current
   * operator is a leaf.
   */
  public Operator<? extends OperatorDesc> getInputOperator() {
    return inpOp;
  }

  public RowResolver getResolver() {
    return lctx.getParseCtx().getOpParseCtx().get(inpOp).getRowResolver();
  }
}
