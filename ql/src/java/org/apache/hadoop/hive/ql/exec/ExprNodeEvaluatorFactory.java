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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;

/**
 * ExprNodeEvaluatorFactory.
 *
 */
public final class ExprNodeEvaluatorFactory {

  private ExprNodeEvaluatorFactory() {
  }

  public static ExprNodeEvaluator get(ExprNodeDesc desc) {
    // Constant node
    if (desc instanceof ExprNodeConstantDesc) {
      return new ExprNodeConstantEvaluator((ExprNodeConstantDesc) desc);
    }
    // Column-reference node, e.g. a column in the input row
    if (desc instanceof ExprNodeColumnDesc) {
      return new ExprNodeColumnEvaluator((ExprNodeColumnDesc) desc);
    }
    // Generic Function node, e.g. CASE, an operator or a UDF node
    if (desc instanceof ExprNodeGenericFuncDesc) {
      return new ExprNodeGenericFuncEvaluator((ExprNodeGenericFuncDesc) desc);
    }
    // Field node, e.g. get a.myfield1 from a
    if (desc instanceof ExprNodeFieldDesc) {
      return new ExprNodeFieldEvaluator((ExprNodeFieldDesc) desc);
    }
    // Null node, a constant node with value NULL and no type information
    if (desc instanceof ExprNodeNullDesc) {
      return new ExprNodeNullEvaluator((ExprNodeNullDesc) desc);
    }

    throw new RuntimeException(
        "Cannot find ExprNodeEvaluator for the exprNodeDesc = " + desc);
  }
}
