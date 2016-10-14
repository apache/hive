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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class represents a non leaf binary operator in the expression tree.
 */
public class FilterExprAndExpr extends VectorExpression {

  private static final long serialVersionUID = 1L;

  public FilterExprAndExpr() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    childExpressions[0].evaluate(batch);
    for (int childIndex = 1; childIndex < childExpressions.length; childIndex++) {
      childExpressions[childIndex].evaluate(batch);
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  @Override
  public String vectorExpressionParameters() {
    // The children are input.
    return null;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {

    // IMPORTANT NOTE: For Multi-AND, the VectorizationContext class will catch cases with 3 or
    //                 more parameters...

    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
