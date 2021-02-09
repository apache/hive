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
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * An expression representing a column, only children are evaluated.
 */
public class IdentityExpression extends VectorExpression {

  private static final long serialVersionUID = 1L;

  public IdentityExpression() {
  }

  public IdentityExpression(int colNum) {
    super(-1, colNum);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }
  }

  public static boolean isColumnOnly(VectorExpression ve) {
    if (ve instanceof IdentityExpression) {
      VectorExpression identityExpression = (IdentityExpression) ve;
      return (identityExpression.childExpressions == null);
    } else {
      return false;
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + outputColumnNum + ":" +
        getTypeName(outputTypeInfo, outputDataTypePhysicalVariation);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }
}
