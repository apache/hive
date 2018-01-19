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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.VectorColumnMapping;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class OperatorExplainVectorization {

  protected final VectorDesc vectorDesc;

  protected final boolean isNative;

  public OperatorExplainVectorization(VectorDesc vectorDesc, boolean isNative) {
    this.vectorDesc = vectorDesc;
    this.isNative = isNative;
  }

  public List<String> vectorExpressionsToStringList(VectorExpression[] vectorExpressions) {
    if (vectorExpressions == null) {
      return null;
    }
    List<String> vecExprList = new ArrayList<String>(vectorExpressions.length);
    for (VectorExpression vecExpr : vectorExpressions) {
      vecExprList.add(vecExpr.toString());
    }
    return vecExprList;
  }

  public String outputColumnsToStringList(VectorColumnMapping vectorColumnMapping) {
    final int size = vectorColumnMapping.getCount();
    if (size == 0) {
      return null;
    }
    int[] outputColumns = vectorColumnMapping.getOutputColumns();
    return Arrays.toString(outputColumns);
  }

  public List<String> columnMappingToStringList(VectorColumnMapping vectorColumnMapping) {
    final int size = vectorColumnMapping.getCount();
    if (size == 0) {
      return null;
    }
    int[] inputColumns = vectorColumnMapping.getInputColumns();
    int[] outputColumns = vectorColumnMapping.getOutputColumns();
    ArrayList<String> result = new ArrayList<String>(size);
    for (int i = 0; i < size; i++) {
      result.add(inputColumns[i] + " -> " + outputColumns[i]);
    }
    return result;
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "className", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public String getClassName() {
    return vectorDesc.getVectorOpClass().getSimpleName();
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "native", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public boolean getNative() {
    return isNative;
  }
}