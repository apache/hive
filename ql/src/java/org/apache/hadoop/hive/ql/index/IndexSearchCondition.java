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
package org.apache.hadoop.hive.ql.index;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * IndexSearchCondition represents an individual search condition
 * found by {@link IndexPredicateAnalyzer}.
 *
 * @deprecated kept only because some storagehandlers are using it internally
 */
@Deprecated
public class IndexSearchCondition
{
  private ExprNodeColumnDesc columnDesc;
  private String comparisonOp;
  private ExprNodeConstantDesc constantDesc;
  private ExprNodeGenericFuncDesc indexExpr;
  private ExprNodeGenericFuncDesc originalExpr;

  private String[] fields;

  public IndexSearchCondition(
      ExprNodeColumnDesc columnDesc,
      String comparisonOp,
      ExprNodeConstantDesc constantDesc,
      ExprNodeGenericFuncDesc comparisonExpr) {
    this(columnDesc, comparisonOp, constantDesc, comparisonExpr, comparisonExpr, null);
  }

  /**
   * Constructs a search condition, which takes the form
   * <pre>column-ref comparison-op constant-value</pre>.
   *
   * @param columnDesc column being compared
   *
   * @param comparisonOp comparison operator, e.g. "="
   * (taken from GenericUDFBridge.getUdfName())
   *
   * @param constantDesc constant value to search for
   *
   * @param indexExpr the comparison expression for the index
   *
   * @param originalExpr the original comparison expression
   */
  public IndexSearchCondition(
    ExprNodeColumnDesc columnDesc,
    String comparisonOp,
    ExprNodeConstantDesc constantDesc,
    ExprNodeGenericFuncDesc indexExpr,
    ExprNodeGenericFuncDesc originalExpr,
    String[] fields) {

    this.columnDesc = columnDesc;
    this.comparisonOp = comparisonOp;
    this.constantDesc = constantDesc;
    this.indexExpr = indexExpr;
    this.originalExpr = originalExpr;
    this.fields = fields;
  }

  public void setColumnDesc(ExprNodeColumnDesc columnDesc) {
    this.columnDesc = columnDesc;
  }

  public ExprNodeColumnDesc getColumnDesc() {
    return columnDesc;
  }

  public void setComparisonOp(String comparisonOp) {
    this.comparisonOp = comparisonOp;
  }

  public String getComparisonOp() {
    return comparisonOp;
  }

  public void setConstantDesc(ExprNodeConstantDesc constantDesc) {
    this.constantDesc = constantDesc;
  }

  public ExprNodeConstantDesc getConstantDesc() {
    return constantDesc;
  }

  public void setIndexExpr(ExprNodeGenericFuncDesc indexExpr) {
    this.indexExpr = indexExpr;
  }

  public ExprNodeGenericFuncDesc getIndexExpr() {
    return indexExpr;
  }

  public void setOriginalExpr(ExprNodeGenericFuncDesc originalExpr) {
    this.originalExpr = originalExpr;
  }

  public ExprNodeGenericFuncDesc getOriginalExpr() {
    return originalExpr;
  }

  public String[] getFields() {
    return fields;
  }

  @Override
  public String toString() {
    return indexExpr.getExprString();
  }
}
