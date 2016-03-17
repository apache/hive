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

package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class PTFExpressionDef {
  String expressionTreeString;
  ExprNodeDesc exprNode;
  transient ExprNodeEvaluator exprEvaluator;
  transient ObjectInspector OI;

  public PTFExpressionDef() {}

  public PTFExpressionDef(PTFExpressionDef e) {
    expressionTreeString = e.getExpressionTreeString();
    exprNode = e.getExprNode();
    exprEvaluator = e.getExprEvaluator();
    OI = e.getOI();
  }

  public String getExpressionTreeString() {
    return expressionTreeString;
  }

  public void setExpressionTreeString(String expressionTreeString) {
    this.expressionTreeString = expressionTreeString;
  }

  public ExprNodeDesc getExprNode() {
    return exprNode;
  }

  public void setExprNode(ExprNodeDesc exprNode) {
    this.exprNode = exprNode;
  }

  @Explain(displayName = "expr", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getExprNodeExplain() {
    return exprNode == null ? null : exprNode.getExprString();
  }

  public ExprNodeEvaluator getExprEvaluator() {
    return exprEvaluator;
  }

  public void setExprEvaluator(ExprNodeEvaluator exprEvaluator) {
    this.exprEvaluator = exprEvaluator;
  }

  public ObjectInspector getOI() {
    return OI;
  }

  public void setOI(ObjectInspector oI) {
    OI = oI;
  }
}
