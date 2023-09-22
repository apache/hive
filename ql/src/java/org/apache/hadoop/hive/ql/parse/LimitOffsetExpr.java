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

package org.apache.hadoop.hive.ql.parse;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;

public class LimitOffsetExpr {
  private final QBParseInfo qbp;

  public LimitOffsetExpr(QBParseInfo qbp) {
    this.qbp = qbp;
  }

  public void setDestLimitOffset(String dest, Integer offsetValue, Integer limitValue) {
    this.qbp.setDestLimit(dest, offsetValue, limitValue);
  }
  public void setDestLimitOffset(String dest, ASTNode offsetExpr, ASTNode limitExpr) {
    this.qbp.setDestLimit(dest, offsetExpr, limitExpr);
  }

  public Integer getDestLimit(RowResolver inputRR, String dest) throws SemanticException {
    Integer limitValue = this.qbp.getDestLimit(dest);
    ASTNode limitExpr = this.qbp.getDestLimitAST(dest);
    if (limitValue == null && limitExpr != null) {
      try {
        limitValue = (Integer) ExprNodeDescUtils.genValueFromConstantExpr(inputRR, limitExpr);
      } catch (SemanticException e) {
        throw new SemanticException("Only constant expressions are supported for limit clause");
      }
    }
    return limitValue;
  }

  public Integer getDestOffset(RowResolver inputRR, String dest) throws SemanticException {
    Integer offsetValue = this.qbp.getDestLimitOffset(dest);
    ASTNode offsetExpr = this.qbp.getDestOffsetAST(dest);
    if (offsetValue == null && offsetExpr != null) {
      try {
        offsetValue = (Integer) ExprNodeDescUtils.genValueFromConstantExpr(inputRR, offsetExpr);
      } catch (SemanticException e) {
        throw new SemanticException("Only constant expressions are supported for offset clause");
      }
    }
    return offsetValue;
  }
}
