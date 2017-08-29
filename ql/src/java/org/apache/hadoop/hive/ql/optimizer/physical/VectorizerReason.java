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

package org.apache.hadoop.hive.ql.optimizer.physical;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Why a node did not vectorize.
 *
 */
public class VectorizerReason  {

  private static final long serialVersionUID = 1L;

  public static enum VectorizerNodeIssue {
    NONE,
    NODE_ISSUE,
    OPERATOR_ISSUE,
    EXPRESSION_ISSUE
  }

  private final VectorizerNodeIssue vectorizerNodeIssue;

  private final Operator<? extends OperatorDesc> operator;

  private final String expressionTitle;

  private final String issue;

  private VectorizerReason(VectorizerNodeIssue vectorizerNodeIssue,
      Operator<? extends OperatorDesc> operator, String expressionTitle, String issue) {
    this.vectorizerNodeIssue = vectorizerNodeIssue;
    this.operator = operator;
    this.expressionTitle = expressionTitle;
    this.issue = issue;
  }

  public static VectorizerReason createNodeIssue(String issue) {
    return new VectorizerReason(
        VectorizerNodeIssue.NODE_ISSUE,
        null,
        null,
        issue);
  }

  public static VectorizerReason createOperatorIssue(Operator<? extends OperatorDesc> operator,
      String issue) {
    return new VectorizerReason(
        VectorizerNodeIssue.OPERATOR_ISSUE,
        operator,
        null,
        issue);
  }

  public static VectorizerReason createExpressionIssue(Operator<? extends OperatorDesc> operator,
      String expressionTitle, String issue) {
    return new VectorizerReason(
        VectorizerNodeIssue.EXPRESSION_ISSUE,
        operator,
        expressionTitle,
        issue);
  }

  @Override
  public VectorizerReason clone() {
    return new VectorizerReason(vectorizerNodeIssue, operator, expressionTitle, issue);
  }

  public VectorizerNodeIssue getVectorizerNodeIssue() {
    return vectorizerNodeIssue;
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return operator;
  }

  public String getExpressionTitle() {
    return expressionTitle;
  }

  public String getIssue() {
    return issue;
  }

  @Override
  public String toString() {
    String reason;
    switch (vectorizerNodeIssue) {
    case NODE_ISSUE:
      reason = (issue == null ? "unknown" : issue);
      break;
    case OPERATOR_ISSUE:
      reason = (operator == null ? "Unknown" : operator.getType()) + " operator: " +
           (issue == null ? "unknown" : issue);
      break;
    case EXPRESSION_ISSUE:
      reason = expressionTitle + " expression for " +
          (operator == null ? "Unknown" : operator.getType()) + " operator: " +
              (issue == null ? "unknown" : issue);
      break;
    default:
      reason = "Unknown " + vectorizerNodeIssue;
    }
    return reason;
  }
}
