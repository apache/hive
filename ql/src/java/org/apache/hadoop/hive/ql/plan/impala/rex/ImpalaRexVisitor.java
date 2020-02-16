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

package org.apache.hadoop.hive.ql.plan.impala.rex;

import com.google.common.collect.Lists;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;

import java.util.List;
/**
 * Impala Rex Visitor providing Expr objects of the
 * ImpalaRexNodes.
 */
public class ImpalaRexVisitor extends RexVisitorImpl<Expr> {

  private final Analyzer analyzer;
  private final ReferrableNode impalaPlanNode;

  public ImpalaRexVisitor(Analyzer analyzer, ReferrableNode impalaPlanNode) {
    super(false);
    this.analyzer = analyzer;
    this.impalaPlanNode = impalaPlanNode;
  }

  @Override
  public Expr visitCall(RexCall rexCall) {
    try {
      RexNode removedRexNode = ImpalaRexCall.removeRedundantCast(rexCall);
      // if removedRexNode is different from original call, we know we removed the cast
      // and we return the expression of the operand within the cast.
      if (removedRexNode != rexCall) {
        return removedRexNode.accept(this);
      }
      List<Expr> params = Lists.newArrayList();
      for (RexNode operand : rexCall.getOperands()) {
        params.add(operand.accept(this));
      }
      return ImpalaRexCall.getExpr(analyzer, rexCall, params);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Expr visitInputRef(RexInputRef rexInputRef) {
    /* The input ref calls back out to plan node holding the RexNode. */
    return impalaPlanNode.getExpr(rexInputRef.getIndex());
  }

  @Override
  public Expr visitLiteral(RexLiteral rexLiteral) {
    try {
      return ImpalaRexLiteral.getExpr(analyzer, rexLiteral);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Expr visitLocalRef(RexLocalRef localRef) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitOver(RexOver over) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitFieldAccess(RexFieldAccess fieldAccess) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Expr visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new RuntimeException("Not supported");
  }
}
