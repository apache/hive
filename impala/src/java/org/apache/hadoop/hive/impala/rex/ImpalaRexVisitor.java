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

package org.apache.hadoop.hive.impala.rex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This visitor can generate Impala expressions for function calls and literals.
 * It is used as a base class for other visitors, e.g.,
 * {@link ImpalaInferMappingRexVisitor} and {@link ImpalaProvidedMappingRexVisitor}.
 */
public class ImpalaRexVisitor extends RexVisitorImpl<Expr> {

  private final Analyzer analyzer;

  private final RexBuilder rexBuilder;

  protected ImpalaRexVisitor(Analyzer analyzer, RexBuilder rexBuilder) {
    super(false);
    Preconditions.checkArgument(analyzer != null);
    this.analyzer = analyzer;
    this.rexBuilder = rexBuilder;
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
      return ImpalaRexCall.getExpr(analyzer, rexCall, params, rexBuilder);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Expr visitInputRef(RexInputRef rexInputRef) {
    throw new RuntimeException("Not supported");
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

  /**
   * This visitor will generate an Impala expression generating the mappings
   * for references from the input plan nodes and possibly relying on the input
   * tuple ids to generate some of the function calls.
   */
  public static class ImpalaInferMappingRexVisitor extends ImpalaRexVisitor {

    private final List<ReferrableNode> impalaPlanNodes;
    // set of indexes of RexInputRef corresponding to the partition columns
    private final Set<Integer> partitionColsIndexes;
    // true if this expr contains partition column references
    private boolean hasPartitionCols;
    // true if this expr contains non-partition column references
    private boolean hasNonPartitionCols;

    public ImpalaInferMappingRexVisitor(Analyzer analyzer, List<ReferrableNode> impalaPlanNodes,
        RexBuilder rexBuilder) {
      this(analyzer, impalaPlanNodes, null, rexBuilder);
    }

    public ImpalaInferMappingRexVisitor(Analyzer analyzer, List<ReferrableNode> impalaPlanNodes,
        Set<Integer> partitionColsIndexes, RexBuilder rexBuilder) {
      super(analyzer, rexBuilder);
      Preconditions.checkArgument(impalaPlanNodes != null);
      this.impalaPlanNodes = impalaPlanNodes;
      this.partitionColsIndexes = partitionColsIndexes;
    }

    @Override
    public Expr visitInputRef(RexInputRef rexInputRef) {
      // first compute the index relative to the input
      int inputNum = 0;
      int numOutputExprs = 0;
      // Suppose the rexInputRef's index is $4 and there are 2
      // input relnodes r0 and r1 with their respective output exprs.
      // We want to map the $4 (a total index) into a local index which
      // is relative to either r0 or r1
      // So, the local_index = total_index - current_total_output_exprs
      // Note that the index ordinals are increasing but not necessarily consecutive.
      for (; inputNum < impalaPlanNodes.size(); inputNum++) {
        Pair<Integer, Integer> maxIndexInfo = impalaPlanNodes.get(inputNum).getMaxIndexInfo();
        if (rexInputRef.getIndex() <= numOutputExprs + maxIndexInfo.left) {
          break;
        }
        numOutputExprs += maxIndexInfo.right;
      }

      int localIndex = rexInputRef.getIndex() - numOutputExprs;
      Preconditions.checkState(inputNum < impalaPlanNodes.size());
      Expr e = impalaPlanNodes.get(inputNum).getExpr(localIndex);
      // check if this input ref is a partition column
      if (partitionColsIndexes != null) {
        if (partitionColsIndexes.contains(rexInputRef.getIndex())) {
          hasPartitionCols = true;
        } else {
          hasNonPartitionCols = true;
        }
      }
      return e;
    }

    public boolean hasPartitionColsOnly() {
      return hasPartitionCols && !hasNonPartitionCols;
    }

    public void resetPartitionState() {
      hasPartitionCols = false;
      hasNonPartitionCols = false;
    }
  }
  /**
   * This visitor will generate an Impala expression from the mappings
   * that have already been created. This visitor can map analytic
   * functions ({@link RexOver) nodes) in addition to functions, references,
   * and literals.
   */
  public static class ImpalaProvidedMappingRexVisitor extends ImpalaRexVisitor {

    private final Map<RexNode, Expr> exprsMap;

    public ImpalaProvidedMappingRexVisitor(Analyzer analyzer, Map<RexNode, Expr> exprsMap,
        RexBuilder rexBuilder) {
      super(analyzer, rexBuilder);
      Preconditions.checkArgument(exprsMap != null);
      this.exprsMap = exprsMap;
    }

    @Override
    public Expr visitInputRef(RexInputRef rexInputRef) {
      return exprsMap.get(rexInputRef);
    }

    @Override
    public Expr visitOver(RexOver over) {
      return exprsMap.get(over);
    }

  }
}
