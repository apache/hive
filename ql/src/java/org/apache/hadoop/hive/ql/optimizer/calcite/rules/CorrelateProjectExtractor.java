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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A visitor for relational expressions that extracts a {@link org.apache.calcite.rel.core.Project}, with a "simple"
 * computation over the correlated variables, from the right side of a correlation
 * ({@link org.apache.calcite.rel.core.Correlate}) and places it on the left side.
 *
 * <h3>Plan before</h3>
 * <pre>
 * LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])
 *   LogicalTableScan(table=[[scott, EMP]])
 *   LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])
 *     LogicalTableScan(table=[[scott, DEPT]])
 * </pre>
 *
 * <h3>Plan after</h3>
 * <pre>
 * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],... DNAME=[$10], LOC=[$11])
 *   LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{8}])
 *     LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], ... COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])
 *       LogicalTableScan(table=[[scott, EMP]])
 *     LogicalFilter(condition=[=($0, $cor0.$f8)])
 *       LogicalTableScan(table=[[scott, DEPT]])
 * </pre>
 *
 * Essentially this transformation moves the computation over a correlated expression from the inner
 * loop to the outer loop. It materializes the computation on the left side and flattens expressions
 * on correlated variables on the right side.
 */
public final class CorrelateProjectExtractor extends RelHomogeneousShuttle {

  private final RelBuilderFactory builderFactory;

  public CorrelateProjectExtractor(RelBuilderFactory factory) {
    this.builderFactory = factory;
  }

  @Override public RelNode visit(LogicalCorrelate correlate) {
    RelNode left = correlate.getLeft().accept(this);
    RelNode right = correlate.getRight().accept(this);
    int oldLeft = left.getRowType().getFieldCount();
    // Find the correlated expressions from the right side that can be moved to the left
    Set<RexNode> callsWithCorrelationInRight =
        findCorrelationDependentCalls(correlate.getCorrelationId(), right);
    boolean isTrivialCorrelation =
        callsWithCorrelationInRight.stream().allMatch(exp -> exp instanceof RexFieldAccess);
    // Early exit condition
    if (isTrivialCorrelation) {
      if (correlate.getLeft().equals(left) && correlate.getRight().equals(right)) {
        return correlate;
      } else {
        return correlate.copy(
            correlate.getTraitSet(),
            left,
            right,
            correlate.getCorrelationId(),
            correlate.getRequiredColumns(),
            correlate.getJoinType());
      }
    }

    RelBuilder builder = builderFactory.create(correlate.getCluster(), null);
    // Transform the correlated expression from the right side to an expression over the left side
    builder.push(left);

    List<RexNode> callsWithCorrelationOverLeft = new ArrayList<>();
    for (RexNode callInRight : callsWithCorrelationInRight) {
      callsWithCorrelationOverLeft.add(replaceCorrelationsWithInputRef(callInRight, builder));
    }
    builder.projectPlus(callsWithCorrelationOverLeft);

    // Construct the mapping to transform the expressions in the right side based on the new
    // projection in the left side.
    Map<RexNode, RexNode> transformMapping = new HashMap<>();
    for (RexNode callInRight : callsWithCorrelationInRight) {
      RexBuilder xb = builder.getRexBuilder();
      RexNode v = xb.makeCorrel(builder.peek().getRowType(), correlate.getCorrelationId());
      RexNode flatCorrelationInRight = xb.makeFieldAccess(v, oldLeft + transformMapping.size());
      transformMapping.put(callInRight, flatCorrelationInRight);
    }

    // Select the required fields/columns from the left side of the correlation. Based on the code
    // above all these fields should be at the end of the left relational expression.
    List<RexNode> requiredFields =
        builder.fields(
            ImmutableBitSet.range(oldLeft, oldLeft + callsWithCorrelationOverLeft.size()).asList());

    final int newLeft = builder.fields().size();
    // Transform the expressions in the right side using the mapping constructed earlier.
    right = replaceExpressionsUsingMap(right, transformMapping);
    builder.push(right);

    builder.correlate(correlate.getJoinType(), correlate.getCorrelationId(), requiredFields);
    // Remove the additional fields that were added for the needs of the correlation to keep the old
    // and new plan equivalent.
    List<Integer> retainFields;
    switch (correlate.getJoinType()) {
    case SEMI:
    case ANTI:
      retainFields = ImmutableBitSet.range(0, oldLeft).asList();
      break;
    case LEFT:
    case INNER:
      retainFields =
          ImmutableBitSet.builder()
              .set(0, oldLeft)
              .set(newLeft, newLeft + right.getRowType().getFieldCount())
              .build()
              .asList();
      break;
    default:
      throw new AssertionError(correlate.getJoinType());
    }
    builder.project(builder.fields(retainFields));
    return builder.build();
  }

  /**
   * Traverses a plan and finds all simply correlated row expressions with the specified id.
   */
  private static Set<RexNode> findCorrelationDependentCalls(CorrelationId corrId, RelNode plan) {
    SimpleCorrelationCollector finder = new SimpleCorrelationCollector(corrId);
    plan.accept(new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode other) {
        if (other instanceof Project || other instanceof Filter) {
          other.accept(finder);
        }
        return super.visit(other);
      }
    });
    return finder.correlations;
  }

  /**
   * Replaces all row expressions in the plan using the provided mapping.
   *
   * @param plan the relational expression on which we want to perform the replacements.
   * @param mapping a mapping defining how to replace row expressions in the plan
   * @return a new relational expression where all expressions present in the mapping are replaced.
   */
  private static RelNode replaceExpressionsUsingMap(RelNode plan, Map<RexNode, RexNode> mapping) {
    CallReplacer replacer = new CallReplacer(mapping);
    return plan.accept(new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode other) {
        RelNode mNode = super.visitChildren(other);
        return mNode.accept(replacer);
      }
    });
  }

  /**
   * A collector of simply correlated row expressions.
   *
   * The shuttle traverses the tree and collects all calls and field accesses that are classified
   * as simply correlated expressions. Multiple nodes in a call hierarchy may satisfy the criteria
   * of a simple correlation so we peek the expressions closest to the root.
   *
   * @see SimpleCorrelationDetector
   */
  private static final class SimpleCorrelationCollector extends RexShuttle {
    private final CorrelationId correlationId;
    // Clients are iterating over the collection thus it is better to use LinkedHashSet to keep
    // plans stable among executions.
    private final Set<RexNode> correlations = new LinkedHashSet<>();

    SimpleCorrelationCollector(CorrelationId corrId) {
      this.correlationId = corrId;
    }

    @Override public RexNode visitCall(RexCall call) {
      if (isSimpleCorrelatedExpression(call, correlationId)) {
        correlations.add(call);
        return call;
      } else {
        return super.visitCall(call);
      }
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (isSimpleCorrelatedExpression(fieldAccess, correlationId)) {
        correlations.add(fieldAccess);
        return fieldAccess;
      } else {
        return super.visitFieldAccess(fieldAccess);
      }
    }
  }

  /**
   * Returns whether the specified node is a simply correlated expression.
   */
  private static boolean isSimpleCorrelatedExpression(RexNode node, CorrelationId id) {
    Boolean r = node.accept(new SimpleCorrelationDetector(id));
    return r == null ? Boolean.FALSE : r;
  }

  /**
   * A visitor classifying row expressions as simply correlated if they satisfy the conditions
   * below.
   * <ul>
   * <li>all correlated variables have the specified correlation id</li>
   * <li>all leafs are either correlated variables, dynamic parameters, or literals</li>
   * <li>intermediate nodes are either calls or field access expressions</li>
   * </ul>
   *
   * Examples:
   * <pre>
   * +(10, $cor0.DEPTNO) -> TRUE
   * /(100,+(10, $cor0.DEPTNO)) -> TRUE
   * CAST(+(10, $cor0.DEPTNO)):INTEGER NOT NULL -> TRUE
   * +($0, $cor0.DEPTNO) -> FALSE
   * </pre>
   *
   */
  private static class SimpleCorrelationDetector extends RexVisitorImpl<Boolean> {
    private final CorrelationId corrId;

    private SimpleCorrelationDetector(CorrelationId corrId) {
      super(true);
      this.corrId = corrId;
    }

    @Override public Boolean visitOver(RexOver over) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitSubQuery(RexSubQuery subQuery) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitCall(RexCall call) {
      Boolean hasSimpleCorrelation = null;
      for (RexNode op : call.operands) {
        Boolean b = op.accept(this);
        if (b != null) {
          hasSimpleCorrelation = hasSimpleCorrelation == null ? b : hasSimpleCorrelation && b;
        }
      }
      return hasSimpleCorrelation == null ? Boolean.FALSE : hasSimpleCorrelation;
    }

    @Override public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override public Boolean visitInputRef(RexInputRef inputRef) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return correlVariable.id.equals(corrId);
    }

    @Override public Boolean visitTableInputRef(RexTableInputRef ref) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitLocalRef(RexLocalRef localRef) {
      return Boolean.FALSE;
    }

    @Override public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return Boolean.FALSE;
    }
  }

  private static RexNode replaceCorrelationsWithInputRef(RexNode exp, RelBuilder b) {
    return exp.accept(new RexShuttle() {
      @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
          return b.field(fieldAccess.getField().getIndex());
        }
        return super.visitFieldAccess(fieldAccess);
      }
    });
  }

  /**
   * A visitor traversing row expressions and replacing calls with other expressions according
   * to the specified mapping.
   */
  private static final class CallReplacer extends RexShuttle {
    private final Map<RexNode, RexNode> mapping;

    CallReplacer(Map<RexNode, RexNode> mapping) {
      this.mapping = mapping;
    }

    @Override public RexNode visitCall(RexCall oldCall) {
      RexNode newCall = mapping.get(oldCall);
      if (newCall != null) {
        return newCall;
      } else {
        return super.visitCall(oldCall);
      }
    }
  }
}
