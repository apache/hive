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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;


public class HivePreFilteringRule extends RelOptRule {

  protected static final Log LOG = LogFactory
          .getLog(HivePreFilteringRule.class.getName());


  public static final HivePreFilteringRule INSTANCE =
          new HivePreFilteringRule();

  private final FilterFactory filterFactory;


  private static final Set<String> COMPARISON_UDFS = Sets.newHashSet(
          GenericUDFOPEqual.class.getAnnotation(Description.class).name(),
          GenericUDFOPEqualNS.class.getAnnotation(Description.class).name(),
          GenericUDFOPEqualOrGreaterThan.class.getAnnotation(Description.class).name(),
          GenericUDFOPEqualOrLessThan.class.getAnnotation(Description.class).name(),
          GenericUDFOPGreaterThan.class.getAnnotation(Description.class).name(),
          GenericUDFOPLessThan.class.getAnnotation(Description.class).name(),
          GenericUDFOPNotEqual.class.getAnnotation(Description.class).name());
  private static final String IN_UDF =
          GenericUDFIn.class.getAnnotation(Description.class).name();
  private static final String BETWEEN_UDF =
          GenericUDFBetween.class.getAnnotation(Description.class).name();


  private HivePreFilteringRule() {
    super(operand(Filter.class,
            operand(RelNode.class, any())));
    this.filterFactory = HiveFilter.DEFAULT_FILTER_FACTORY;
  }

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final RelNode filterChild = call.rel(1);

    // 0. If the filter is already on top of a TableScan,
    //    we can bail out
    if (filterChild instanceof TableScan) {
      return;
    }

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    final RexNode condition = RexUtil.pullFactors(rexBuilder, filter.getCondition());

    // 1. We extract possible candidates to be pushed down
    List<RexNode> commonOperands = new ArrayList<>();
    switch (condition.getKind()) {
      case AND:
        ImmutableList<RexNode> operands = RexUtil.flattenAnd(((RexCall) condition).getOperands());
        for (RexNode operand: operands) {
          if (operand.getKind() == SqlKind.OR) {
            commonOperands.addAll(extractCommonOperands(rexBuilder,operand));
          }
        }
        break;
      case OR:
        commonOperands = extractCommonOperands(rexBuilder,condition);
        break;
      default:
        return;
    }

    // 2. If we did not generate anything for the new predicate, we bail out
    if (commonOperands.isEmpty()) {
      return;
    }

    // 3. If the new conjuncts are already present in the plan, we bail out
    final RelOptPredicateList predicates = RelMetadataQuery.getPulledUpPredicates(filter);
    final List<RexNode> newConjuncts = new ArrayList<>();
    for (RexNode commonOperand : commonOperands) {
      boolean found = false;
      for (RexNode conjunct : predicates.pulledUpPredicates) {
        if (commonOperand.toString().equals(conjunct.toString())) {
          found = true;
          break;
        }
      }
      if (!found) {
        newConjuncts.add(commonOperand);
      }
    }
    if (newConjuncts.isEmpty()) {
      return;
    }

    // 4. Otherwise, we create a new condition
    final RexNode newCondition = RexUtil.pullFactors(rexBuilder,
            RexUtil.composeConjunction(rexBuilder, newConjuncts, false));

    // 5. We create the new filter that might be pushed down
    RelNode newFilter = filterFactory.createFilter(filterChild, newCondition);
    RelNode newTopFilter = filterFactory.createFilter(newFilter, condition);

    call.transformTo(newTopFilter);

  }

  private static List<RexNode> extractCommonOperands(RexBuilder rexBuilder, RexNode condition) {
    assert condition.getKind() == SqlKind.OR;
    Multimap<String,RexNode> reductionCondition = LinkedHashMultimap.create();

    // Data structure to control whether a certain reference is present in every operand
    Set<String> refsInAllOperands = null;

    // 1. We extract the information necessary to create the predicate for the new
    //    filter; currently we support comparison functions, in and between
    ImmutableList<RexNode> operands = RexUtil.flattenOr(((RexCall) condition).getOperands());
    for (int i = 0; i < operands.size(); i++) {
      final RexNode operand = operands.get(i);

      final RexNode operandCNF = RexUtil.toCnf(rexBuilder, operand);
      final List<RexNode> conjunctions = RelOptUtil.conjunctions(operandCNF);

      Set<String> refsInCurrentOperand = Sets.newHashSet();
      for (RexNode conjunction: conjunctions) {
        // We do not know what it is, we bail out for safety
        if (!(conjunction instanceof RexCall)) {
          return new ArrayList<>();
        }
        RexCall conjCall = (RexCall) conjunction;
        RexNode ref = null;
        if(COMPARISON_UDFS.contains(conjCall.getOperator().getName())) {
          if (conjCall.operands.get(0) instanceof RexInputRef &&
                  conjCall.operands.get(1) instanceof RexLiteral) {
            ref = conjCall.operands.get(0);
          } else if (conjCall.operands.get(1) instanceof RexInputRef &&
                  conjCall.operands.get(0) instanceof RexLiteral) {
            ref = conjCall.operands.get(1);
          } else {
            // We do not know what it is, we bail out for safety
            return new ArrayList<>();
          }
        } else if(conjCall.getOperator().getName().equals(IN_UDF)) {
          ref = conjCall.operands.get(0);
        } else if(conjCall.getOperator().getName().equals(BETWEEN_UDF)) {
          ref = conjCall.operands.get(1);
        } else {
          // We do not know what it is, we bail out for safety
          return new ArrayList<>();
        }

        String stringRef = ref.toString();
        reductionCondition.put(stringRef, conjCall);
        refsInCurrentOperand.add(stringRef);
      }

      // Updates the references that are present in every operand up till now
      if (i == 0) {
        refsInAllOperands = refsInCurrentOperand;
      } else {
        refsInAllOperands = Sets.intersection(refsInAllOperands, refsInCurrentOperand);
      }
      // If we did not add any factor or there are no common factors, we can bail out
      if (refsInAllOperands.isEmpty()) {
        return new ArrayList<>();
      }
    }

    // 2. We gather the common factors and return them
    List<RexNode> commonOperands = new ArrayList<>();
    for (String ref : refsInAllOperands) {
      commonOperands.add(RexUtil.composeDisjunction(rexBuilder, reductionCondition.get(ref), false));
    }
    return commonOperands;
  }

}
