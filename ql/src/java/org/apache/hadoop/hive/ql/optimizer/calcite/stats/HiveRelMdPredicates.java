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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;


//TODO: Move this to calcite
public class HiveRelMdPredicates extends RelMdPredicates {

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
                                                     BuiltInMethod.PREDICATES.method,
                                                     new HiveRelMdPredicates());

  private static final List<RexNode> EMPTY_LIST = ImmutableList.of();

  /**
   * Infers predicates for a project.
   *
   * <ol>
   * <li>create a mapping from input to projection. Map only positions that
   * directly reference an input column.
   * <li>Expressions that only contain above columns are retained in the
   * Project's pullExpressions list.
   * <li>For e.g. expression 'a + e = 9' below will not be pulled up because 'e'
   * is not in the projection list.
   *
   * <pre>
   * childPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
   * projectionExprs:       {a, b, c, e / 2}
   * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
   * </pre>
   *
   * </ol>
   */
  @Override
  public RelOptPredicateList getPredicates(Project project, RelMetadataQuery mq) {
    RelNode child = project.getInput();
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    RelOptPredicateList childInfo = mq.getPulledUpPredicates(child);

    List<RexNode> projectPullUpPredicates = new ArrayList<RexNode>();
    HashMultimap<Integer, Integer> inpIndxToOutIndxMap = HashMultimap.create();

    ImmutableBitSet.Builder columnsMappedBuilder = ImmutableBitSet.builder();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION, child.getRowType().getFieldCount(),
        project.getRowType().getFieldCount());

    for (Ord<RexNode> o : Ord.zip(project.getProjects())) {
      if (o.e instanceof RexInputRef) {
        int sIdx = ((RexInputRef) o.e).getIndex();
        m.set(sIdx, o.i);
        inpIndxToOutIndxMap.put(sIdx, o.i);
        columnsMappedBuilder.set(sIdx);
      }
    }

    // Go over childPullUpPredicates. If a predicate only contains columns in
    // 'columnsMapped' construct a new predicate based on mapping.
    final ImmutableBitSet columnsMapped = columnsMappedBuilder.build();
    for (RexNode r : childInfo.pulledUpPredicates) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
      if (columnsMapped.contains(rCols)) {
        r = r.accept(new RexPermuteInputsShuttle(m, child));
        projectPullUpPredicates.add(r);
      }
    }

    // Project can also generate constants. We need to include them.
    for (Ord<RexNode> expr : Ord.zip(project.getProjects())) {
      if (RexLiteral.isNullLiteral(expr.e)) {
        projectPullUpPredicates.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(project, expr.i)));
      } else if (expr.e instanceof RexLiteral) {
        final RexLiteral literal = (RexLiteral) expr.e;
        projectPullUpPredicates.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(project, expr.i), literal));
      } else if (expr.e instanceof RexCall && HiveCalciteUtil.isDeterministicFuncOnLiterals(expr.e)) {
      //TODO: Move this to calcite
        projectPullUpPredicates.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(project, expr.i), expr.e));
      }
    }
    return RelOptPredicateList.of(projectPullUpPredicates);
  }

  /**
   * Infers predicates for a Union.
   */
  @Override
  public RelOptPredicateList getPredicates(Union union, RelMetadataQuery mq) {
    RexBuilder rB = union.getCluster().getRexBuilder();

    Map<String, RexNode> finalPreds = new LinkedHashMap<>();
    Map<String, RexNode> finalResidualPreds = new LinkedHashMap<>();
    for (int i = 0; i < union.getInputs().size(); i++) {
      RelNode input = union.getInputs().get(i);
      RelOptPredicateList info = mq.getPulledUpPredicates(input);
      if (info.pulledUpPredicates.isEmpty()) {
        return RelOptPredicateList.EMPTY;
      }
      Map<String, RexNode> preds = new LinkedHashMap<>();
      for (RexNode pred : info.pulledUpPredicates) {
        final String predString = pred.toString();
        if (i == 0) {
          preds.put(predString, pred);
          continue;
        }
        if (finalPreds.containsKey(predString)) {
          preds.put(predString, pred);
        } else {
          finalResidualPreds.put(predString, pred);
        }
      }
      // Add those that are not part of the final set to residual
      for (Entry<String, RexNode> e : finalPreds.entrySet()) {
        if (!preds.containsKey(e.getKey())) {
          finalResidualPreds.put(e.getKey(), e.getValue());
        }
      }
      finalPreds = preds;
    }

    List<RexNode> preds = new ArrayList<>(finalPreds.values());
    RexNode disjPred = RexUtil.composeDisjunction(rB, finalResidualPreds.values(), true);
    if (disjPred != null) {
      preds.add(disjPred);
    }
    return RelOptPredicateList.of(preds);
  }

}
