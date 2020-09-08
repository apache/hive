/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexCall;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.hadoop.hive.ql.plan.impala.rex.ReferrableNode;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;

import java.util.List;
import java.util.Set;

/**
 * Class holding Impala and RexNode conjuncts organized by whether the
 * conjunct can be resolved by partition pruning.
 */
public class ImpalaConjuncts {

  private final List<Expr> impalaPartitionConjuncts;
  private final List<Expr> impalaNonPartitionConjuncts;
  private final List<Expr> normalizedPartitionConjuncts;
  private final List<RexNode> partitionConjuncts;
  private final List<RexNode> nonPartitionConjuncts;

  private ImpalaConjuncts() {
    this.impalaPartitionConjuncts = ImmutableList.of();
    this.impalaNonPartitionConjuncts = ImmutableList.of();
    this.normalizedPartitionConjuncts = ImmutableList.of();
    this.partitionConjuncts = ImmutableList.of();
    this.nonPartitionConjuncts = ImmutableList.of();
  }

  private ImpalaConjuncts(List<RexNode> andOperands, List<RexNode> existingPartitionConjuncts,
      Analyzer analyzer, ReferrableNode relNode, RexBuilder rexBuilder,
      Set<Integer> partitionColsIndexes) throws HiveException {
    Preconditions.checkNotNull(andOperands);
    Preconditions.checkNotNull(existingPartitionConjuncts);
    List<Expr> tmpImpalaPartitionConjuncts = Lists.newArrayList();
    List<Expr> tmpImpalaNonPartitionConjuncts = Lists.newArrayList();
    List<Expr> tmpNormalizedPartitionConjuncts = Lists.newArrayList();
    List<RexNode> tmpPartitionConjuncts = Lists.newArrayList();
    List<RexNode> tmpNonPartitionConjuncts = Lists.newArrayList();

    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(relNode), partitionColsIndexes, rexBuilder);

    // add existing partitions
    for (RexNode existingPartition : existingPartitionConjuncts) {
      visitor.resetPartitionState();
      Expr impalaConjunct = existingPartition.accept(visitor);
      tmpImpalaPartitionConjuncts.add(impalaConjunct);
      tmpPartitionConjuncts.add(existingPartition);
    }

    for (RexNode andOperand : andOperands) {
      // reset the visitor's partition state because we want each conjunct's
      // eligibility as a partitioning expr to be evaluated independently
      visitor.resetPartitionState();
      Expr impalaConjunct = andOperand.accept(visitor);
      // check if this conjunct only has partition column references.
      // e.g  'part_col = 5 OR non_part_col = 10' will return false, so don't add
      // it to the list of partition conjuncts
      if (partitionColsIndexes != null && visitor.hasPartitionColsOnly()) {
        tmpImpalaPartitionConjuncts.add(impalaConjunct);
        tmpPartitionConjuncts.add(andOperand);
        // normalize the operand. If the expression contains a binary expression,
        // the SlotRef portion needs to be the first parameter
        // (e.g. 'where 1 <= my_col' --> 'where my_col >= 1')
        RexNode normalizedAndOperand = normalizeOperand(rexBuilder, andOperand);
        Expr normalizedImpalaConjunct = normalizedAndOperand.accept(visitor);
        tmpNormalizedPartitionConjuncts.add(normalizedImpalaConjunct);
      } else {
        tmpImpalaNonPartitionConjuncts.add(impalaConjunct);
        tmpNonPartitionConjuncts.add(andOperand);
      }
    }

    this.impalaPartitionConjuncts = ImmutableList.copyOf(tmpImpalaPartitionConjuncts);
    this.impalaNonPartitionConjuncts = ImmutableList.copyOf(tmpImpalaNonPartitionConjuncts);
    this.normalizedPartitionConjuncts = ImmutableList.copyOf(tmpNormalizedPartitionConjuncts);
    this.partitionConjuncts = ImmutableList.copyOf(tmpPartitionConjuncts);
    this.nonPartitionConjuncts = ImmutableList.copyOf(tmpNonPartitionConjuncts);
  }

  public List<Expr> getImpalaPartitionConjuncts() {
    return impalaPartitionConjuncts;
  }

  public List<Expr> getImpalaNonPartitionConjuncts() {
    return impalaNonPartitionConjuncts;
  }

  public List<Expr> getNormalizedPartitionConjuncts() {
    return normalizedPartitionConjuncts;
  }

  public List<RexNode> getPartitionConjuncts() {
    return partitionConjuncts;
  }

  public List<RexNode> getNonPartitionConjuncts() {
    return nonPartitionConjuncts;
  }

  /**
   * Normalize the operand. If the expression contains a binary expression,
   * the SlotRef portion needs to be the first parameter
   * (e.g. 'where 1 <= my_col' --> 'where my_col >= 1')
   */
  private static RexNode normalizeOperand(RexBuilder rexBuilder, RexNode node) {
    RexVisitor<RexNode> visitor = new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        if (ImpalaRexCall.isBinaryComparison(call.getKind()) &&
            !RexUtil.containsInputRef(call.getOperands().get(0)) &&
            RexUtil.containsInputRef(call.getOperands().get(1))) {
          RexCall invertedCall = (RexCall) RexUtil.invert(rexBuilder, call);
          call = (invertedCall != null) ? invertedCall : call;
        }
        return super.visitCall(call);
      }
    };

    return node.accept(visitor);
  }

  private static void validatePartitionConjuncts(RexBuilder rexBuilder, List<RexNode> allConjuncts,
      List<RexNode> existingPartitionConjuncts) throws HiveException {
    if (existingPartitionConjuncts.isEmpty()) {
      return;
    }
    Set<RexNode> allConjunctsSet = Sets.newHashSet(allConjuncts);
    for (RexNode conjunct : existingPartitionConjuncts) {
      if (!allConjunctsSet.contains(conjunct)) {
        throw new HiveException("Error: The following pruned partition conjunct"
          + " did not exist in the final filter condition: " + conjunct);
      }
    }
  }

  private static List<RexNode> removePartitionConjuncts(List<RexNode> allConjuncts,
      List<RexNode> existingPartitionConjuncts) {
    if (existingPartitionConjuncts.isEmpty()) {
      return allConjuncts;
    }
    Set<RexNode> nodesToRemove = Sets.newHashSet(existingPartitionConjuncts);
    List<RexNode> result = Lists.newArrayList();
    for (RexNode conjunct : allConjuncts) {
      if (!nodesToRemove.contains(conjunct)) {
        result.add(conjunct);
      }
    }
    return result;
  }

  private static List<RexNode> getConjuncts(RexNode conjuncts) {
    if (conjuncts == null) {
      return ImmutableList.of();
    }

    if (!(conjuncts instanceof RexCall)) {
      return ImmutableList.of(conjuncts);
    }
    RexCall rexCallConjuncts = (RexCall) conjuncts;
    if (rexCallConjuncts.getKind() != SqlKind.AND) {
      return ImmutableList.of(conjuncts);
    }
    return rexCallConjuncts.getOperands();
  }

  public static String toString(List<Expr> conjuncts) {
    String exprString = "";
    for (Expr e : conjuncts) {
      exprString += e.toSql() + ";";
    }
    return exprString;
  }

  public static ImpalaConjuncts create(HiveFilter filter, Analyzer analyzer,
      ReferrableNode relNode) throws HiveException {
    if (filter == null) {
      return create();
    }
    return create(filter.getCondition(), Lists.newArrayList(), analyzer, relNode,
        filter.getCluster().getRexBuilder(), null);
  }

  public static ImpalaConjuncts create(RexNode filterCondition, Analyzer analyzer,
      ReferrableNode relNode, RexBuilder rexBuilder, Set<Integer> partitionColsIndexes)
      throws HiveException {
    return create(filterCondition, Lists.newArrayList(), analyzer, relNode, rexBuilder,
        partitionColsIndexes);
  }

  /**
   * This flavor of create is called after partition pruning is done and we already have
   * existing partition conjuncts. There will be validation done to make sure that all
   * existing partition conjuncts exist within the filter condition. Also, because there
   * is no partitionColsIndexes in this flavor, that will ensure no new partition conjuncts
   * will be added.
   */
  public static ImpalaConjuncts create(HiveFilter filter,
      List<RexNode> existingPartitionConjuncts, Analyzer analyzer, ReferrableNode relNode,
      RexBuilder rexBuilder) throws HiveException {
    if (filter == null) {
      Preconditions.checkNotNull(existingPartitionConjuncts);
      Preconditions.checkState(existingPartitionConjuncts.isEmpty());
      return create();
    }
    return create(filter.getCondition(), existingPartitionConjuncts, analyzer, relNode, rexBuilder,
        null);
  }

  public static ImpalaConjuncts create(RexNode filterCondition,
      List<RexNode> existingPartitionConjuncts, Analyzer analyzer, ReferrableNode relNode,
      RexBuilder rexBuilder, Set<Integer> partitionColsIndexes) throws HiveException {
    List<RexNode> andOperands =
        (filterCondition == null) ? Lists.newArrayList() : getConjuncts(filterCondition);
    validatePartitionConjuncts(rexBuilder, andOperands, existingPartitionConjuncts);
    andOperands = removePartitionConjuncts(andOperands, existingPartitionConjuncts);
    return new ImpalaConjuncts(andOperands, existingPartitionConjuncts, analyzer, relNode,
        rexBuilder, partitionColsIndexes);
  }

  /**
   * Default for when structure is needed when there are no conjuncts (e.g. "select (1)")
   */
  public static ImpalaConjuncts create() throws HiveException {
    return new ImpalaConjuncts();
  }
}
