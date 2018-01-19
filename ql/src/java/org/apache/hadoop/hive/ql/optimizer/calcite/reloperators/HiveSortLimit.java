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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;

import com.google.common.collect.ImmutableMap;

public class HiveSortLimit extends Sort implements HiveRelNode {

  // NOTE: this is to work around Hive Calcite Limitations w.r.t OB.
  // 1. Calcite can not accept expressions in OB; instead it needs to be expressed
  // as VC in input Select.
  // 2. Hive can not preserve ordering through select boundaries.
  // 3. This map is used for outermost OB to migrate the VC corresponding OB
  // expressions from input select.
  // 4. This is used by ASTConverter after we are done with Calcite Planning
  private ImmutableMap<Integer, RexNode> mapOfInputRefToRexCall;

  private boolean ruleCreated;

  public HiveSortLimit(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, TraitsUtil.getSortTraitSet(cluster, traitSet, collation), child, collation,
        offset, fetch);
  }

  /**
   * Creates a HiveSortLimit.
   *
   * @param input     Input relational expression
   * @param collation array of sort specifications
   * @param offset    Expression for number of rows to discard before returning
   *                  first row
   * @param fetch     Expression for number of rows to fetch
   */
  public static HiveSortLimit create(RelNode input, RelCollation collation,
      RexNode offset, RexNode fetch) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet =
        TraitsUtil.getSortTraitSet(cluster, input.getTraitSet(), collation);
    return new HiveSortLimit(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override
  public HiveSortLimit copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
      RexNode offset, RexNode fetch) {
    // TODO: can we blindly copy sort trait? What if inputs changed and we
    // are now sorting by different cols
    RelCollation canonizedCollation = traitSet.canonize(newCollation);
    HiveSortLimit sortLimit =
            new HiveSortLimit(getCluster(), traitSet, newInput, canonizedCollation, offset, fetch);
    sortLimit.setRuleCreated(ruleCreated);
    return sortLimit;
  }

  public RexNode getFetchExpr() {
    return fetch;
  }

  public RexNode getOffsetExpr() {
    return offset;
  }

  public void setInputRefToCallMap(ImmutableMap<Integer, RexNode> refToCall) {
    this.mapOfInputRefToRexCall = refToCall;
  }

  public Map<Integer, RexNode> getInputRefToCallMap() {
    return this.mapOfInputRefToRexCall;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  public boolean isRuleCreated() {
    return ruleCreated;
  }

  public void setRuleCreated(boolean ruleCreated) {
    this.ruleCreated = ruleCreated;
  }

  //required for HiveRelDecorrelator
  public RelNode accept(RelShuttle shuttle) {
    if (shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }

}
