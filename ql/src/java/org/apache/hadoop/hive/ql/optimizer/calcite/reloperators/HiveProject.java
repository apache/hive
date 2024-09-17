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

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.CorrelationInfoVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.HiveCorrelationInfo;

public class HiveProject extends Project implements HiveRelNode {

  // Information about correlations within a subquery.
  private final CorrelationInfoSupplier correlationInfos;

  private boolean isSysnthetic;

  /**
   * Creates a HiveProject.
   * @param cluster
   *          Cluster this relational expression belongs to
   * @param child
   *          input relational expression
   * @param exps
   *          List of expressions for the input columns
   * @param rowType
   *          output row type
   */
  public HiveProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, List<? extends RexNode> exps,
      RelDataType rowType) {
    super(cluster, traitSet, child, exps, rowType);
    this.correlationInfos = new CorrelationInfoSupplier(getProjects());
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
  }

  /**
   * Creates a HiveProject with no sort keys.
   *
   * @param child
   *          input relational expression
   * @param exps
   *          set of expressions for the input columns
   * @param fieldNames
   *          aliases of the expressions
   */
  public static HiveProject create(RelNode child, List<? extends RexNode> exps,
      List<String> fieldNames) throws CalciteSemanticException{
    RelOptCluster cluster = child.getCluster();

    // 1 Ensure columnNames are unique - CALCITE-411
    if (fieldNames != null && !Util.isDistinct(fieldNames)) {
      String msg = "Select list contains multiple expressions with the same name." + fieldNames;
      throw new CalciteSemanticException(msg, UnsupportedFeature.Same_name_in_multiple_expressions);
    }
    RelDataType rowType = RexUtil.createStructType(
        cluster.getTypeFactory(), exps, fieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
    return create(cluster, child, exps, rowType, Collections.<RelCollation> emptyList());
  }

  /**
   * Creates a HiveProject.
   */
  public static HiveProject create(RelOptCluster cluster, RelNode child, List<? extends RexNode> exps,
      RelDataType rowType, final List<RelCollation> collationList) {
    RelTraitSet traitSet = TraitsUtil.getDefaultTraitSet(cluster);
    return new HiveProject(cluster, traitSet, child, exps, rowType);
  }

  /**
   * Creates a HiveProject.
   */
  public static HiveProject create(RelOptCluster cluster, RelNode child, List<? extends RexNode> exps,
      RelDataType rowType, RelTraitSet traitSet, final List<RelCollation> collationList) {
    return new HiveProject(cluster, traitSet, child, exps, rowType);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    HiveProject hp = new HiveProject(getCluster(), traitSet, input, exps, rowType);
    if (this.isSynthetic()) {
      hp.setSynthetic();
    }

    return hp;
  }

  // TODO: this should come through RelBuilder to the constructor as opposed to
  // set method. This requires calcite change
  public void setSynthetic() {
    this.isSysnthetic = true;
  }

  public boolean isSynthetic() {
    return isSysnthetic;
  }

  //required for HiveRelDecorrelator
  @Override public RelNode accept(RelShuttle shuttle) {
    if(shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("synthetic", this.isSysnthetic, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
  }

  public List<HiveCorrelationInfo> getCorrelationInfos() {
    return correlationInfos.get();
  }

  /**
   * CorrelationInfoSupplier allows for a lazy fetch so that the HiveCorrelationInfo
   * only gets retrieved on demand.
   */
  private static class CorrelationInfoSupplier {
    public final List<RexNode> projects;
    private List<HiveCorrelationInfo> correlationInfos;

    public CorrelationInfoSupplier(List<RexNode> projects) {
      this.projects = projects;
    }

    public List<HiveCorrelationInfo> get() {
      if (correlationInfos == null) {
        correlationInfos = new ArrayList<>();
        for (RexNode r : projects) {
          correlationInfos.addAll(CorrelationInfoVisitor.getCorrelationInfos(r));
        }
      }
      return correlationInfos;
    }
  }
}
