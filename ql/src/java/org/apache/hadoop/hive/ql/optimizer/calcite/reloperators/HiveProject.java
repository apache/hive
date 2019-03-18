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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;

import com.google.common.collect.ImmutableList;

public class HiveProject extends Project implements HiveRelNode {

  private final List<Integer>        virtualCols;
  private boolean isSysnthetic;

  /**
   * Creates a HiveProject.
   *
   * @param cluster
   *          Cluster this relational expression belongs to
   * @param child
   *          input relational expression
   * @param exps
   *          List of expressions for the input columns
   * @param rowType
   *          output row type
   * @param flags
   *          values as in {@link Project.Flags}
   */
  public HiveProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      List<? extends RexNode> exps, RelDataType rowType, int flags) {
    super(cluster, traitSet, child, exps, rowType, flags);
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    virtualCols = ImmutableList.copyOf(HiveCalciteUtil.getVirtualCols(exps));
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
    return new HiveProject(cluster, traitSet, child, exps, rowType, Flags.BOXED);
  }

  /**
   * Creates a HiveProject.
   */
  public static HiveProject create(RelOptCluster cluster, RelNode child, List<? extends RexNode> exps,
      RelDataType rowType, RelTraitSet traitSet, final List<RelCollation> collationList) {
    return new HiveProject(cluster, traitSet, child, exps, rowType, Flags.BOXED);
  }

  /**
   * Creates a relational expression which projects the output fields of a
   * relational expression according to a partial mapping.
   *
   * <p>
   * A partial mapping is weaker than a permutation: every target has one
   * source, but a source may have 0, 1 or more than one targets. Usually the
   * result will have fewer fields than the source, unless some source fields
   * are projected multiple times.
   *
   * <p>
   * This method could optimize the result as {@link #permute} does, but does
   * not at present.
   *
   * @param rel
   *          Relational expression
   * @param mapping
   *          Mapping from source fields to target fields. The mapping type must
   *          obey the constraints {@link MappingType#isMandatorySource()} and
   *          {@link MappingType#isSingleSource()}, as does
   *          {@link MappingType#INVERSE_FUNCTION}.
   * @param fieldNames
   *          Field names; if null, or if a particular entry is null, the name
   *          of the permuted field is used
   * @return relational expression which projects a subset of the input fields
   * @throws CalciteSemanticException
   */
  public static RelNode projectMapping(RelNode rel, Mapping mapping, List<String> fieldNames) throws CalciteSemanticException {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();

    if (mapping.isIdentity()) {
      return rel;
    }

    final List<String> outputNameList = new ArrayList<String>();
    final List<RexNode> outputProjList = new ArrayList<RexNode>();
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    for (int i = 0; i < mapping.getTargetCount(); i++) {
      int source = mapping.getSource(i);
      final RelDataTypeField sourceField = fields.get(source);
      outputNameList
          .add(((fieldNames == null) || (fieldNames.size() <= i) || (fieldNames.get(i) == null)) ? sourceField
              .getName() : fieldNames.get(i));
      outputProjList.add(rexBuilder.makeInputRef(rel, source));
    }

    return create(rel, outputProjList, outputNameList);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    HiveProject hp = new HiveProject(getCluster(), traitSet, input, exps, rowType, getFlags());
    if (this.isSynthetic()) {
      hp.setSynthetic();
    }

    return hp;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  public List<Integer> getVirtualCols() {
    return virtualCols;
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

}
