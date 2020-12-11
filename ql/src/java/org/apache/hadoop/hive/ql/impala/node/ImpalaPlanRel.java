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
package org.apache.hadoop.hive.ql.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.impala.plan.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.impala.rex.ReferrableNode;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;

import java.util.List;
import java.util.Map;

/**
 * Abstract base class for Impala relational expressions with a variable number
 * of inputs (0 or more)
 */
public abstract class ImpalaPlanRel extends AbstractRelNode implements ReferrableNode {

  protected ImmutableList<RelNode> inputs;
  protected ImpalaNodeInfo nodeInfo; // initialized in concrete derived classes
  protected ImmutableMap<Integer, Expr> outputExprs;
  protected Pair<Integer, Integer> maxIndexInfo;

  /**
   * Creates a <code>ImpalaPlanRel</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits  traits
   * @param inputs  Input relational expressions, if any
   */
  protected ImpalaPlanRel(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, RelDataType rowType) {
    super(cluster, traits);
    this.rowType = rowType;
    this.inputs = ImmutableList.copyOf(inputs);
  }

  @Override
  public RelNode getInput(int i) {
    return inputs.get(i);
  }

  public ImpalaPlanRel getImpalaRelInput(int i) {
    return (ImpalaPlanRel) getInput(i);
  }

  @Override
  public List<RelNode> getInputs() {
    return inputs;
  }

  @Override
  public void childrenAccept(RelVisitor visitor) {
    int i = 0;
    for (RelNode input : inputs) {
      visitor.visit(inputs.get(i), i, this);
      i++;
    }
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode rel) {
    Preconditions.checkState(ordinalInParent < inputs.size());
    List<RelNode> newList = Lists.newArrayList();
    for (int i = 0; i < inputs.size(); ++i) {
      RelNode relNodeToAdd = (i == ordinalInParent) ? rel : inputs.get(i);
      newList.add(relNodeToAdd);
    }
    inputs = ImmutableList.copyOf(newList);
  }

  public RelWriter explainTerms(RelWriter pw) {
    RelWriter ret = super.explainTerms(pw);
    for (RelNode input : inputs) {
      ret.input("input", input);
    }
    return ret;
  }

  public List<Expr> getOutputExprs() {
    Preconditions.checkNotNull(this.outputExprs);
    return this.outputExprs.values().asList();
  }

  public ImmutableMap<Integer, Expr> getOutputExprsMap() {
    Preconditions.checkNotNull(this.outputExprs);
    return this.outputExprs;
  }

  @Override
  public Expr getExpr(int index) {
    Preconditions.checkState(this.outputExprs.containsKey(index));
    return this.outputExprs.get(index);
  }

  @Override
  public int numOutputExprs() {
    Preconditions.checkNotNull(this.outputExprs);
    return this.outputExprs.values().size();
  }

  @Override
  public Pair<Integer, Integer> getMaxIndexInfo() {
    if (maxIndexInfo != null) {
      return maxIndexInfo;
    }

    Preconditions.checkNotNull(this.outputExprs);
    int maxIndex = Integer.MIN_VALUE;
    for (Map.Entry<Integer, Expr> e : outputExprs.entrySet()) {
      maxIndex = Math.max(maxIndex, e.getKey());
    }
    maxIndexInfo = Pair.of(maxIndex, outputExprs.size());
    return maxIndexInfo;
  }

  public PlanNode getRootPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    PlanNode rootPlanNode = getPlanNode(ctx);
    ctx.setResultExprs(getOutputExprs());
    return rootPlanNode;
  }

  /**
   * Get the Impala PlanNode corresponding to this RelNode
   */
  public abstract PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException;

  /**
   * Create the output SlotRefs using the supplied slot descriptors.
   * This will be invoked by most plan rels but not all. For instance,
   * the ImpalaProjectRel may have expressions other than SlotRefs.
   * Also, the ImpalaHdfsScanRel does not map the index number directly
   * to the slot descriptor array position.
   */
  public static ImmutableMap<Integer, Expr> createOutputExprs(List<SlotDescriptor> slotDescs) {
    Map<Integer, Expr> exprs = Maps.newLinkedHashMap();
    int index = 0;
    for (SlotDescriptor slotDesc : slotDescs) {
      slotDesc.setIsMaterialized(true);
      exprs.put(index++, new SlotRef(slotDesc));
    }
    return ImmutableMap.copyOf(exprs);
  }

  @Override
  public abstract RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq);

  @Override
  public abstract double estimateRowCount(RelMetadataQuery mq);

}
