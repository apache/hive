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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.Calcite2302;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionMapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala specific rule to add a cast when needed for Impala functions.
 * For example, Impala supports sum for BIGINT, but not INT.  If we see an INT param,
 * this rule will add the CAST INT AS BIGINT function so that it matches the Impala
 * signature.
 */
public class ImpalaAggCastRule extends RelOptRule {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaAggCastRule.class);

  public static ImpalaAggCastRule INSTANCE = new ImpalaAggCastRule();
  /**
   * When this rule is matched, a new Project will be created.
   * This class is a small structure containing each project and the position
   * within the project list.  It is used to check for duplicates and to do
   * the final creation.
   * It implements Comparable so that an array of these structures can be sorted
   * by position number when writing out the new Project Node.
   */
  private static class NewPositionAndNode implements Comparable {
    public Integer newPosition;
    public RexNode node;

    public NewPositionAndNode(int newPosition, RexNode node) {
      this.newPosition = newPosition;
      this.node = node;
    }

    @Override
    public int compareTo(Object other) {
      return this.newPosition.compareTo(((NewPositionAndNode)other).newPosition);
    }
  }

  public ImpalaAggCastRule() {
    super(operand(HiveAggregate.class, any()), HiveRelFactories.HIVE_BUILDER, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveAggregate aggregate = call.rel(0);
    RelNode aggregateInput = aggregate.getInputs().get(0);
    RelBuilder relBuilder = call.builder();

    // Structure to hold the unique map of all the projections that will be needed.
    // Used to check and prevent duplicates.  For instance, if we use sum(c2), avg(c2),
    // and these map to sum(BIGINT) and avg(BIGINT), then we only need one projection
    // column to handle both these aggs.
    Map<RexInputRef, NewPositionAndNode> newProjects = Maps.newHashMap();

    // Creates the "newProjects" map containing all new projections needed and returns
    // the list of new positions needed for each existing aggcall (the list of lists)
    // This returns null if no casting is needed and thus no new projections are needed.
    List<List<Integer>> newArgLists = getNewArgLists(aggregate, aggregateInput, newProjects);

    if (newArgLists == null) {
      return;
    }

    // We also need to map the groupsets to the newprojections.  This gets us the mapping
    // of the old position number to the new position number.  This will also populate any
    // potential new projects that are needed.
    Mapping groupSetMapping = createGroupSetMapping(aggregate, aggregateInput, newProjects);

    // apply the mapping to create the new groupsets
    ImmutableBitSet newGroupSet = Mappings.apply(groupSetMapping, aggregate.getGroupSet());
    List<ImmutableBitSet> newGroupSets = Mappings.apply2(groupSetMapping, aggregate.getGroupSets());

    // create new projection node.
    RelNode newProjectNode = createNewProject(relBuilder, aggregate, aggregateInput, newProjects);

    // need to generate the new list of aggcalls with the newly generated arglist structure.
    List<AggregateCall> newAggregateCalls = createNewAggregateCalls(aggregate, newArgLists);

    RelNode newAggNode = aggregate.copy(aggregate.getTraitSet(), newProjectNode,
        aggregate.indicator, newGroupSet, newGroupSets, newAggregateCalls);

    call.transformTo(newAggNode);
  }

  /**
   * Walk through all agg calls and their operands and return the inputref position
   * for each operand.  If no operands need to be cast, this method returns null.
   * The "positionMap" should be passed in as empty and is mutated by this method.
   */
  private List<List<Integer>> getNewArgLists(Aggregate aggregate,
      RelNode aggregateInput, Map<RexInputRef, NewPositionAndNode> newProjects) {
    assert newProjects.isEmpty();
    List<List<Integer>> argLists = Lists.newArrayList();
    // track if anything changed.  If nothing changed, there is no need to add a projection
    // transformation.
    boolean isChanged = false;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      // need to get the operand types into the "SqlTypeName" structure for comparison purposes.
      List<RelDataType> currentOperandTypes = getRelDataTypeOperands(aggCall, aggregateInput);
      // retrieve mapped operand types.  If no mapping is needed (i.e. the signature of the
      // operands in the function match the existing operands without casting, then the
      // getMappedOperandTypes method returns null.  If no matching function signature is found,
      // a RuntimeExcepiton is thrown.
      List<RelDataType> mappedOperandTypes = getMappedOperandTypes(aggCall, aggregateInput, currentOperandTypes);
      if (mappedOperandTypes != null) {
        isChanged = true;
        // add new projections to set.  This will prevent duplicates in projects.
      } else {
        // we still need to create the position node list even if there are no changes.
        // Just use the unchanged one.  We do this because even though this iteration of aggCall
        // may not need mapping, a different iteration might need mapping, and we need the projections
        // from all iterations.
        mappedOperandTypes = currentOperandTypes;
      }

      // after this call, newProjects will be mutated.and contain all known projections as of this
      // iteration.
      addAggInputsToNewProjects(aggCall, aggregateInput, mappedOperandTypes, newProjects);

      // generate the argList for the current iteration.
      argLists.add(getNewArgList(aggCall, aggregateInput, mappedOperandTypes, newProjects));
    }

    if (!isChanged) {
      return null;
    }
    return argLists;
  }

  /**
   * Translation function to get the RelDataType operands from the relnode rowtype.
   */
  private List<RelDataType> getRelDataTypeOperands(AggregateCall aggCall, RelNode inputNode) {
    List<RelDataType> result = Lists.newArrayList();
    List<RelDataTypeField> fields = inputNode.getRowType().getFieldList();
    for (Integer i : aggCall.getArgList()) {
      result.add(fields.get(i).getType());
    }
    return result;
  }

  /**
   * Helper function to get the corresponding SqlTypeName array from the RelDataType array
   */
  private List<SqlTypeName> getSqlTypeNameOperands(List<RelDataType> relDataTypes) {
    List<SqlTypeName> result = Lists.newArrayList();
    for (RelDataType relDataType : relDataTypes) {
      result.add(relDataType.getSqlTypeName());
    }
    return result;
  }

  /**
   * returns a list of the casted operand type needed in order to use the function in the aggCall.
   * If all operand types do not need casting, this method returns null.
   */
  private List<RelDataType> getMappedOperandTypes(AggregateCall aggCall, RelNode inputNode,
      List<RelDataType> currentOperandTypes) {
    String opName = aggCall.getAggregation().getName();
    SqlTypeName currentSqlRetType = aggCall.getType().getSqlTypeName();

    List<SqlTypeName> currentSqlOperandTypes = getSqlTypeNameOperands(currentOperandTypes);

    // create the signature as/is to see if it needs casting.
    ImpalaFunctionMapper ifs =
        new ImpalaFunctionMapper(opName, currentSqlOperandTypes, currentSqlRetType);

    // The main call to check if this AggCall maps to a known Impala function signature.
    Pair<SqlTypeName, List<SqlTypeName>> pair =
        ifs.mapOperands(AggFunctionDetails.AGG_BUILTINS_INSTANCE);

    SqlTypeName mappedSqlRetType = pair.left;
    List<SqlTypeName> mappedSqlOperandTypes = pair.right;

    // Only the SqlTypeNames have to match.  Decimal values don't need to be recast, and the
    // type will be determined by Calcite.
    if (!mappedSqlRetType.equals(currentSqlRetType)) {
      throw new RuntimeException("Casting of aggregate return types is not supported for Impala");
    }

    // if the mapped oeprands are the same as the current ones, no cast is needed.
    if (mappedSqlOperandTypes.equals(currentSqlOperandTypes)) {
      return null;
    }

    return getCastedRelDataTypes(inputNode.getCluster().getTypeFactory(),
        mappedSqlOperandTypes, currentOperandTypes);
  }

  /**
   * Walk through all the SqlTypeNames to be casted, and return the corresponding
   * RelDataType.
   */
  private List<RelDataType> getCastedRelDataTypes(RelDataTypeFactory dtFactory,
      List<SqlTypeName> postCastSqlTypeNames, List<RelDataType> preCastRelDataTypes) {
    List<RelDataType> result = Lists.newArrayList();
    // walk through all operands and get the RelData
    for (int i = 0; i < preCastRelDataTypes.size(); ++i) {
      // In the case where we are casting to Decimal, we need to provide a precision
      // and scale.  The Calcite method provides the DecimalRelDataType with the
      // appropriate precision and scale with the provided datatype that will be casted.
      if (postCastSqlTypeNames.get(i) == SqlTypeName.DECIMAL) {
        //TODO: CDPD-8258, remove once we upgrade to Calcite 1.21
        result.add(Calcite2302.decimalOf(dtFactory, preCastRelDataTypes.get(i)));
      } else {
        result.add(dtFactory.createSqlType(postCastSqlTypeNames.get(i)));
      }
    }
    return result;
  }

  /**
    * A translation function that takes all our new projects and creates the relNode.
    * The values of the map contain all the projection RexNodes. We need to sort the
    * values by the "newPosition" number.
    */
  private RelNode createNewProject(RelBuilder relBuilder, Aggregate aggregate,
      RelNode aggregateInput, Map<RexInputRef, NewPositionAndNode> newProjects) {
    List<NewPositionAndNode> newPositions = Lists.newArrayList(newProjects.values());
    Collections.sort(newPositions);
    List<RexNode> newRexNodes = Lists.newArrayList();
    for (NewPositionAndNode n : newPositions) {
      newRexNodes.add(n.node);
    }

    return relBuilder.push(aggregateInput).project(newRexNodes).build();
  }

  /**
   * Create the transformed AggList by using the list of new argLists.
   */
  private List<AggregateCall> createNewAggregateCalls(Aggregate aggregate, List<List<Integer>> newArgList) {
    Preconditions.checkState(newArgList.size() == aggregate.getAggCallList().size());
    List<AggregateCall> result = Lists.newArrayList();
    for (int i = 0; i < newArgList.size(); ++i) {
      AggregateCall aggCall = aggregate.getAggCallList().get(i);
      result.add(aggCall.copy(newArgList.get(i)));
    }
    return result;
  }

  /**
   * Add any newly needed Project RexNodes into the "existingProjects" structure.
   * The number of elements in "argList" for this call should contain the same number
   * as the number of elements in the mappedOperandTypes. We need to check if the input
   * ref from the arglist paired with the mapped operand type matches something we've
   * already seen.
   */
  private void addAggInputsToNewProjects(AggregateCall aggCall, RelNode aggregateInput,
      List<RelDataType> mappedOperandTypes, Map<RexInputRef, NewPositionAndNode> existingProjects) {
    assert mappedOperandTypes.size() == aggCall.getArgList().size();
    for (int i = 0; i < aggCall.getArgList().size(); ++i) {
      int inputRef = aggCall.getArgList().get(i);
      RelDataType operandType = mappedOperandTypes.get(i);
      RexInputRef info = new RexInputRef(inputRef, operandType);
      if (!existingProjects.containsKey(info)) {
        // The getNewPositionAndNode function retrieves the new position and rexNode.  The new position
        // will be "existingProjects.size()", which is the "highest existing projection number + 1".
        // The RexNode in the structure will either be a RexInputRef of the inputRef num if no casting is needed
        // or a RexCall cast of the inputref if casting is needed.
        NewPositionAndNode n = getNewPositionAndNode(aggregateInput, operandType, inputRef, existingProjects.size());
        // add to the new projects.
        existingProjects.put(info, n);
      }
    }
  }

  /**
   * Create a "Mapping" of the existing inputref number within the group set to the
   * new projection input ref number needed.  If the projecction oesn't exist, it
   * will need to be created (within the addIndexedGroupToNewProjects method).
   */
  private Mapping createGroupSetMapping(Aggregate aggregate, RelNode aggregateInput,
      Map<RexInputRef, NewPositionAndNode> newProjects) {
    int maxSize = newProjects.size() + aggregate.getGroupSet().size();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION, maxSize, maxSize);
    for (Integer index : aggregate.getGroupSet()) {
      // after this call, newProjects will be mutated.
      int newPosition = addIndexedGroupToNewProjects(aggregateInput, index, newProjects);
      m.set(index, newPosition);
    }
    return m;
  }

  /**
   * Check if the projection already exists in our "existingProjects".  If it does,
   * return the found new position.  Otherwise, create the new project and return
   * that new position.
   * In this method, we do not need to worry about casting since there is no Impala
   * function to worry about. If you are worried about a function like
   * "group by lower(c1)", don't worry at all!  The "lower" function is handled in
   * a different existing Project RelNode, and isn't relevant to the Aggregate.
   */
  private int addIndexedGroupToNewProjects(RelNode aggregateInput, int index,
      Map<RexInputRef, NewPositionAndNode> existingProjects) {
    // Just use the existing groupType of the input field since no casting is needed.
    RelDataType groupType = aggregateInput.getRowType().getFieldList().get(index).getType();
    RexInputRef info = new RexInputRef(index, groupType);
    NewPositionAndNode n = existingProjects.get(info);
    if (n != null) {
      return n.newPosition;
    }
    n = getNewPositionAndNode(aggregateInput, groupType, index, existingProjects.size());
    existingProjects.put(info, n);
    return n.newPosition;
  }

  /**
   * Create the NewPositionAndNode structure.  The "operand type" is the signature of the Impala function
   * The argNum is the location of the datatype in the input node
   */
  private NewPositionAndNode getNewPositionAndNode(RelNode inputNode, RelDataType operandType, int argNum,
      int newPosition) {
    RexNode newRexNode = inputNode.getCluster().getRexBuilder().makeInputRef(inputNode, argNum);
    // if the mapped operand doesn't match the input ref operand, we need to cast.
    if (!newRexNode.getType().equals(operandType)) {
      newRexNode = inputNode.getCluster().getRexBuilder().makeCast(operandType, newRexNode);
      // unique name given for this field.
    }
    return new NewPositionAndNode(newPosition, newRexNode);
  }

  /**
   * Retrieves all the new positions into a list for the given Aggregate Call.  This list will
   * be used in the final AggregateCall.getArgList()
   */
  private List<Integer> getNewArgList(AggregateCall aggCall, RelNode aggregateInput,
      List<RelDataType> mappedOperandTypes, Map<RexInputRef, NewPositionAndNode> existingProjects) {
    List<Integer> result = Lists.newArrayList();
    assert mappedOperandTypes.size() == aggCall.getArgList().size();
    for (int i = 0; i < aggCall.getArgList().size(); ++i) {
      int inputRef = aggCall.getArgList().get(i);
      RelDataType operandType = mappedOperandTypes.get(i);
      RexInputRef info = new RexInputRef(inputRef, operandType);
      // The project should already have been created by this point.
      NewPositionAndNode newPosAndNode = existingProjects.get(info);
      assert newPosAndNode != null;
      result.add(newPosAndNode.newPosition);
    }
    return result;
  }
}
