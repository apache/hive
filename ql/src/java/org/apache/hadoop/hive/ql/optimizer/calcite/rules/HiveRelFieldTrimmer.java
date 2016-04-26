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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.CorrelationReferenceFinder;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class HiveRelFieldTrimmer extends RelFieldTrimmer {

  protected static final Log LOG = LogFactory.getLog(HiveRelFieldTrimmer.class);

  private RelBuilder relBuilder;

  private ColumnAccessInfo columnAccessInfo;

  private Map<HiveProject, Table> viewProjectToTableSchema;

  public HiveRelFieldTrimmer(SqlValidator validator, RelBuilder relBuilder) {
    super(validator, relBuilder);
    this.relBuilder = relBuilder;
  }

  public HiveRelFieldTrimmer(SqlValidator validator, RelBuilder relBuilder,
      ColumnAccessInfo columnAccessInfo, Map<HiveProject, Table> viewToTableSchema) {
    super(validator, relBuilder);
    this.relBuilder = relBuilder;
    this.columnAccessInfo = columnAccessInfo;
    this.viewProjectToTableSchema = viewToTableSchema;
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin}.
   */
  public TrimResult trimFields(
      HiveMultiJoin join,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = join.getRowType().getFieldCount();
    final RexNode conditionExpr = join.getCondition();
    final List<RexNode> joinFilters = join.getJoinFilters();

    // Add in fields used in the condition.
    final Set<RelDataTypeField> combinedInputExtraFields =
        new LinkedHashSet<RelDataTypeField>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(combinedInputExtraFields);
    inputFinder.inputBitSet.addAll(fieldsUsed);
    conditionExpr.accept(inputFinder);
    final ImmutableBitSet fieldsUsedPlus = inputFinder.inputBitSet.build();

    int inputStartPos = 0;
    int changeCount = 0;
    int newFieldCount = 0;
    List<RelNode> newInputs = new ArrayList<RelNode>();
    List<Mapping> inputMappings = new ArrayList<Mapping>();
    for (RelNode input : join.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      // Compute required mapping.
      ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
      for (int bit : fieldsUsedPlus) {
        if (bit >= inputStartPos && bit < inputStartPos + inputFieldCount) {
          inputFieldsUsed.set(bit - inputStartPos);
        }
      }

      Set<RelDataTypeField> inputExtraFields =
              Collections.<RelDataTypeField>emptySet();
      TrimResult trimResult =
          trimChild(join, input, inputFieldsUsed.build(), inputExtraFields);
      newInputs.add(trimResult.left);
      if (trimResult.left != input) {
        ++changeCount;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      // Move offset to point to start of next input.
      inputStartPos += inputFieldCount;
      newFieldCount += inputMapping.getTargetCount();
    }

    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            newFieldCount);
    int offset = 0;
    int newOffset = 0;
    for (int i = 0; i < inputMappings.size(); i++) {
      Mapping inputMapping = inputMappings.get(i);
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount();
    }

    if (changeCount == 0
        && mapping.isIdentity()) {
      return new TrimResult(join, Mappings.createIdentity(fieldCount));
    }

    // Build new join.
    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(
            mapping, newInputs.toArray(new RelNode[newInputs.size()]));
    RexNode newConditionExpr = conditionExpr.accept(shuttle);

    List<RexNode> newJoinFilters = Lists.newArrayList();

    for (RexNode joinFilter : joinFilters) {
      newJoinFilters.add(joinFilter.accept(shuttle));
    }

    final RelDataType newRowType = RelOptUtil.permute(join.getCluster().getTypeFactory(),
            join.getRowType(), mapping);
    final RelNode newJoin = new HiveMultiJoin(join.getCluster(),
            newInputs,
            newConditionExpr,
            newRowType,
            join.getJoinInputs(),
            join.getJoinTypes(),
            newJoinFilters);

    return new TrimResult(newJoin, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.Sort}.
   */
  public TrimResult trimFields(
      HiveSortLimit sort,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = sort.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelCollation collation = sort.getCollation();
    final RelNode input = sort.getInput();
    RelOptCluster cluster = sort.getCluster();

    // We use the fields used by the consumer, plus any fields used as sort
    // keys.
    final ImmutableBitSet.Builder inputFieldsUsed =
        ImmutableBitSet.builder(fieldsUsed);
    for (RelFieldCollation field : collation.getFieldCollations()) {
      inputFieldsUsed.set(field.getFieldIndex());
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(sort, input, inputFieldsUsed.build(), inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && inputMapping.isIdentity()
        && fieldsUsed.cardinality() == fieldCount) {
      return result(sort, Mappings.createIdentity(fieldCount));
    }

    relBuilder.push(newInput);
    final int offset =
        sort.offset == null ? 0 : RexLiteral.intValue(sort.offset);
    final int fetch =
        sort.fetch == null ? -1 : RexLiteral.intValue(sort.fetch);
    final ImmutableList<RexNode> fields =
        relBuilder.fields(RexUtil.apply(inputMapping, collation));

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    // TODO: Calcite will return empty LogicalValues when offset == 0 && fetch == 0.
    // However, Hive ASTConverter can not deal with LogicalValues.
    sortLimit(cluster, relBuilder, offset, fetch, fields);
    return result(relBuilder.build(), inputMapping);
  }
  
  private List<RexNode> projects(RelDataType inputRowType, RelOptCluster cluster) {
    final List<RexNode> exprList = new ArrayList<>();
    for (RelDataTypeField field : inputRowType.getFieldList()) {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      exprList.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
    }
    return exprList;
  }
  
  private static RelFieldCollation collation(RexNode node,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection, List<RexNode> extraNodes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return new RelFieldCollation(((RexInputRef) node).getIndex(), direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    case DESCENDING:
      return collation(((RexCall) node).getOperands().get(0),
          RelFieldCollation.Direction.DESCENDING,
          nullDirection, extraNodes);
    case NULLS_FIRST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.FIRST, extraNodes);
    case NULLS_LAST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.LAST, extraNodes);
    default:
      final int fieldIndex = extraNodes.size();
      extraNodes.add(node);
      return new RelFieldCollation(fieldIndex, direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    }
  }
  
 private void sortLimit(RelOptCluster cluster, RelBuilder relBuilder, int offset, int fetch,
     Iterable<? extends RexNode> nodes) {
   final List<RelFieldCollation> fieldCollations = new ArrayList<>();
   final RelDataType inputRowType = relBuilder.peek().getRowType();
   final List<RexNode> extraNodes = projects(inputRowType, cluster);
   final List<RexNode> originalExtraNodes = ImmutableList.copyOf(extraNodes);
   for (RexNode node : nodes) {
     fieldCollations.add(
         collation(node, RelFieldCollation.Direction.ASCENDING,
                 RelFieldCollation.NullDirection.FIRST, extraNodes));
   }
   final RexNode offsetNode = offset <= 0 ? null : relBuilder.literal(offset);
   final RexNode fetchNode = fetch < 0 ? null : relBuilder.literal(fetch);
   if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
     return; // sort is trivial
   }

   final boolean addedFields = extraNodes.size() > originalExtraNodes.size();
   if (fieldCollations.isEmpty()) {
     assert !addedFields;
     RelNode top = relBuilder.peek();
     if (top instanceof Sort) {
       final Sort sort2 = (Sort) top;
       if (sort2.offset == null && sort2.fetch == null) {
         relBuilder.build();
         relBuilder.push(sort2.getInput());
         final RelNode sort =
             HiveSortLimit.create(relBuilder.build(), sort2.collation,
                 offsetNode, fetchNode);
         relBuilder.push(sort);
         return;
       }
     }
     if (top instanceof Project) {
       final Project project = (Project) top;
       if (project.getInput() instanceof Sort) {
         final Sort sort2 = (Sort) project.getInput();
         if (sort2.offset == null && sort2.fetch == null) {
           relBuilder.build();
           relBuilder.push(sort2.getInput());
           final RelNode sort =
               HiveSortLimit.create(relBuilder.build(), sort2.collation,
                   offsetNode, fetchNode);
           relBuilder.push(sort);
           relBuilder.project(project.getProjects());
           return;
         }
       }
     }
   }
   if (addedFields) {
     relBuilder.project(extraNodes);
   }
   final RelNode sort =
       HiveSortLimit.create(relBuilder.build(), RelCollations.of(fieldCollations),
           offsetNode, fetchNode);
   relBuilder.push(sort);
   if (addedFields) {
     relBuilder.project(originalExtraNodes);
   }
   return;
 }
 
  private TrimResult result(RelNode r, final Mapping mapping) {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    for (final CorrelationId correlation : r.getVariablesSet()) {
      r = r.accept(
          new CorrelationReferenceFinder() {
            @Override
            protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)
                  && v.getType().getFieldCount() == mapping.getSourceCount()) {
                final int old = fieldAccess.getField().getIndex();
                final int new_ = mapping.getTarget(old);
                final RelDataTypeFactory.FieldInfoBuilder typeBuilder =
                    relBuilder.getTypeFactory().builder();
                for (int target : Util.range(mapping.getTargetCount())) {
                  typeBuilder.add(
                      v.getType().getFieldList().get(mapping.getSource(target)));
                }
                final RexNode newV =
                    rexBuilder.makeCorrel(typeBuilder.build(), v.id);
                if (old != new_) {
                  return rexBuilder.makeFieldAccess(newV, new_);
                }
              }
              return fieldAccess;
            }

          });
    }
    return new TrimResult(r, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  public TrimResult trimFields(Project project, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // set columnAccessInfo for ViewColumnAuthorization
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        if (this.columnAccessInfo != null && this.viewProjectToTableSchema != null
            && this.viewProjectToTableSchema.containsKey(project)) {
          Table tab = this.viewProjectToTableSchema.get(project);
          this.columnAccessInfo.add(tab.getCompleteName(), tab.getCols().get(ord.i).getName());
        }
      }
    }
    return super.trimFields(project, fieldsUsed, extraFields);
  }

}
