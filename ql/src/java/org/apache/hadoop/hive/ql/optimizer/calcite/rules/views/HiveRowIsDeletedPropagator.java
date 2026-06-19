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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild.AlterMaterializedViewRebuildAnalyzer;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * {@link ReflectiveVisitor} to propagate row is deleted or inserted columns to all HiveRelNodes' rowType in the plan.
 * General rule: we expect that these columns are the last columns in the input rowType of the current
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode}.
 *
 * This class is part of incremental rebuild of materialized view plan generation.
 * <br>
 * @see AlterMaterializedViewRebuildAnalyzer
 * @see HiveAggregateInsertDeleteIncrementalRewritingRule
 */
public class HiveRowIsDeletedPropagator implements ReflectiveVisitor {

  private static final String ANY_DELETED_COLUMN_NAME = "_any_deleted";
  private static final String ANY_INSERTED_COLUMN_NAME = "_any_inserted";
  private static final String DELETED_COLUMN_NAME = "_deleted";
  private static final String INSERTED_COLUMN_NAME = "_inserted";

  private final RelBuilder relBuilder;
  private final ReflectUtil.MethodDispatcher<RelNode> dispatcher;

  public HiveRowIsDeletedPropagator(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
    this.dispatcher = ReflectUtil.createMethodDispatcher(
        RelNode.class, this, "visit", RelNode.class, Context.class);
  }

  public RelNode propagate(RelNode relNode) {
    return dispatcher.invoke(relNode, new Context());
  }

  private RelNode visitChild(RelNode parent, int i, RelNode child, Context context) {
    RelNode newRel = dispatcher.invoke(child, context);
    final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
    newInputs.set(i, newRel);
    return parent.copy(parent.getTraitSet(), newInputs);
  }

  private RelNode visitChildren(RelNode rel, Context context) {
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      rel = visitChild(rel, input.i, input.e, context);
    }
    return rel;
  }

  public static final class Context {
    private final Map<Integer, RexNode> rowIdPredicates = new HashMap<>();
  }

  public RelNode visit(RelNode relNode, Context context) {
    return visitChildren(relNode, context);
  }

  // Add a project on top of the TS.
  // Project two boolean columns: one for indicating the row is deleted another
  // for newly inserted.
  // A row is considered to be
  //  - deleted when the ROW_IS_DELETED virtual column is true and the writeId of the record is higher than the
  //    saved in materialized view snapshot metadata
  //  - newly inserted when the ROW_IS_DELETED virtual column is false and the writeId of the record is higher than the
  //    saved in materialized view snapshot metadata
  public RelNode visit(HiveTableScan scan, Context context) {
    RelDataType tableRowType = scan.getTable().getRowType();
    RelDataTypeField rowIdField = getVirtualColumnField(tableRowType, VirtualColumn.ROWID, scan);
    RexNode rowIdPredicate = context.rowIdPredicates.get(rowIdField.getIndex());

    RelDataTypeField rowIsDeletedField = getVirtualColumnField(tableRowType, VirtualColumn.ROWISDELETED, scan);

    RexBuilder rexBuilder = relBuilder.getRexBuilder();

    List<RexNode> projects = new ArrayList<>(tableRowType.getFieldCount());
    List<String> projectNames = new ArrayList<>(tableRowType.getFieldCount());
    populateProjects(rexBuilder, tableRowType, projects, projectNames);
    // Propagated column is already in the TS move it to the end
    RexNode rowIsDeleted = projects.remove(rowIsDeletedField.getIndex());
    projects.add(rowIsDeleted);
    // predicates on rowId introduced by HiveAugmentMaterializationRule into the original MV definition query plan
    // on top of each TS operators.
    // Later that plan is transformed to a Union rewrite plan where all rowId predicates are pulled up on top of
    // the top Join operator.
    if (rowIdPredicate == null) {
      // If a table have not changed then no predicate is introduced for the TS. All rows in the table should remain.
      projects.add(rexBuilder.makeLiteral(false));
      projects.add(rexBuilder.makeLiteral(false));
    } else {
      // A row is deleted if ROW_IS_DELETED is true and rowId > <saved_rowId>
      projects.add(rexBuilder.makeCall(SqlStdOperatorTable.AND, rowIsDeleted, rowIdPredicate));
      // A row is newly inserted if ROW_IS_DELETED is false and rowId > <saved_rowId>
      projects.add(rexBuilder.makeCall(SqlStdOperatorTable.AND,
          rexBuilder.makeCall(SqlStdOperatorTable.NOT, rowIsDeleted), rowIdPredicate));
    }
    String rowIsDeletedName = projectNames.remove(rowIsDeletedField.getIndex());
    projectNames.add(rowIsDeletedName);
    projectNames.add(DELETED_COLUMN_NAME);
    projectNames.add(INSERTED_COLUMN_NAME);

    // Note: as a nature of Calcite if row schema of TS and the new Project would be exactly the same no
    // Project is created.
    return relBuilder
        .push(scan.setTableScanTrait(HiveTableScan.HiveTableScanTrait.FetchDeletedRows))
        .project(projects, projectNames)
        .build();
  }

  // Add the new columns(_deleted, _inserted) to the original project
  public RelNode visit(HiveProject project, Context context) {
    RelNode newProject = visitChild(project, 0, project.getInput(), context);
    RelNode projectInput = newProject.getInput(0);

    List<RexNode> newProjects = new ArrayList<>(project.getProjects().size() + 2);
    newProjects.addAll(project.getProjects());
    newProjects.add(createInputRef(projectInput, 2));
    newProjects.add(createInputRef(projectInput, 1));

    return relBuilder
        .push(projectInput)
        .project(newProjects)
        .build();
  }

  // Union rewrite algorithm pulls up all the predicates on rowId on top of top Join operator:
  // Example:
  //   HiveUnion(all=[true])
  //    ...
  //    HiveFilter(condition=[OR(<(1, $14.writeid), <(1, $6.writeid))])
  //      HiveJoin(condition=[=($0, $8)], joinType=[inner], algorithm=[none], cost=[not available])
  // Check the filter condition and collect operands of OR expressions referencing only one column
  public RelNode visit(HiveFilter filter, Context context) {
    RexNode condition = filter.getCondition();

    // The condition might be a single predicate on the rowId (if only one table changed)
    RexInputRef rexInputRef = findPossibleRowIdRef(filter.getCondition());
    if (rexInputRef != null) {
      context.rowIdPredicates.put(rexInputRef.getIndex(), filter.getCondition());
      return visitChild(filter, 0, filter.getInput(0), context);
    }

    if (!condition.isA(SqlKind.OR)) {
      return visitChild(filter, 0, filter.getInput(0), context);
    }

    for (RexNode operand : ((RexCall)condition).operands) {
      RexInputRef inputRef = findPossibleRowIdRef(operand);
      if (inputRef != null) {
        context.rowIdPredicates.put(inputRef.getIndex(), operand);
      }
    }

    return visitChild(filter, 0, filter.getInput(0), context);
  }

  private RexInputRef findPossibleRowIdRef(RexNode operand) {
    Set<RexInputRef> inputRefs = findRexInputRefs(operand);
    if (inputRefs.size() != 1) {
      return null;
    }

    // This is a candidate for predicate on rowId
    return inputRefs.iterator().next();
  }

  // Propagate new column to each side of the join.
  // Create a project to combine the propagated expressions.
  // Create a filter to remove rows which are joined from a deleted and a newly inserted row.
  public RelNode visit(HiveJoin join, Context context) {
    // Propagate columns to left input
    RelNode tmpJoin = visitChild(join, 0, join.getInput(0), context);
    RelNode newLeftInput = tmpJoin.getInput(0);
    RelDataType newLeftRowType = newLeftInput.getRowType();
    // Propagate columns to right input.
    // All column references should be shifted in candidate predicates to the left
    Context rightContext = new Context();
    int originalLeftFieldCount = join.getInput(0).getRowType().getFieldCount();
    for (Map.Entry<Integer, RexNode> entry : context.rowIdPredicates.entrySet()) {
      if (entry.getKey() > originalLeftFieldCount) {
        rightContext.rowIdPredicates.put(entry.getKey() - originalLeftFieldCount,
          new InputRefShifter(originalLeftFieldCount, -originalLeftFieldCount, relBuilder).apply(entry.getValue()));
      }
    }
    tmpJoin = visitChild(join, 1, join.getInput(1), rightContext);
    RelNode newRightInput = tmpJoin.getInput(1);
    RelDataType newRightRowType = newRightInput.getRowType();

    // Create input refs to propagated columns in left and right inputs
    int rightAnyDeletedIndex = newRightRowType.getFieldCount() - 2;
    int rightAnyInsertedIndex = newRightRowType.getFieldCount() - 1;
    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode leftDeleted = createInputRef(newLeftInput, 2);
    RexNode leftInserted = createInputRef(newLeftInput, 1);
    RexNode rightDeleted = rexBuilder.makeInputRef(
        newRightRowType.getFieldList().get(rightAnyDeletedIndex).getType(),
        newLeftRowType.getFieldCount() + rightAnyDeletedIndex);
    RexNode rightInserted = rexBuilder.makeInputRef(
        newRightRowType.getFieldList().get(rightAnyInsertedIndex).getType(),
        newLeftRowType.getFieldCount() + rightAnyInsertedIndex);

    // Shift column references refers columns coming from right input in join condition since the new left input
    // has a new columns
    int newLeftFieldCount = newLeftRowType.getFieldCount() - 2;
    RexNode newJoinCondition = new InputRefShifter(newLeftFieldCount, 2, relBuilder).apply(join.getCondition());

    // Collect projected columns: all columns from both inputs
    List<RexNode> projects = new ArrayList<>(newLeftFieldCount + newRightRowType.getFieldCount() + 1);
    List<String> projectNames = new ArrayList<>(newLeftFieldCount + newRightRowType.getFieldCount() + 1);
    populateProjects(rexBuilder, newLeftRowType, 0, newLeftFieldCount, projects, projectNames);
    populateProjects(rexBuilder, newRightRowType, newLeftRowType.getFieldCount(),
        newRightRowType.getFieldCount() - 2, projects, projectNames);

    // Create derived expressions
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftDeleted, rightDeleted));
    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftInserted, rightInserted));
    projectNames.add(ANY_DELETED_COLUMN_NAME);
    projectNames.add(ANY_INSERTED_COLUMN_NAME);

    // Create input refs to derived expressions in project
    int anyDeletedIndex = projects.size() - 2;
    int anyInsertedIndex = projects.size() - 1;
    // Get types from corresponding projects, otherwise we may see a Type mismatch AssertionError
    // from Calcite's Litmus. For example,
    //    type mismatch:
    //    ref:
    //    BOOLEAN NOT NULL
    //    input:
    //    BOOLEAN
    RexNode anyDeleted = rexBuilder.makeInputRef(projects.get(anyDeletedIndex).getType(), anyDeletedIndex);
    RexNode anyInserted = rexBuilder.makeInputRef(projects.get(anyInsertedIndex).getType(), anyInsertedIndex);

    // Create filter condition: NOT( (leftDeleted OR rightDeleted) AND (leftInserted OR rightInserted) )
    // We exploit that a row can not be deleted and inserted at the same time.
    RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.NOT,
        RexUtil.composeConjunction(rexBuilder, asList(anyDeleted, anyInserted)));

    return relBuilder
        .push(newLeftInput)
        .push(newRightInput)
        .join(join.getJoinType(), newJoinCondition)
        .project(projects, projectNames)
        .filter(filterCondition)
        .build();
  }

  private RelDataTypeField getVirtualColumnField(
      RelDataType tableRowType, VirtualColumn virtualColumn, HiveTableScan scan) {
    RelDataTypeField field = tableRowType.getField(
        virtualColumn.getName(), false, false);
    if (field == null) {
      // This should not happen since Virtual columns are propagated for all native table scans in
      // CalcitePlanner.genTableLogicalPlan()
      throw new ColumnPropagationException("TableScan " + scan + " row schema does not contain " +
          virtualColumn.getName() + " virtual column");
    }
    return field;
  }

  private void populateProjects(RexBuilder rexBuilder, RelDataType inputRowType,
                                List<RexNode> projects, List<String> projectNames) {
    populateProjects(rexBuilder, inputRowType, 0, inputRowType.getFieldCount(), projects, projectNames);
  }
  private void populateProjects(RexBuilder rexBuilder, RelDataType inputRowType, int offset, int length,
                                List<RexNode> projects, List<String> projectNames) {
    for (int i = 0; i < length; ++i) {
      RelDataTypeField relDataTypeField = inputRowType.getFieldList().get(i);
      projects.add(rexBuilder.makeInputRef(relDataTypeField.getType(), offset + i));
      projectNames.add(relDataTypeField.getName());
    }
  }

  private RexNode createInputRef(RelNode relNode, int negativeOffset) {
    int index = relNode.getRowType().getFieldCount() - negativeOffset;
    return relBuilder.getRexBuilder().makeInputRef(
        relNode.getRowType().getFieldList().get(index).getType(), index);
  }

  private Set<RexInputRef> findRexInputRefs(RexNode rexNode) {
    Set<RexInputRef> rexTableInputRefs = new HashSet<>();
    RexVisitor<RexInputRef> visitor = new RexVisitorImpl<RexInputRef>(true) {

      @Override
      public RexInputRef visitInputRef(RexInputRef inputRef) {
        rexTableInputRefs.add(inputRef);
        return super.visitInputRef(inputRef);
      }
    };

    rexNode.accept(visitor);
    return rexTableInputRefs;
  }
}
