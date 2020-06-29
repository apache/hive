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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimization to reduce the amount of broadcasted/shuffled data throughout the DAG processing.
 * This optimization targets queries with one or more tables joined multiple times on their keys
 * and several columns are projected from those tables.
 *
 * Example:
 * with sq as (
 * select c_customer_id customer_id
 *       ,c_first_name customer_first_name
 *       ,c_last_name customer_last_name
 *   from customer
 * )
 * select c1.customer_id
 *       ,c1.customer_first_name
 *       ,c1.customer_last_name
 *  from sq c1
 *      ,sq c2
 *       ...
 *      ,sq cn
 * where c1.customer_id = c2.customer_id
 *   and ...
 *
 * In this case all column data in the cte will be shuffled.
 *
 * Goal of this optimization: rewrite the plan to include only primary key or non null unique key columns of
 * affected tables and join the them back to the result set of the main query to fetch the rest of the wide columns.
 * This reduces the data size of the affected tables that is broadcast/shuffled throughout the DAG processing.
 *
 *   HiveProject(customer_id=[$0], c_first_name=[$2], c_last_name=[$3])
 *     HiveJoin(condition=[=($0, $1)], joinType=[inner], algorithm=[none], cost=[not available])
 *       (original plan)
 *       HiveProject(customer_id=[$2])
 *         HiveJoin(...)
 *           ...
 *       (joined back customer table)
 *       HiveProject(c_customer_id=[$1], c_first_name=[$8], c_last_name=[$9])
 *         HiveTableScan(table=[[default, customer]], table:alias=[customer])
 */
public class HiveCardinalityPreservingJoinOptimization extends HiveRelFieldTrimmer {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCardinalityPreservingJoinOptimization.class);

  public HiveCardinalityPreservingJoinOptimization() {
    super(false);
  }

  @Override
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    try {
      if (root.getInputs().size() != 1) {
        LOG.debug("Only plans where root has one input are supported. Root: " + root);
        return root;
      }

      REL_BUILDER.set(relBuilder);

      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      RelNode rootInput = root.getInput(0);
      if (rootInput instanceof Aggregate) {
        LOG.debug("Root input is Aggregate: not supported.");
        return root;
      }

      // Build the list of projected fields from root's input RowType
      List<RexInputRef> rootFieldList = new ArrayList<>(rootInput.getRowType().getFieldCount());
      for (int i = 0; i < rootInput.getRowType().getFieldList().size(); ++i) {
        RelDataTypeField relDataTypeField = rootInput.getRowType().getFieldList().get(i);
        rootFieldList.add(rexBuilder.makeInputRef(relDataTypeField.getType(), i));
      }

      List<ProjectedFields> lineages = getExpressionLineageOf(rootFieldList, rootInput);
      if (lineages == null) {
        LOG.debug("Some projected field lineage can not be determined");
        return root;
      }

      // 1. Collect candidate tables for join back
      // 2. Collect all used fields from original plan
      ImmutableBitSet fieldsUsed = ImmutableBitSet.of();
      List<TableToJoinBack> tableToJoinBackList = new ArrayList<>();
      for (ProjectedFields projectedFields : lineages) {
        Optional<ImmutableBitSet> projectedKeys = projectedFields.relOptHiveTable.getNonNullableKeys().stream()
            .filter(projectedFields.fieldsInSourceTable::contains)
            .findFirst();

        if (projectedKeys.isPresent()) {
          TableToJoinBack tableToJoinBack = new TableToJoinBack(projectedKeys.get(), projectedFields);
          tableToJoinBackList.add(tableToJoinBack);
          fieldsUsed = fieldsUsed.union(projectedFields.getSource(projectedKeys.get()));
        } else {
          fieldsUsed = fieldsUsed.union(projectedFields.fieldsInRootProject);
        }
      }

      if (tableToJoinBackList.isEmpty()) {
        LOG.debug("None of the tables has keys projected, unable to join back");
        return root;
      }

      // 3. Trim out non-key fields of joined back tables
      Set<RelDataTypeField> extraFields = Collections.emptySet();
      TrimResult trimResult = dispatchTrimFields(rootInput, fieldsUsed, extraFields);
      RelNode newInput = trimResult.left;
      if (newInput.getRowType().equals(rootInput.getRowType())) {
        LOG.debug("Nothing was trimmed out.");
        return root;
      }

      // 4. Collect fields for new Project on the top of Join backs
      Mapping newInputMapping = trimResult.right;
      RexNode[] newProjects = new RexNode[rootFieldList.size()];
      String[] newColumnNames = new String[rootFieldList.size()];
      projectsFromOriginalPlan(rexBuilder, newInput.getRowType().getFieldCount(), newInput, newInputMapping,
          newProjects, newColumnNames);

      // 5. Join back tables to the top of original plan
      for (TableToJoinBack tableToJoinBack : tableToJoinBackList) {
        LOG.debug("Joining back table " + tableToJoinBack.projectedFields.relOptHiveTable.getName());

        // 5.1 Create new TableScan of tables to join back
        RelOptHiveTable relOptTable = tableToJoinBack.projectedFields.relOptHiveTable;
        RelOptCluster cluster = relBuilder.getCluster();
        HiveTableScan tableScan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            relOptTable, relOptTable.getHiveTableMD().getTableName(), null, false, false);
        // 5.2 Project only required fields from this table
        RelNode projectTableAccessRel = tableScan.project(
            tableToJoinBack.projectedFields.fieldsInSourceTable, new HashSet<>(0), REL_BUILDER.get());

        Mapping keyMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
            tableScan.getRowType().getFieldCount(), tableToJoinBack.keys.cardinality());
        int projectIndex = 0;
        int offset = newInput.getRowType().getFieldCount();

        for (int source : tableToJoinBack.projectedFields.fieldsInSourceTable) {
          if (tableToJoinBack.keys.get(source)) {
            // 5.3 Map key field to it's index in the Project on the TableScan
            keyMapping.set(source, projectIndex);
          } else {
            // 5.4 if this is not a key field then we need it in the new Project on the top of Join backs
            ProjectMapping currentProjectMapping =
                tableToJoinBack.projectedFields.mapping.stream()
                    .filter(projectMapping -> projectMapping.indexInSourceTable == source)
                    .findFirst().get();
            addToProject(projectTableAccessRel, projectIndex, rexBuilder,
                offset + projectIndex,
                currentProjectMapping.indexInRootProject,
                newProjects, newColumnNames);
          }
          ++projectIndex;
        }

        // 5.5 Create Join
        relBuilder.push(newInput);
        relBuilder.push(projectTableAccessRel);

        RexNode joinCondition = joinCondition(
            newInput, newInputMapping, tableToJoinBack, projectTableAccessRel, keyMapping, rexBuilder);

        newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
      }

      // 6 Create Project on top of all Join backs
      relBuilder.push(newInput);
      relBuilder.project(asList(newProjects), asList(newColumnNames));

      return root.copy(root.getTraitSet(), singletonList(relBuilder.build()));
    } finally {
      REL_BUILDER.remove();
    }
  }

  private List<ProjectedFields> getExpressionLineageOf(
      List<RexInputRef> projectExpressions, RelNode projectInput) {
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    Map<RexTableInputRef.RelTableRef, ProjectedFieldsBuilder> fieldMappingBuilders = new HashMap<>();
    List<RexTableInputRef.RelTableRef> tablesOrdered = new ArrayList<>(); // use this list to keep the order of tables
    for (RexInputRef expr : projectExpressions) {
      Set<RexNode> expressionLineage = relMetadataQuery.getExpressionLineage(projectInput, expr);
      if (expressionLineage == null || expressionLineage.size() != 1) {
        LOG.debug("Lineage can not be determined of expression: " + expr);
        return null;
      }

      RexNode rexNode = expressionLineage.iterator().next();
      RexTableInputRef rexTableInputRef = rexTableInputRef(rexNode);
      if (rexTableInputRef == null) {
        LOG.debug("Unable determine expression lineage " + rexNode);
        return null;
      }

      RexTableInputRef.RelTableRef tableRef = rexTableInputRef.getTableRef();
      ProjectedFieldsBuilder projectedFieldsBuilder = fieldMappingBuilders.computeIfAbsent(
          tableRef, k -> {
            tablesOrdered.add(tableRef);
            return new ProjectedFieldsBuilder(tableRef);
          });
      projectedFieldsBuilder.add(expr, rexTableInputRef);
    }

    return tablesOrdered.stream()
        .map(relOptHiveTable -> fieldMappingBuilders.get(relOptHiveTable).build())
        .collect(Collectors.toList());
  }

  public RexTableInputRef rexTableInputRef(RexNode rexNode) {
    if (rexNode.getKind() == SqlKind.TABLE_INPUT_REF) {
      return (RexTableInputRef) rexNode;
    }
    LOG.debug("Unable determine expression lineage " + rexNode);
    return null;
  }

  private void projectsFromOriginalPlan(RexBuilder rexBuilder, int count, RelNode newInput, Mapping newInputMapping,
                                        RexNode[] newProjects, String[] newColumnNames) {
    for (int newProjectIndex = 0; newProjectIndex < count; ++newProjectIndex) {
      addToProject(newInput, newProjectIndex, rexBuilder, newProjectIndex, newInputMapping.getSource(newProjectIndex),
          newProjects, newColumnNames);
    }
  }

  private void addToProject(RelNode relNode, int projectSourceIndex, RexBuilder rexBuilder,
                             int targetIndex, int index,
                             RexNode[] newProjects, String[] newColumnNames) {
    RelDataTypeField relDataTypeField =
        relNode.getRowType().getFieldList().get(projectSourceIndex);
    newProjects[index] = rexBuilder.makeInputRef(
        relDataTypeField.getType(),
        targetIndex);
    newColumnNames[index] = relDataTypeField.getName();
  }

  private RexNode joinCondition(
      RelNode leftInput, Mapping leftInputMapping,
      TableToJoinBack tableToJoinBack, RelNode rightInput, Mapping rightInputKeyMapping,
      RexBuilder rexBuilder) {

    List<RexNode> equalsConditions = new ArrayList<>(tableToJoinBack.keys.size());
    for (ProjectMapping projectMapping : tableToJoinBack.projectedFields.mapping) {
      if (!tableToJoinBack.keys.get(projectMapping.indexInSourceTable)) {
        continue;
      }

      int leftKeyIndex = leftInputMapping.getTarget(projectMapping.indexInRootProject);
      RelDataTypeField leftKeyField = leftInput.getRowType().getFieldList().get(leftKeyIndex);
      int rightKeyIndex = rightInputKeyMapping.getTarget(projectMapping.indexInSourceTable);
      RelDataTypeField rightKeyField = rightInput.getRowType().getFieldList().get(rightKeyIndex);

      equalsConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
          rexBuilder.makeInputRef(rightKeyField.getValue(),
              leftInput.getRowType().getFieldCount() + rightKeyIndex)));
    }
    return RexUtil.composeConjunction(rexBuilder, equalsConditions);
  }

  private static final class ProjectMapping {
    private final int indexInRootProject;
    private final int indexInSourceTable;

    private ProjectMapping(int indexInRootProject, RexTableInputRef rexTableInputRef) {
      this.indexInRootProject = indexInRootProject;
      this.indexInSourceTable = rexTableInputRef.getIndex();
    }
  }

  private static final class ProjectedFields {
    private final RelOptHiveTable relOptHiveTable;
    private final ImmutableBitSet fieldsInRootProject;
    private final ImmutableBitSet fieldsInSourceTable;
    private final List<ProjectMapping> mapping;

    private ProjectedFields(RexTableInputRef.RelTableRef relTableRef,
                            ImmutableBitSet fieldsInRootProject, ImmutableBitSet fieldsInSourceTable,
                            List<ProjectMapping> mapping) {
      this.relOptHiveTable = (RelOptHiveTable) relTableRef.getTable();
      this.fieldsInRootProject = fieldsInRootProject;
      this.fieldsInSourceTable = fieldsInSourceTable;
      this.mapping = mapping;
    }

    public ImmutableBitSet getSource(ImmutableBitSet fields) {
      ImmutableBitSet.Builder targetFieldsBuilder = ImmutableBitSet.builder();
      for (ProjectMapping fieldMapping : mapping) {
        if (fields.get(fieldMapping.indexInSourceTable)) {
          targetFieldsBuilder.set(fieldMapping.indexInRootProject);
        }
      }
      return targetFieldsBuilder.build();
    }
  }

  private static class ProjectedFieldsBuilder {
    private final RexTableInputRef.RelTableRef relTableRef;
    private final ImmutableBitSet.Builder fieldsInRootProjectBuilder = ImmutableBitSet.builder();
    private final ImmutableBitSet.Builder fieldsInSourceTableBuilder = ImmutableBitSet.builder();
    private final List<ProjectMapping> mapping = new ArrayList<>();

    private ProjectedFieldsBuilder(RexTableInputRef.RelTableRef relTableRef) {
      this.relTableRef = relTableRef;
    }

    public void add(RexInputRef rexInputRef, RexTableInputRef sourceTableInputRef) {
      fieldsInRootProjectBuilder.set(rexInputRef.getIndex());
      fieldsInSourceTableBuilder.set(sourceTableInputRef.getIndex());
      mapping.add(new ProjectMapping(rexInputRef.getIndex(), sourceTableInputRef));
    }

    public ProjectedFields build() {
      return new ProjectedFields(relTableRef,
          fieldsInRootProjectBuilder.build(),
          fieldsInSourceTableBuilder.build(),
          mapping);
    }
  }

  private static final class TableToJoinBack {
    private final ProjectedFields projectedFields;
    private final ImmutableBitSet keys;

    private TableToJoinBack(ImmutableBitSet keys, ProjectedFields projectedFields) {
      this.projectedFields = projectedFields;
      this.keys = keys;
    }
  }
}

