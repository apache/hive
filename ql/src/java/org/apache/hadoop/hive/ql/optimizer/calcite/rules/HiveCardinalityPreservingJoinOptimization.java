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

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.findRexTableInputRefs;

import java.util.ArrayList;
import java.util.BitSet;
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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
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
        LOG.debug("Only plans where root has one input are supported. Root: {}", root);
        return root;
      }

      REL_BUILDER.set(relBuilder);

      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      RelNode rootInput = root.getInput(0);

      // Build the list of RexInputRef from root input RowType
      List<RexInputRef> rootFieldList = new ArrayList<>(rootInput.getRowType().getFieldCount());
      List<String> newColumnNames = new ArrayList<>();
      for (int i = 0; i < rootInput.getRowType().getFieldList().size(); ++i) {
        RelDataTypeField relDataTypeField = rootInput.getRowType().getFieldList().get(i);
        rootFieldList.add(rexBuilder.makeInputRef(relDataTypeField.getType(), i));
        newColumnNames.add(relDataTypeField.getName());
      }

      // Bit set to gather the refs that backtrack to constant values
      BitSet constants = new BitSet();
      List<JoinedBackFields> lineages = getExpressionLineageOf(rootFieldList, rootInput, constants);
      if (lineages == null) {
        LOG.debug("Some projected field lineage can not be determined");
        return root;
      }

      // 1. Collect candidate tables for join back and map RexNodes coming from those tables to their index in the
      // rootInput row type
      // Collect all used fields from original plan
      ImmutableBitSet fieldsUsed = ImmutableBitSet.of(constants.stream().toArray());
      List<TableToJoinBack> tableToJoinBackList = new ArrayList<>(lineages.size());
      Map<Integer, RexNode> rexNodesToShuttle = new HashMap<>(rootInput.getRowType().getFieldCount());

      for (JoinedBackFields joinedBackFields : lineages) {
        Optional<ImmutableBitSet> projectedKeys = joinedBackFields.relOptHiveTable.getNonNullableKeys().stream()
            .filter(joinedBackFields.fieldsInSourceTable::contains)
            .findFirst();

        if (projectedKeys.isPresent() && !projectedKeys.get().equals(joinedBackFields.fieldsInSourceTable)) {
          TableToJoinBack tableToJoinBack = new TableToJoinBack(projectedKeys.get(), joinedBackFields);
          tableToJoinBackList.add(tableToJoinBack);
          fieldsUsed = fieldsUsed.union(joinedBackFields.getSource(projectedKeys.get()));

          for (TableInputRefHolder mapping : joinedBackFields.mapping) {
            if (!fieldsUsed.get(mapping.indexInOriginalRowType)) {
              rexNodesToShuttle.put(mapping.indexInOriginalRowType, mapping.rexNode);
            }
          }
        } else {
          fieldsUsed = fieldsUsed.union(joinedBackFields.fieldsInOriginalRowType);
        }
      }

      if (tableToJoinBackList.isEmpty()) {
        LOG.debug("None of the tables has keys projected, unable to join back");
        return root;
      }

      // 2. Trim out non-key fields of joined back tables
      Set<RelDataTypeField> extraFields = Collections.emptySet();
      TrimResult trimResult = dispatchTrimFields(rootInput, fieldsUsed, extraFields);
      RelNode newInput = trimResult.left;
      if (newInput.getRowType().equals(rootInput.getRowType())) {
        LOG.debug("Nothing was trimmed out.");
        return root;
      }

      // 3. Join back tables to the top of original plan
      Mapping newInputMapping = trimResult.right;
      Map<RexTableInputRef, Integer> tableInputRefMapping = new HashMap<>();

      for (TableToJoinBack tableToJoinBack : tableToJoinBackList) {
        LOG.debug("Joining back table {}", tableToJoinBack.joinedBackFields.relOptHiveTable.getName());

        // 3.1. Create new TableScan of tables to join back
        RelOptHiveTable relOptTable = tableToJoinBack.joinedBackFields.relOptHiveTable;
        RelOptCluster cluster = relBuilder.getCluster();
        HiveTableScan tableScan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            relOptTable, relOptTable.getHiveTableMD().getTableName(), null, false, false);
        // 3.2. Create Project with the required fields from this table
        RelNode projectTableAccessRel = tableScan.project(
            tableToJoinBack.joinedBackFields.fieldsInSourceTable, new HashSet<>(0), REL_BUILDER.get());

        // 3.3. Create mapping between the Project and TableScan
        Mapping projectMapping = Mappings.create(MappingType.INVERSE_SURJECTION,
            tableScan.getRowType().getFieldCount(),
            tableToJoinBack.joinedBackFields.fieldsInSourceTable.cardinality());
        int projectIndex = 0;
        for (int i : tableToJoinBack.joinedBackFields.fieldsInSourceTable) {
          projectMapping.set(i, projectIndex);
          ++projectIndex;
        }

        int offset = newInput.getRowType().getFieldCount();

        // 3.4. Map rexTableInputRef to the index where it can be found in the new Input row type
        for (TableInputRefHolder mapping : tableToJoinBack.joinedBackFields.mapping) {
          int indexInSourceTable = mapping.tableInputRef.getIndex();
          if (!tableToJoinBack.keys.get(indexInSourceTable)) {
            // 3.5. if this is not a key field it is shifted by the left input field count
            tableInputRefMapping.put(mapping.tableInputRef, offset + projectMapping.getTarget(indexInSourceTable));
          }
        }

        // 3.7. Create Join
        relBuilder.push(newInput);
        relBuilder.push(projectTableAccessRel);

        RexNode joinCondition = joinCondition(
            newInput, newInputMapping, tableToJoinBack, projectTableAccessRel, projectMapping, rexBuilder);

        newInput = relBuilder.join(JoinRelType.INNER, joinCondition).build();
      }

      // 4. Collect rexNodes for Project
      TableInputRefMapper mapper = new TableInputRefMapper(tableInputRefMapping, rexBuilder, newInput);
      List<RexNode> rexNodeList = new ArrayList<>(rootInput.getRowType().getFieldCount());
      for (int i = 0; i < rootInput.getRowType().getFieldCount(); i++) {
        RexNode rexNode = rexNodesToShuttle.get(i);
        if (rexNode != null) {
          rexNodeList.add(mapper.apply(rexNode));
        } else {
          int target = newInputMapping.getTarget(i);
          rexNodeList.add(
              rexBuilder.makeInputRef(newInput.getRowType().getFieldList().get(target).getType(), target));
        }
      }

      // 5. Create Project on top of all Join backs
      relBuilder.push(newInput);
      relBuilder.project(rexNodeList, newColumnNames);

      return root.copy(root.getTraitSet(), singletonList(relBuilder.build()));
    } finally {
      REL_BUILDER.remove();
    }
  }

  private List<JoinedBackFields> getExpressionLineageOf(
      List<RexInputRef> projectExpressions, RelNode projectInput, BitSet constants) {
    RelMetadataQuery relMetadataQuery = RelMetadataQuery.instance();
    Map<RexTableInputRef.RelTableRef, JoinedBackFieldsBuilder> fieldMappingBuilders = new HashMap<>();
    List<RexTableInputRef.RelTableRef> tablesOrdered = new ArrayList<>(); // use this list to keep the order of tables
    for (RexInputRef expr : projectExpressions) {
      Set<RexNode> expressionLineage = relMetadataQuery.getExpressionLineage(projectInput, expr);
      if (expressionLineage == null || expressionLineage.size() != 1) {
        LOG.debug("Lineage of expression in node {} can not be determined: {}", projectInput, expr);
        return null;
      }

      RexNode rexNode = expressionLineage.iterator().next();
      Set<RexTableInputRef> refs = findRexTableInputRefs(rexNode);
      if (refs.isEmpty()) {
        if (!RexUtil.isConstant(rexNode)) {
          LOG.debug("Unknown expression that should be a constant: {}", rexNode);
          return null;
        }
        constants.set(expr.getIndex());
      } else {
        for (RexTableInputRef rexTableInputRef : refs) {
          RexTableInputRef.RelTableRef tableRef = rexTableInputRef.getTableRef();
          JoinedBackFieldsBuilder joinedBackFieldsBuilder = fieldMappingBuilders.computeIfAbsent(
              tableRef, k -> {
                tablesOrdered.add(tableRef);
                return new JoinedBackFieldsBuilder(tableRef);
              });
          joinedBackFieldsBuilder.add(expr, rexNode, rexTableInputRef);
        }
      }
    }

    return tablesOrdered.stream()
        .map(relOptHiveTable -> fieldMappingBuilders.get(relOptHiveTable).build())
        .collect(Collectors.toList());
  }

  private RexNode joinCondition(
      RelNode leftInput, Mapping leftInputMapping,
      TableToJoinBack tableToJoinBack, RelNode rightInput, Mapping rightInputKeyMapping,
      RexBuilder rexBuilder) {

    List<RexNode> equalsConditions = new ArrayList<>(tableToJoinBack.keys.size());
    BitSet usedKeys = new BitSet(0);
    for (TableInputRefHolder tableInputRefHolder : tableToJoinBack.joinedBackFields.mapping) {
      if (usedKeys.get(tableInputRefHolder.tableInputRef.getIndex()) ||
          !tableToJoinBack.keys.get(tableInputRefHolder.tableInputRef.getIndex())) {
        continue;
      }

      usedKeys.set(tableInputRefHolder.tableInputRef.getIndex());

      int leftKeyIndex = leftInputMapping.getTarget(tableInputRefHolder.indexInOriginalRowType);
      RelDataTypeField leftKeyField = leftInput.getRowType().getFieldList().get(leftKeyIndex);
      int rightKeyIndex = rightInputKeyMapping.getTarget(tableInputRefHolder.tableInputRef.getIndex());
      RelDataTypeField rightKeyField = rightInput.getRowType().getFieldList().get(rightKeyIndex);

      equalsConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(leftKeyField.getValue(), leftKeyField.getIndex()),
          rexBuilder.makeInputRef(rightKeyField.getValue(),
              leftInput.getRowType().getFieldCount() + rightKeyIndex)));
    }
    return RexUtil.composeConjunction(rexBuilder, equalsConditions);
  }

  private static final class TableInputRefHolder {
    private final RexTableInputRef tableInputRef;
    private final RexNode rexNode;
    private final int indexInOriginalRowType;

    private TableInputRefHolder(RexInputRef inputRef, RexNode rexNode, RexTableInputRef sourceTableRef) {
      this.indexInOriginalRowType = inputRef.getIndex();
      this.rexNode = rexNode;
      this.tableInputRef = sourceTableRef;
    }
  }

  private static final class JoinedBackFields {
    private final RelOptHiveTable relOptHiveTable;
    private final ImmutableBitSet fieldsInOriginalRowType;
    private final ImmutableBitSet fieldsInSourceTable;
    private final List<TableInputRefHolder> mapping;

    private JoinedBackFields(RexTableInputRef.RelTableRef relTableRef,
                             ImmutableBitSet fieldsInOriginalRowType, ImmutableBitSet fieldsInSourceTable,
                             List<TableInputRefHolder> mapping) {
      this.relOptHiveTable = (RelOptHiveTable) relTableRef.getTable();
      this.fieldsInOriginalRowType = fieldsInOriginalRowType;
      this.fieldsInSourceTable = fieldsInSourceTable;
      this.mapping = mapping;
    }

    public ImmutableBitSet getSource(ImmutableBitSet fields) {
      ImmutableBitSet.Builder targetFieldsBuilder = ImmutableBitSet.builder();
      for (TableInputRefHolder fieldMapping : mapping) {
        if (fields.get(fieldMapping.tableInputRef.getIndex())) {
          targetFieldsBuilder.set(fieldMapping.indexInOriginalRowType);
        }
      }
      return targetFieldsBuilder.build();
    }
  }

  private static class JoinedBackFieldsBuilder {
    private final RexTableInputRef.RelTableRef relTableRef;
    private final ImmutableBitSet.Builder fieldsInOriginalRowTypeBuilder = ImmutableBitSet.builder();
    private final ImmutableBitSet.Builder fieldsInSourceTableBuilder = ImmutableBitSet.builder();
    private final List<TableInputRefHolder> mapping = new ArrayList<>();

    private JoinedBackFieldsBuilder(RexTableInputRef.RelTableRef relTableRef) {
      this.relTableRef = relTableRef;
    }

    public void add(RexInputRef rexInputRef, RexNode rexNode, RexTableInputRef sourceTableInputRef) {
      fieldsInOriginalRowTypeBuilder.set(rexInputRef.getIndex());
      fieldsInSourceTableBuilder.set(sourceTableInputRef.getIndex());
      mapping.add(new TableInputRefHolder(rexInputRef, rexNode, sourceTableInputRef));
    }

    public JoinedBackFields build() {
      return new JoinedBackFields(relTableRef,
          fieldsInOriginalRowTypeBuilder.build(),
          fieldsInSourceTableBuilder.build(),
          mapping);
    }
  }

  private static final class TableToJoinBack {
    private final JoinedBackFields joinedBackFields;
    private final ImmutableBitSet keys;

    private TableToJoinBack(ImmutableBitSet keys, JoinedBackFields joinedBackFields) {
      this.joinedBackFields = joinedBackFields;
      this.keys = keys;
    }
  }

  private static final class TableInputRefMapper extends RexShuttle {
    private final Map<RexTableInputRef, Integer> tableInputRefMapping;
    private final RexBuilder rexBuilder;
    private final RelNode newInput;

    private TableInputRefMapper(Map<RexTableInputRef, Integer> tableInputRefMapping, RexBuilder rexBuilder, RelNode newInput) {
      this.tableInputRefMapping = tableInputRefMapping;
      this.rexBuilder = rexBuilder;
      this.newInput = newInput;
    }

    @Override
    public RexNode visitTableInputRef(RexTableInputRef ref) {
      int source = tableInputRefMapping.get(ref);
      return rexBuilder.makeInputRef(newInput.getRowType().getFieldList().get(source).getType(), source);
    }
  }
}

