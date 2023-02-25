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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.CorrelationReferenceFinder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HiveRelFieldTrimmer extends RelFieldTrimmer {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRelFieldTrimmer.class);

  // We initialize the field trimmer statically here and we will reuse it across
  // queries. The reason is that otherwise we will create a new dispatcher with
  // each instantiation, thus effectively removing the caching mechanism that is
  // built within the dispatcher.
  private static final HiveRelFieldTrimmer FIELD_TRIMMER_STATS =
      new HiveRelFieldTrimmer(true);
  private static final HiveRelFieldTrimmer FIELD_TRIMMER_NO_STATS =
      new HiveRelFieldTrimmer(false);
  // For testing
  private static final HiveRelFieldTrimmer FIELD_TRIMMER_STATS_METHOD_DISPATCHER =
      new HiveRelFieldTrimmer(true, false);
  private static final HiveRelFieldTrimmer FIELD_TRIMMER_NO_STATS_METHOD_DISPATCHER =
      new HiveRelFieldTrimmer(false, false);

  private final boolean fetchStats;

  private static final ThreadLocal<ColumnAccessInfo> COLUMN_ACCESS_INFO =
      new ThreadLocal<>();
  private static final ThreadLocal<Map<HiveProject, Table>> VIEW_PROJECT_TO_TABLE_SCHEMA =
      new ThreadLocal<>();


  protected HiveRelFieldTrimmer(boolean fetchStats) {
    this(fetchStats, true);
  }

  private HiveRelFieldTrimmer(boolean fetchStats, boolean useLMFBasedDispatcher) {
    super(useLMFBasedDispatcher);
    this.fetchStats = fetchStats;
  }

  /**
   * Returns a HiveRelFieldTrimmer instance that does not retrieve
   * stats.
   */
  public static HiveRelFieldTrimmer get() {
    return get(false);
  }

  /**
   * Returns a HiveRelFieldTrimmer instance that can retrieve stats.
   */
  public static HiveRelFieldTrimmer get(boolean fetchStats) {
    return get(fetchStats, true);
  }

  /**
   * Returns a HiveRelFieldTrimmer instance that can retrieve stats and use
   * a custom dispatcher.
   */
  public static HiveRelFieldTrimmer get(boolean fetchStats, boolean useLMFBasedDispatcher) {
    return fetchStats ?
        (useLMFBasedDispatcher ? FIELD_TRIMMER_STATS : FIELD_TRIMMER_STATS_METHOD_DISPATCHER) :
        (useLMFBasedDispatcher ? FIELD_TRIMMER_NO_STATS : FIELD_TRIMMER_NO_STATS_METHOD_DISPATCHER);
  }

  /**
   * Trims unused fields from a relational expression.
   *
   * <p>We presume that all fields of the relational expression are wanted by
   * its consumer, so only trim fields that are not used within the tree.
   *
   * @param root Root node of relational expression
   * @return Trimmed relational expression
   */
  @Override
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    return trim(relBuilder, root, null, null);
  }

  public RelNode trim(RelBuilder relBuilder, RelNode root,
      ColumnAccessInfo columnAccessInfo, Map<HiveProject, Table> viewToTableSchema) {
    try {
      // Set local thread variables
      COLUMN_ACCESS_INFO.set(columnAccessInfo);
      VIEW_PROJECT_TO_TABLE_SCHEMA.set(viewToTableSchema);
      // Execute pruning
      return super.trim(relBuilder, root);
    } finally {
      // Always remove the local thread variables to avoid leaks
      COLUMN_ACCESS_INFO.remove();
      VIEW_PROJECT_TO_TABLE_SCHEMA.remove();
    }
  }

  /**
   * Trims the fields of an input relational expression.
   *
   * @param rel        Relational expression
   * @param input      Input relational expression, whose fields to trim
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChild(
      RelNode rel,
      RelNode input,
      final ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final ImmutableBitSet.Builder fieldsUsedBuilder = fieldsUsed.rebuild();

    // Correlating variables are a means for other relational expressions to use
    // fields.
    for (final CorrelationId correlation : rel.getVariablesSet()) {
      rel.accept(
          new CorrelationReferenceFinder() {
            protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)) {
                fieldsUsedBuilder.set(fieldAccess.getField().getIndex());
              }
              return fieldAccess;
            }
          });
    }

    return dispatchTrimFields(input, fieldsUsedBuilder.build(), extraFields);
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
        new RelOptUtil.InputFinder(combinedInputExtraFields, fieldsUsed);
    conditionExpr.accept(inputFinder);
    final ImmutableBitSet fieldsUsedPlus = inputFinder.build();

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
   * {@link org.apache.calcite.adapter.druid.DruidQuery}.
   */
  public TrimResult trimFields(DruidQuery dq, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = dq.getRowType().getFieldCount();
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
        && extraFields.isEmpty()) {
      // if there is nothing to project or if we are projecting everything
      // then no need to introduce another RelNode
      return trimFields(
          (RelNode) dq, fieldsUsed, extraFields);
    }
    final RelNode newTableAccessRel = project(dq, fieldsUsed, extraFields, REL_BUILDER.get());

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0) {
      RelNode input = newTableAccessRel;
      if (input instanceof Project) {
        // The table has implemented the project in the obvious way - by
        // creating project with 0 fields. Strip it away, and create our own
        // project with one field.
        Project project = (Project) input;
        if (project.getRowType().getFieldCount() == 0) {
          input = project.getInput();
        }
      }
      return dummyProject(fieldCount, input);
    }

    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    return result(newTableAccessRel, mapping);
  }

  private static RelNode project(DruidQuery dq, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields, RelBuilder relBuilder) {
    final int fieldCount = dq.getRowType().getFieldCount();
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
        && extraFields.isEmpty()) {
      return dq;
    }
    final List<RexNode> exprList = new ArrayList<>();
    final List<String> nameList = new ArrayList<>();
    final RexBuilder rexBuilder = dq.getCluster().getRexBuilder();
    final List<RelDataTypeField> fields = dq.getRowType().getFieldList();

    // Project the subset of fields.
    for (int i : fieldsUsed) {
      RelDataTypeField field = fields.get(i);
      exprList.add(rexBuilder.makeInputRef(dq, i));
      nameList.add(field.getName());
    }

    // Project nulls for the extra fields. (Maybe a sub-class table has
    // extra fields, but we don't.)
    for (RelDataTypeField extraField : extraFields) {
      exprList.add(
          rexBuilder.ensureType(
              extraField.getType(),
              rexBuilder.constantNull(),
              true));
      nameList.add(extraField.getName());
    }

    HiveProject hp = (HiveProject) relBuilder.push(dq).project(exprList, nameList).build();
    hp.setSynthetic();
    return hp;
  }

  private boolean isRexLiteral(final RexNode rexNode) {
    if(rexNode instanceof RexLiteral) {
      return true;
    } else if(rexNode instanceof RexCall
        && ((RexCall)rexNode).getOperator().getKind() == SqlKind.CAST){
      return isRexLiteral(((RexCall)(rexNode)).getOperands().get(0));
    } else {
      return false;
    }
  }


  // Given a groupset this tries to find out if the cardinality of the grouping columns could have changed
  // because if not, and it consists of keys (unique + not null OR pk), we can safely remove rest of the columns
  // if those are columns are not being used further up
  private ImmutableBitSet generateGroupSetIfCardinalitySame(final Aggregate aggregate,
      final ImmutableBitSet originalGroupSet, final ImmutableBitSet fieldsUsed) {

    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelMetadataQuery mq = aggregate.getCluster().getMetadataQuery();

    // map from backtracked table ref to list of gb keys and list of corresponding backtracked columns
    Map<RexTableInputRef.RelTableRef, List<Pair<Integer, Integer>>> mapGBKeysLineage= new HashMap<>();

    // map from table ref to list of columns (from gb keys) which are candidate to be removed
    Map<RexTableInputRef.RelTableRef, List<Integer>> candidateKeys = new HashMap<>();

    for(int key:originalGroupSet) {
      RexNode inputRef = rexBuilder.makeInputRef(aggregate.getInput(), key);
      Set<RexNode> exprLineage = mq.getExpressionLineage(aggregate.getInput(), inputRef);
      if(exprLineage != null && exprLineage.size() == 1){
        RexNode expr = exprLineage.iterator().next();
        if(expr instanceof RexTableInputRef) {
          RexTableInputRef tblRef = (RexTableInputRef)expr;
          if(mapGBKeysLineage.containsKey(tblRef.getTableRef())) {
            mapGBKeysLineage.get(tblRef.getTableRef()).add(Pair.of(tblRef.getIndex(), key));
          } else {
            List<Pair<Integer, Integer>> newList = new ArrayList<>();
            newList.add(Pair.of(tblRef.getIndex(), key));
            mapGBKeysLineage.put(tblRef.getTableRef(), newList);
          }
        } else if(RexUtil.isDeterministic(expr)){
          // even though we weren't able to backtrack this key it could still be candidate for removal
          // if rest of the columns contain pk/unique
          Set<RexTableInputRef.RelTableRef> tableRefs = RexUtil.gatherTableReferences(Lists.newArrayList(expr));
          if(tableRefs.size() == 1) {
            RexTableInputRef.RelTableRef tblRef = tableRefs.iterator().next();
            if(candidateKeys.containsKey(tblRef)) {
              List<Integer> candidateGBKeys = candidateKeys.get(tblRef);
              candidateGBKeys.add(key);
            } else {
              List<Integer> candidateGBKeys =  new ArrayList<>();
              candidateGBKeys.add(key);
              candidateKeys.put(tblRef, candidateGBKeys);
            }
          }
        }
      }
    }

    // we want to delete all columns in original GB set except the key
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

    for(Map.Entry<RexTableInputRef.RelTableRef, List<Pair<Integer, Integer>>> entry:mapGBKeysLineage.entrySet()) {
      RelOptHiveTable tbl = (RelOptHiveTable)entry.getKey().getTable();
      List<Pair<Integer, Integer>> gbKeyCols = entry.getValue();

      ImmutableBitSet.Builder btBuilder = ImmutableBitSet.builder();
      gbKeyCols.forEach(pair -> btBuilder.set(pair.left));
      ImmutableBitSet backtrackedGBSet = btBuilder.build();

      List<ImmutableBitSet> allKeys = tbl.getNonNullableKeys();
      ImmutableBitSet currentKey = null;
      for(ImmutableBitSet key:allKeys) {
        if(backtrackedGBSet.contains(key)) {
          // only if grouping sets consist of keys
          currentKey = key;
          break;
        }
      }
      if(currentKey == null || currentKey.isEmpty()) {
        continue;
      }

      // we have established that this gb set contains keys and it is safe to remove rest of the columns
      for(Pair<Integer, Integer> gbKeyColPair:gbKeyCols) {
        Integer backtrackedCol = gbKeyColPair.left;
        Integer orgCol =  gbKeyColPair.right;
        if(!fieldsUsed.get(orgCol)
            && !currentKey.get(backtrackedCol)) {
          // this could could be removed
          builder.set(orgCol);
        }
      }
      // remove candidate keys if possible
      if(candidateKeys.containsKey(entry.getKey())) {
        List<Integer> candidateGbKeys= candidateKeys.get(entry.getKey());
        for(Integer keyToRemove:candidateGbKeys) {
          if(!fieldsUsed.get(keyToRemove)) {
            builder.set(keyToRemove);
          }
        }
      }
    }
    ImmutableBitSet keysToRemove = builder.build();
    ImmutableBitSet newGroupSet = originalGroupSet.except(keysToRemove);
    assert(!newGroupSet.isEmpty());
    return newGroupSet;
  }

  // if gby keys consist of pk/uk non-pk/non-uk columns are removed if they are not being used
  private ImmutableBitSet generateNewGroupset(Aggregate aggregate, ImmutableBitSet fieldsUsed) {

    ImmutableBitSet originalGroupSet = aggregate.getGroupSet();

    if (aggregate.getGroupSets().size() > 1 || aggregate.getIndicatorCount() > 0
        || fieldsUsed.contains(originalGroupSet)) {
      // if there is grouping sets, indicator or all the group keys are being used we do no need to proceed further
      return originalGroupSet;
    }

    final RelNode input = aggregate.getInput();
    RelMetadataQuery mq = aggregate.getCluster().getMetadataQuery();

    final Set<ImmutableBitSet> uniqueKeys = mq.getUniqueKeys(input, false);
    if (uniqueKeys == null || uniqueKeys.isEmpty()) {
      return generateGroupSetIfCardinalitySame(aggregate, originalGroupSet, fieldsUsed);
    }

    // we have set of unique key, get to the key which is same as group by key
    ImmutableBitSet groupByUniqueKey = null;

    for (ImmutableBitSet key : uniqueKeys) {
      if (aggregate.getGroupSet().contains(key)) {
        groupByUniqueKey = key;
        break;
      }
    }

    if (groupByUniqueKey == null) {
      // group by keys do not represent unique keys
      return originalGroupSet;
    }

    // we know group by key contains primary key and there is at least one column in group by which is not being used
    // if that column is not part of key it should be removed
    ImmutableBitSet nonKeyColumns = aggregate.getGroupSet().except(groupByUniqueKey);
    ImmutableBitSet columnsToRemove = nonKeyColumns.except(fieldsUsed);
    ImmutableBitSet newGroupSet = aggregate.getGroupSet().except(columnsToRemove);

    return  newGroupSet;
  }

  /**
   * This method replaces group by 'constant key' with group by true (boolean)
   * if and only if
   *  group by doesn't have grouping sets
   *  all keys in group by are constant
   *  none of the relnode above aggregate refers to these keys
   *
   *  If all of above is true then group by is rewritten and a new project is introduced
   *  underneath aggregate
   *
   *  This is mainly done so that hive is able to push down queries with
   *  group by 'constant key with type not supported by druid' into druid.
   *
   */
  private Aggregate rewriteGBConstantKeys(Aggregate aggregate, ImmutableBitSet fieldsUsed,
      ImmutableBitSet aggCallFields) {
    if ((aggregate.getIndicatorCount() > 0)
        || (aggregate.getGroupSet().isEmpty())
        || fieldsUsed.contains(aggregate.getGroupSet())) {
      return aggregate;
    }

    final RelNode input = aggregate.getInput();

    final RelDataType rowType = input.getRowType();
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<RexNode> newProjects = new ArrayList<>();

    final List<RexNode> inputExprs = input instanceof Project ? ((Project) input).getProjects() : null;
    if (inputExprs == null || inputExprs.isEmpty()) {
      return aggregate;
    }

    boolean allConstants = true;
    for (int key : aggregate.getGroupSet()) {
      // getChildExprs on Join could return less number of expressions than there are coming out of join
      if (inputExprs.size() <= key || !isRexLiteral(inputExprs.get(key))) {
        allConstants = false;
        break;
      }
    }

    if (allConstants) {
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        if (aggregate.getGroupSet().get(i) && !aggCallFields.get(i)) {
          newProjects.add(rexBuilder.makeLiteral(true));
        } else {
          newProjects.add(rexBuilder.makeInputRef(input, i));
        }
      }
      final RelBuilder relBuilder = REL_BUILDER.get();
      relBuilder.push(input);
      relBuilder.project(newProjects);
      Aggregate newAggregate = new HiveAggregate(aggregate.getCluster(), aggregate.getTraitSet(), relBuilder.build(),
          aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
      return newAggregate;
    }
    return aggregate;
  }

  @Override
  public TrimResult trimFields(Aggregate aggregate, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    // Fields:
    //
    // | sys fields | group fields | indicator fields | agg functions |
    //
    // Two kinds of trimming:
    //
    // 1. If agg rel has system fields but none of these are used, create an
    // agg rel with no system fields.
    //
    // 2. If aggregate functions are not used, remove them.
    //
    // But group and indicator fields stay, even if they are not used.

    // Compute which input fields are used.


    // agg functions
    // agg functions are added first (before group sets) because rewriteGBConstantsKeys
    // needs it
    final ImmutableBitSet.Builder aggCallFieldsUsedBuilder =  ImmutableBitSet.builder();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      for (int i : aggCall.getArgList()) {
        aggCallFieldsUsedBuilder.set(i);
      }
      if (aggCall.filterArg >= 0) {
        aggCallFieldsUsedBuilder.set(aggCall.filterArg);
      }
      aggCallFieldsUsedBuilder.addAll(RelCollations.ordinals(aggCall.collation));
    }

    // transform if group by contain constant keys
    ImmutableBitSet aggCallFieldsUsed = aggCallFieldsUsedBuilder.build();
    aggregate = rewriteGBConstantKeys(aggregate, fieldsUsed, aggCallFieldsUsed);

    // add group fields
    final ImmutableBitSet.Builder inputFieldsUsed =  aggregate.getGroupSet().rebuild();
    inputFieldsUsed.addAll(aggCallFieldsUsed);


    final RelDataType rowType = aggregate.getRowType();

    // Create input with trimmed columns.
    final RelNode input = aggregate.getInput();
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    final TrimResult trimResult =
        trimChild(aggregate, input, inputFieldsUsed.build(), inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    ImmutableBitSet originalGroupSet = aggregate.getGroupSet();
    ImmutableBitSet updatedGroupSet = generateNewGroupset(aggregate, fieldsUsed);
    ImmutableBitSet gbKeysDeleted = originalGroupSet.except(updatedGroupSet);
    ImmutableBitSet updatedGroupFields = ImmutableBitSet.range(originalGroupSet.cardinality());
    final int updatedGroupCount = updatedGroupSet.cardinality();

    // we need to clear the bits corresponding to deleted gb keys
    int setIdx = 0;
    while(setIdx != -1) {
      setIdx = gbKeysDeleted.nextSetBit(setIdx);
      if(setIdx != -1) {
        updatedGroupFields = updatedGroupFields.clear(setIdx);
        setIdx++;
      }
    }
    fieldsUsed =
        fieldsUsed.union(updatedGroupFields);

    // If the input is unchanged, and we need to project all columns,
    // there's nothing to do.
    if (input == newInput
        && fieldsUsed.equals(ImmutableBitSet.range(rowType.getFieldCount()))) {
      return result(aggregate,
          Mappings.createIdentity(rowType.getFieldCount()));
    }

    // update the group by keys based on inputMapping
    ImmutableBitSet newGroupSet =
        Mappings.apply(inputMapping, updatedGroupSet);

    // Which agg calls are used by our consumer?
    int originalGroupCount = aggregate.getGroupSet().cardinality();
    int j = originalGroupCount;
    int usedAggCallCount = 0;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      if (fieldsUsed.get(j++)) {
        ++usedAggCallCount;
      }
    }

    // Offset due to the number of system fields having changed.
    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            rowType.getFieldCount(),
            updatedGroupCount + usedAggCallCount);


    // if group keys were reduced, it means we didn't have grouping therefore
    // we don't need to transform group sets
    ImmutableList<ImmutableBitSet> newGroupSets = null;
    if(!updatedGroupSet.equals(aggregate.getGroupSet())) {
      newGroupSets = ImmutableList.of(newGroupSet);
    } else {
      newGroupSets = ImmutableList.copyOf(
          Iterables.transform(aggregate.getGroupSets(),
              input1 -> Mappings.apply(inputMapping, input1)));
    }

    // Populate mapping of where to find the fields. System, group key and
    // indicator fields first.
    int gbKeyIdx = 0;
    for (j = 0; j < originalGroupCount; j++) {
      if(fieldsUsed.get(j)) {
        mapping.set(j, gbKeyIdx);
        gbKeyIdx++;
      }
    }

    // Now create new agg calls, and populate mapping for them.
    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInput);
    final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
    j = originalGroupCount; // because lookup in fieldsUsed is done using original group count
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (fieldsUsed.get(j)) {
        final ImmutableList<RexNode> args =
            relBuilder.fields(
                Mappings.apply2(inputMapping, aggCall.getArgList()));
        final RexNode filterArg = aggCall.filterArg < 0 ? null
            : relBuilder.field(Mappings.apply(inputMapping, aggCall.filterArg));
        RelBuilder.AggCall newAggCall =
                relBuilder.aggregateCall(aggCall.getAggregation(), args)
                        .distinct(aggCall.isDistinct())
                        .filter(filterArg)
                        .approximate(aggCall.isApproximate())
                        .sort(relBuilder.fields(aggCall.collation))
                        .as(aggCall.name);
        mapping.set(j, updatedGroupCount +  newAggCallList.size());
        newAggCallList.add(newAggCall);
      }
      ++j;
    }

    final RelBuilder.GroupKey groupKey =
        relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);

    return result(relBuilder.build(), mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  public TrimResult trimFields(Project project, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // set columnAccessInfo for ViewColumnAuthorization
    final ColumnAccessInfo columnAccessInfo = COLUMN_ACCESS_INFO.get();
    final Map<HiveProject, Table> viewProjectToTableSchema = VIEW_PROJECT_TO_TABLE_SCHEMA.get();
    if (columnAccessInfo != null && viewProjectToTableSchema != null
        && viewProjectToTableSchema.containsKey(project)) {
      for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
        if (fieldsUsed.get(ord.i)) {
          Table tab = viewProjectToTableSchema.get(project);
          columnAccessInfo.add(tab.getCompleteName(), tab.getAllCols().get(ord.i).getName());
        }
      }
    }
    return super.trimFields(project, fieldsUsed, extraFields);
  }

  public TrimResult trimFields(HiveTableScan tableAccessRel, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final TrimResult result = super.trimFields(tableAccessRel, fieldsUsed, extraFields);
    final ColumnAccessInfo columnAccessInfo = COLUMN_ACCESS_INFO.get();
    if (columnAccessInfo != null) {
      // Store information about column accessed by the table so it can be used
      // to send only this information for column masking
      final RelOptHiveTable tab = (RelOptHiveTable) tableAccessRel.getTable();
      final String qualifiedName = tab.getHiveTableMD().getCompleteName();
      final List<FieldSchema> allCols = tab.getHiveTableMD().getAllCols();
      final boolean insideView = tableAccessRel.isInsideView();
      fieldsUsed.asList().stream()
        .filter(idx -> idx < tab.getNoOfNonVirtualCols())
        .forEach(idx -> {
          if (insideView) {
            columnAccessInfo.addIndirect(qualifiedName, allCols.get(idx).getName());
          } else {
            columnAccessInfo.add(qualifiedName, allCols.get(idx).getName());
          }
        });
    }
    if (fetchStats) {
      fetchColStats(result.getKey(), tableAccessRel, fieldsUsed, extraFields);
    }
    return result;
  }

  private void fetchColStats(RelNode key, TableScan tableAccessRel, ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final List<Integer> iRefSet = Lists.newArrayList();
    if (key instanceof Project) {
      final Project project = (Project) key;
      for (RexNode rx : project.getProjects()) {
        iRefSet.addAll(HiveCalciteUtil.getInputRefs(rx));
      }
    } else {
      final int fieldCount = tableAccessRel.getRowType().getFieldCount();
      if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount)) && extraFields.isEmpty()) {
        // get all cols
        iRefSet.addAll(ImmutableBitSet.range(fieldCount).asList());
      }
    }

    //Remove any virtual cols
    if (tableAccessRel instanceof HiveTableScan) {
      iRefSet.removeAll(((HiveTableScan)tableAccessRel).getVirtualCols());
    }

    if (!iRefSet.isEmpty()) {
      final RelOptTable table = tableAccessRel.getTable();
      if (table instanceof RelOptHiveTable) {
        ((RelOptHiveTable) table).getColStat(iRefSet, true);
        LOG.debug("Got col stats for {} in {}", iRefSet,
            tableAccessRel.getTable().getQualifiedName());
      }
    }
  }

  protected TrimResult result(RelNode r, final Mapping mapping) {
    return new TrimResult(r, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for {@link HiveTableFunctionScan}.
   * Copied {@link org.apache.calcite.sql2rel.RelFieldTrimmer#trimFields(
   * org.apache.calcite.rel.logical.LogicalTableFunctionScan, ImmutableBitSet, Set)}
   * and replaced <code>tabFun</code> to {@link HiveTableFunctionScan}.
   * Proper fix would be implement this in calcite.
   */
  public TrimResult trimFields(
          HiveTableFunctionScan tabFun,
          ImmutableBitSet fieldsUsed,
          Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = tabFun.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final List<RelNode> newInputs = new ArrayList<>();

    for (RelNode input : tabFun.getInputs()) {
      final int inputFieldCount = input.getRowType().getFieldCount();
      ImmutableBitSet inputFieldsUsed = ImmutableBitSet.range(inputFieldCount);

      // Create input with trimmed columns.
      final Set<RelDataTypeField> inputExtraFields =
              Collections.emptySet();
      TrimResult trimResult =
              trimChildRestore(
                      tabFun, input, inputFieldsUsed, inputExtraFields);
      assert trimResult.right.isIdentity();
      newInputs.add(trimResult.left);
    }

    TableFunctionScan newTabFun = tabFun;
    if (!tabFun.getInputs().equals(newInputs)) {
      newTabFun = tabFun.copy(tabFun.getTraitSet(), newInputs,
              tabFun.getCall(), tabFun.getElementType(), tabFun.getRowType(),
              tabFun.getColumnMappings());
    }
    assert newTabFun.getClass() == tabFun.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return result(newTabFun, mapping);
  }

  /**
   * This method can be called to pre-register all the classes that may be
   * visited during the planning phase.
   */
  protected void register(List<Class<? extends RelNode>> nodeClasses) throws Throwable {
    this.trimFieldsDispatcher.register(nodeClasses);
  }

  /**
   * This method can be called at startup time to pre-register all the
   * Hive classes that may be visited during the planning phase.
   */
  public static void initializeFieldTrimmerClass(List<Class<? extends RelNode>> nodeClasses) {
    try {
      FIELD_TRIMMER_STATS.register(nodeClasses);
      FIELD_TRIMMER_NO_STATS.register(nodeClasses);
    } catch (Throwable t) {
      // LOG it but do not fail
      LOG.warn("Error initializing field trimmer instance", t);
    }
  }

  public TrimResult trimFields(
          HiveSortExchange exchange,
          ImmutableBitSet fieldsUsed,
          Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = exchange.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelCollation collation = exchange.getCollation();
    final RelDistribution distribution = exchange.getDistribution();
    final RelNode input = exchange.getInput();

    // We use the fields used by the consumer, plus any fields used as exchange
    // keys.
    final ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      inputFieldsUsed.set(field.getFieldIndex());
    }
    for (int keyIndex : distribution.getKeys()) {
      inputFieldsUsed.set(keyIndex);
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
            trimChild(exchange, input, inputFieldsUsed.build(), inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
            && inputMapping.isIdentity()
            && fieldsUsed.cardinality() == fieldCount) {
      return result(exchange, Mappings.createIdentity(fieldCount));
    }

    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInput);
    RelCollation newCollation = RexUtil.apply(inputMapping, collation);
    RelDistribution newDistribution = distribution.apply(inputMapping);
    relBuilder.sortExchange(newDistribution, newCollation);

    return result(relBuilder.build(), inputMapping);
  }
}
