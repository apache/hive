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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;


/**
 * Relational expression representing a scan of a HiveDB collection.
 *
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate"
 * methods.
 * </p>
 */
public class HiveTableScan extends TableScan implements HiveRelNode {

  private final RelDataType hiveTableScanRowType;
  private final ImmutableList<Integer> neededColIndxsFrmReloptHT;
  private final String tblAlias;
  private final String concatQbIDAlias;
  private final boolean useQBIdInDigest;

  public String getTableAlias() {
    return tblAlias;
  }

  public String getConcatQbIDAlias() {
    return concatQbIDAlias;
  }

  /**
   * Creates a HiveTableScan.
   *
   * @param cluster
   *          Cluster
   * @param traitSet
   *          Traits
   * @param table
   *          Table
   * @param table
   *          HiveDB table
   */
  public HiveTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptHiveTable table,
      String alias, String concatQbIDAlias, boolean useQBIdInDigest) {
    this(cluster, traitSet, table, alias, concatQbIDAlias, table.getRowType(), useQBIdInDigest);
  }

  private HiveTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptHiveTable table,
      String alias, String concatQbIDAlias, RelDataType newRowtype, boolean useQBIdInDigest) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), table);
    assert getConvention() == HiveRelNode.CONVENTION;
    this.tblAlias = alias;
    this.concatQbIDAlias = concatQbIDAlias;
    this.hiveTableScanRowType = newRowtype;
    this.neededColIndxsFrmReloptHT = buildNeededColIndxsFrmReloptHT(table.getRowType(), newRowtype);
    this.useQBIdInDigest = useQBIdInDigest;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  /**
   * Copy TableScan operator with a new Row Schema. The new Row Schema can only
   * be a subset of this TS schema.
   *
   * @param newRowtype
   * @return
   */
  public HiveTableScan copy(RelDataType newRowtype) {
    return new HiveTableScan(getCluster(), getTraitSet(), ((RelOptHiveTable) table), this.tblAlias, this.concatQbIDAlias,
            newRowtype, this.useQBIdInDigest);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return RelMetadataQuery.getNonCumulativeCost(this);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    if (this.useQBIdInDigest) {
      // TODO: Only the qualified name should be left here
      return super.explainTerms(pw)
          .item("qbid:alias", concatQbIDAlias);
    } else {
      return super.explainTerms(pw);
    }
  }

  @Override
  public void register(RelOptPlanner planner) {

  }

  @Override
  public void implement(Implementor implementor) {

  }

  @Override
  public double getRows() {
    return ((RelOptHiveTable) table).getRowCount();
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    return ((RelOptHiveTable) table).getColStat(projIndxLst);
  }

  @Override
  public RelNode project(ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields,
      RelFactories.ProjectFactory projectFactory) {

    // 1. If the schema is the same then bail out
    final int fieldCount = getRowType().getFieldCount();
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount)) && extraFields.isEmpty()) {
      return this;
    }

    // 2. Make sure there is no dynamic addition of virtual cols
    if (extraFields != null && !extraFields.isEmpty()) {
      throw new RuntimeException("Hive TS does not support adding virtual columns dynamically");
    }

    // 3. Create new TS schema that is a subset of original
    final List<RelDataTypeField> fields = getRowType().getFieldList();
    List<RelDataType> fieldTypes = new LinkedList<RelDataType>();
    List<String> fieldNames = new LinkedList<String>();
    List<RexNode> exprList = new ArrayList<RexNode>();
    RexBuilder rexBuilder = getCluster().getRexBuilder();
    for (int i : fieldsUsed) {
      RelDataTypeField field = fields.get(i);
      fieldTypes.add(field.getType());
      fieldNames.add(field.getName());
      exprList.add(rexBuilder.makeInputRef(this, i));
    }

    // 4. Build new TS
    HiveTableScan newHT = copy(getCluster().getTypeFactory().createStructType(fieldTypes,
        fieldNames));

    // 5. Add Proj on top of TS
    return projectFactory.createProject(newHT, exprList, new ArrayList<String>(fieldNames));
  }

  public List<Integer> getNeededColIndxsFrmReloptHT() {
    return neededColIndxsFrmReloptHT;
  }

  public RelDataType getPrunedRowType() {
    return hiveTableScanRowType;
  }

  private static ImmutableList<Integer> buildNeededColIndxsFrmReloptHT(RelDataType htRowtype,
      RelDataType scanRowType) {
    Builder<Integer> neededColIndxsFrmReloptHTBldr = new ImmutableList.Builder<Integer>();
    Map<String, Integer> colNameToPosInReloptHT = HiveCalciteUtil.getRowColNameIndxMap(htRowtype
        .getFieldList());
    List<String> colNamesInScanRowType = scanRowType.getFieldNames();

    for (int i = 0; i < colNamesInScanRowType.size(); i++) {
      neededColIndxsFrmReloptHTBldr.add(colNameToPosInReloptHT.get(colNamesInScanRowType.get(i)));
    }

    return neededColIndxsFrmReloptHTBldr.build();
  }
}
