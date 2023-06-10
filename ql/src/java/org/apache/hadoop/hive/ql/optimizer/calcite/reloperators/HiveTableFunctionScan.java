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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;

public class HiveTableFunctionScan extends TableFunctionScan implements HiveRelNode {

  // True if this RelNode was created through a lateral view. This is needed because
  // when the return type is different based on whether the node was created as a lateral
  // view. For lateral views, the fields from the base source table are accessible along
  // with the fields coming from the udtf that expand the output rows into multiple rows.
  // When this RelNode is created without a lateral view, only the udtf output is present
  // in the return type.
  private final boolean isLateralView;

  /**
   * @param cluster
   *          cluster - Cluster that this relational expression belongs to
   * @param traitSet
   * @param inputs
   *          inputs - 0 or more relational inputs
   * @param rexCall
   *          rexCall - Function invocation expression
   * @param elementType
   *          elementType - Element type of the collection that will implement
   *          this table
   * @param rowType
   *          rowType - Row type produced by function
   * @param columnMappings
   *          columnMappings - Column mappings associated with this function
   */
  private HiveTableFunctionScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs,
      RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings,
      boolean isLateralView) {
    super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
    this.isLateralView = isLateralView;
  }

  public static HiveTableFunctionScan create(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings, boolean isLateralView) throws CalciteSemanticException {
    return new HiveTableFunctionScan(cluster, traitSet,
        inputs, rexCall, elementType, rowType, columnMappings, isLateralView);
  }

  public static HiveTableFunctionScan create(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType,
      Set<RelColumnMapping> columnMappings) throws CalciteSemanticException {
    return new HiveTableFunctionScan(cluster, traitSet,
        inputs, rexCall, elementType, rowType, columnMappings, false);
  }

  @Override
  public TableFunctionScan copy(RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall,
      Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings) {
    return new HiveTableFunctionScan(getCluster(), traitSet, inputs, rexCall,
        elementType, rowType, columnMappings, isLateralView);
  }

  // return the field in the return type where the udtf field starts. If it is not
  // a lateral view, the first field in the return type is the udtf return field.
  // Otherwise, we can subtract from the end the amount of udtf fields and the rest
  // are from the input ref.
  public int getStartUdtfField() {
    return isLateralView()
        ? getRowType().getFieldCount() - getCall().getType().getFieldCount() : 0;
  }

  public boolean isLateralView() {
    return isLateralView;
  }
}
