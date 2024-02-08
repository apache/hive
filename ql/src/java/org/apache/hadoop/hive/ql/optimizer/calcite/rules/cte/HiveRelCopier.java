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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.cte;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePartitionPruneRuleHelper;
import org.apache.hadoop.hive.ql.parse.QueryTables;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a full copy of the RelNode using the specified cluster.
 */
class HiveRelCopier extends RelHomogeneousShuttle {
  private final RelOptCluster targetCluster;

  HiveRelCopier(RelOptCluster cluster) {
    this.targetCluster = cluster;
  }

  RelOptMaterialization copy(RelOptMaterialization m) {
    return new RelOptMaterialization(m.tableRel.accept(this), m.queryRel.accept(this), m.starRelOptTable,
        m.qualifiedTableName);
  }

  @Override public RelNode visit(RelNode other) {
    other = super.visit(other);
    RelTraitSet traitSet = other.getTraitSet().replace(HiveRelNode.CONVENTION);
    if (other instanceof Aggregate) {
      Aggregate agg = (Aggregate) other;
      return new HiveAggregate(targetCluster, traitSet, agg.getInput(), agg.getGroupSet(), agg.getGroupSets(),
          agg.getAggCallList());
    } else if (other instanceof Filter) {
      Filter fil = (Filter) other;
      return new HiveFilter(targetCluster, traitSet, fil.getInput(), fil.getCondition());
    } else if (other instanceof Project) {
      Project pro = (Project) other;
      return new HiveProject(targetCluster, traitSet, pro.getInput(), pro.getProjects(), pro.getRowType(),
          pro.getFlags());
    } else if (other instanceof Join) {
      Join j = (Join) other;
      return HiveJoin.getJoin(targetCluster, j.getLeft(), j.getRight(), j.getCondition(), j.getJoinType());
    } else if (other instanceof Sort) {
      Sort s = (Sort) other;
      return new HiveSortLimit(targetCluster, traitSet, s.getInput(), s.getCollation(), s.offset, s.fetch);
    } else if (other instanceof TableFunctionScan) {
      TableFunctionScan tfs = (TableFunctionScan) other;
      try {
        return HiveTableFunctionScan.create(targetCluster, traitSet, tfs.getInputs(), tfs.getCall(),
            tfs.getElementType(), tfs.getRowType(), tfs.getColumnMappings());
      } catch (CalciteSemanticException e) {
        throw new RuntimeException(e);
      }
    } else if (other instanceof HiveTableScan) {
      HiveTableScan scan = (HiveTableScan) other;
      return new HiveTableScan(targetCluster, traitSet, (RelOptHiveTable) scan.getTable(), scan.getTableAlias(),
          scan.getConcatQbIDAlias(), false, scan.isInsideView());
    } else if (other instanceof TableScan) {
      // The advisor code will create DasTableScan or something similar it is not easy to create a Hive scan unless
      // we hack it badly. Obviously if here we create a LogicalTableScan it has to be something temporary cause
      // there is no way to execute something with it.
      // The traitset though needs to contain the HiveConvention otherwise the plan may be considered non-implementatable
      // and thus have infinite cost.
      TableScan scan = (TableScan) other;
      RelOptTable originalTbl = scan.getTable();
      RelOptHiveTable copyTbl =
          tmpOptTable(originalTbl.getQualifiedName(), scan.getRowType(), targetCluster.getTypeFactory(),
              originalTbl.getRowCount());
      return new HiveTableScan(targetCluster, traitSet, copyTbl, originalTbl.getQualifiedName().get(0), null, false,
          false);
    }
    return other.copy(traitSet, other.getInputs());
  }

  private static RelOptHiveTable tmpOptTable(List<String> qname, RelDataType type, RelDataTypeFactory typeFactory,
      double rowCount) {
    Table tblMetadata = new Table("dummy", qname.get(0));
    // The TableType.CTE is a hack to make the cost model read the size of the table instead of returning infinite
    // for the materialized view. Obviously this has to change to use the metadata providers.
    RelOptHiveTable tbl = new RelOptHiveTable(null, typeFactory, qname, type, tblMetadata, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), new HiveConf(), Hive.getThreadLocal(), new QueryTables(true),
        Collections.emptyMap(), Collections.emptyMap(), new AtomicInteger(), RelOptHiveTable.TableType.CTE,
        new HivePartitionPruneRuleHelper());
    tbl.setRowCount(rowCount);
    return tbl;
  }
}
