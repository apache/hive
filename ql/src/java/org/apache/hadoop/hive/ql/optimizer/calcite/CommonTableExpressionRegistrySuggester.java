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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.QueryTables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

/**
 * Suggester for join common table expressions that appear more than once in the query plan.
 */
public class CommonTableExpressionRegistrySuggester implements CommonTableExpressionSuggester {
  private int cteId = 0;

  @Override
  public List<RelOptMaterialization> suggest(final RelNode input, final Configuration configuration) {
    CommonTableExpressionRegistry r =
        input.getCluster().getPlanner().getContext().unwrap(CommonTableExpressionRegistry.class);
    if (r != null) {
      Optional<RelNode> bestCte =
          StreamSupport.stream(r.spliterator(), false).max(Comparator.comparing(HiveCalciteUtil::countNodes));
      return bestCte.map(this::wrap).map(Collections::singletonList).orElse(Collections.emptyList());
    } else {
      return Collections.emptyList();
    }
  }

  private RelOptMaterialization wrap(RelNode input) {
    RelOptCluster cluster = input.getCluster();
    List<ColumnInfo> columns = new ArrayList<>();
    String cteTableName = "cte_table_suggest_" + (cteId++);
    for (RelDataTypeField f : input.getRowType().getFieldList()) {
      columns.add(
          new ColumnInfo(f.getName(), TypeConverter.convert(f.getType()), f.getType().isNullable(), cteTableName, false,
              false));
    }
    List<String> tableName = Arrays.asList("cte", cteTableName);
    RelOptHiveTable optTable = new RelOptHiveTable(null, cluster.getTypeFactory(), tableName, input.getRowType(),
        new Table("cte", cteTableName), columns, Collections.emptyList(), Collections.emptyList(), new HiveConf(),
        Hive.getThreadLocal(), new QueryTables(true), new HashMap<>(), new HashMap<>(), new AtomicInteger(),
        RelOptHiveTable.Type.CTE);
    optTable.setRowCount(cluster.getMetadataQuery().getRowCount(input));
    final TableScan scan =
        new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable, cteTableName, null, false,
            false);

    return new RelOptMaterialization(scan, input, null, tableName);
  }
}
