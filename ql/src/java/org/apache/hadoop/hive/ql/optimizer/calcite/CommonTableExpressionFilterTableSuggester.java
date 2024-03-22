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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonTableExpressionFilterTableSuggester implements CommonTableExpressionSuggester {
  @Override
  public List<RelNode> suggest(final RelNode input, final Configuration configuration) {
    HepProgram p = new HepProgramBuilder().addRuleInstance(new FilterTableScanExtractorRule()).build();
    ScanPredicates registry = new ScanPredicates();
    HepPlanner hep = new HepPlanner(p, Contexts.of(registry));
    hep.setRoot(input);
    hep.findBestExp();
    List<RelNode> ctes = new ArrayList<>();
    for (Map.Entry<RelOptTable, List<RexNode>> e : registry.tableToPredicates.entrySet()) {
      RelBuilder b = HiveRelFactories.HIVE_BUILDER.create(input.getCluster(), null);
      RelOptHiveTable table = (RelOptHiveTable) e.getKey();
      b.push(new HiveTableScan(input.getCluster(), TraitsUtil.getDefaultTraitSet(input.getCluster()), table,
          (table).getName(), null, false, false));
      b.filter(b.or(e.getValue()));
      ctes.add(b.build());
    }
    return ctes;
  }

  private static final class FilterTableScanExtractorRule extends RelOptRule {

    public FilterTableScanExtractorRule() {
      super(operand(Filter.class, operand(TableScan.class, none())));
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      Filter f = call.rel(0);
      TableScan s = call.rel(1);
      ScanPredicates predicates = call.getPlanner().getContext().unwrap(ScanPredicates.class);
      if (predicates != null) {
        List<RexNode> preds = predicates.tableToPredicates.computeIfAbsent(s.getTable(), t -> new ArrayList<>());
        preds.add(f.getCondition());
      }

    }
  }

  private static final class ScanPredicates {
    private final Map<RelOptTable, List<RexNode>> tableToPredicates = new HashMap<>();
  }
}
