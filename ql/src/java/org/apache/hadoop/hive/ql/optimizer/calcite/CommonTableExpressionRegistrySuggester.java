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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.CommonRelSubExprRegisterRule;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Suggester for join common table expressions that appear more than once in the query plan.
 */
public class CommonTableExpressionRegistrySuggester implements CommonTableExpressionSuggester {

  @Override
  public List<RelNode> suggest(final RelNode input, final Configuration configuration) {
    CommonTableExpressionRegistry localRegistry = new CommonTableExpressionRegistry();
    HepProgram ruleProgram = new HepProgramBuilder()
        .addRuleInstance(CommonRelSubExprRegisterRule.JOIN)
        .addRuleInstance(CommonRelSubExprRegisterRule.AGGREGATE)
        .addRuleInstance(CommonRelSubExprRegisterRule.FILTER)
        .addRuleInstance(CommonRelSubExprRegisterRule.PROJECT)
        .build();
    HepPlanner planner = new HepPlanner(ruleProgram, Contexts.of(localRegistry));
    planner.setRoot(input);
    planner.findBestExp();
    RelMetadataQuery mq = input.getCluster().getMetadataQuery();
    Comparator<RelNode> rowCountCmp = Comparator.comparing(mq::getRowCount).reversed();
    Comparator<RelNode> rowSizeCmp = Comparator.comparing(mq::getAverageRowSize).reversed();
    return localRegistry.entries()
        .sorted(Comparator.comparing(HiveCalciteUtil::countNodes).thenComparing(rowCountCmp).thenComparing(rowSizeCmp))
        .collect(Collectors.toList());
  }

}
