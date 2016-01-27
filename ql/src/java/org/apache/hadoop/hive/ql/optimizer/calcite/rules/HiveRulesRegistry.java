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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class HiveRulesRegistry {

  private SetMultimap<RelOptRule, RelNode> registryVisited;
  private ListMultimap<RelNode,Set<String>> registryPushedPredicates;

  public HiveRulesRegistry() {
    this.registryVisited = HashMultimap.create();
    this.registryPushedPredicates = ArrayListMultimap.create();
  }

  public void registerVisited(RelOptRule rule, RelNode operator) {
    this.registryVisited.put(rule, operator);
  }

  public Set<RelNode> getVisited(RelOptRule rule) {
    return this.registryVisited.get(rule);
  }

  public Set<String> getPushedPredicates(RelNode operator, int pos) {
    if (!this.registryPushedPredicates.containsKey(operator)) {
      for (int i = 0; i < operator.getInputs().size(); i++) {
        this.registryPushedPredicates.get(operator).add(Sets.<String>newHashSet());
      }
    }
    return this.registryPushedPredicates.get(operator).get(pos);
  }

  public void copyPushedPredicates(RelNode operator, RelNode otherOperator) {
    if (this.registryPushedPredicates.containsKey(operator)) {
      for (Set<String> s : this.registryPushedPredicates.get(operator)) {
        this.registryPushedPredicates.put(otherOperator, Sets.newHashSet(s));
      }
    }
  }
}
