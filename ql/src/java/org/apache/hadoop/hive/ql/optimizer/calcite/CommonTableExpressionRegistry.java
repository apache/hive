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

import org.apache.calcite.rel.RelNode;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

public final class CommonTableExpressionRegistry implements Iterable<RelNode> {
  private final Set<RelNode> ctes = new HashSet<>();

  public void add(RelNode cte) {
    this.ctes.add(cte);
  }

  @NotNull
  @Override
  public Iterator<RelNode> iterator() {
    // When is it safe to modify the plan at this stage (in the middle of optimizations?) If we start removing vertices while the HepPlanner is still running the we can possibly ruin the internal stage of the planner. We should find an alternative way to expose this
    return ctes.stream().map(HiveCalciteUtil::stripHepVertices).collect(Collectors.toList()).iterator();
  }
}
