/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A registry of common table expressions for a given query.
 * <p>The registry is only meant to hold common expressions for a single query.</p>
 * <p>The class is not thread-safe.</p>
 */
@InterfaceAudience.Private
public final class CommonTableExpressionRegistry {
  /**
   * A unique collection of common table expressions.
   * <p>The expressions may contain internal planning concepts such as {@link org.apache.calcite.plan.hep.HepRelVertex}.
   * </p>
   */
  private final Map<String, RelNode> ctes = new HashMap<>();

  /**
   * Adds the specified common table expression to this registry.
   * @param cte common table expression to be added to the registry.
   */
  public void add(RelNode cte) {
    this.ctes.put(cte.getDigest(), cte);
  }

  /**
   * @return a stream with all common table expression entries
   */
  public Stream<RelNode> entries() {
    return ctes.values().stream().map(HiveCalciteUtil::stripHepVertices);
  }
}
