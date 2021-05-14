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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.EnumSet;
import java.util.List;

import static org.apache.commons.collections.CollectionUtils.intersection;

/**
 * Hive extension of {@link RelOptMaterialization}.
 */
public class HiveRelOptMaterialization extends RelOptMaterialization {

  /**
   * Enumeration of Materialized view query rewrite algorithms.
   */
  public enum RewriteAlgorithm {
    /**
     * Query sql text is compared to stored materialized view definition sql texts.
     */
    TEXT,
    /**
     * Use rewriting algorithm provided by Calcite.
     */
    CALCITE;

    public static final EnumSet<RewriteAlgorithm> ALL = EnumSet.allOf(RewriteAlgorithm.class);

  }
  private final EnumSet<RewriteAlgorithm> scope;

  public HiveRelOptMaterialization(RelNode tableRel, RelNode queryRel,
                                   RelOptTable starRelOptTable, List<String> qualifiedTableName,
                                   EnumSet<RewriteAlgorithm> scope) {
    super(tableRel, queryRel, starRelOptTable, qualifiedTableName);
    this.scope = scope;
  }

  public EnumSet<RewriteAlgorithm> getScope() {
    return scope;
  }

  /**
   * Is this materialized view applicable to the specified scope.
   * @param scope Set of algorithms
   * @return true if applicable false otherwise
   */
  public boolean isSupported(EnumSet<RewriteAlgorithm> scope) {
    return !intersection(this.scope, scope).isEmpty();
  }
}
