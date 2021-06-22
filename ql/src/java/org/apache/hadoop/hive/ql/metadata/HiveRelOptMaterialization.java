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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;

import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;

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

    public static final Predicate<EnumSet<RewriteAlgorithm>> ANY =
            rewriteAlgorithms -> true;
    public static final Predicate<EnumSet<RewriteAlgorithm>> NON_CALCITE =
            rewriteAlgorithms -> !rewriteAlgorithms.contains(HiveRelOptMaterialization.RewriteAlgorithm.CALCITE);
  }

  public enum IncrementalRebuildMode {
    AVAILABLE,
    INSERT_ONLY,
    NOT_AVAILABLE,
    UNKNOWN
  }

  private final EnumSet<RewriteAlgorithm> scope;
  private final boolean sourceTablesUpdateDeleteModified;
  private final boolean sourceTablesCompacted;
  private final IncrementalRebuildMode rebuildMode;

  public HiveRelOptMaterialization(RelNode tableRel, RelNode queryRel,
                                   RelOptTable starRelOptTable, List<String> qualifiedTableName,
                                   EnumSet<RewriteAlgorithm> scope, IncrementalRebuildMode rebuildMode) {
    this(tableRel, queryRel, starRelOptTable, qualifiedTableName, scope, false, false, rebuildMode);
  }

  private HiveRelOptMaterialization(RelNode tableRel, RelNode queryRel,
                                    RelOptTable starRelOptTable, List<String> qualifiedTableName,
                                    EnumSet<RewriteAlgorithm> scope,
                                    boolean sourceTablesUpdateDeleteModified, boolean sourceTablesCompacted, IncrementalRebuildMode rebuildMode) {
    super(tableRel, queryRel, starRelOptTable, qualifiedTableName);
    this.scope = scope;
    this.sourceTablesUpdateDeleteModified = sourceTablesUpdateDeleteModified;
    this.sourceTablesCompacted = sourceTablesCompacted;
    this.rebuildMode = rebuildMode;
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

  public boolean isSourceTablesUpdateDeleteModified() {
    return sourceTablesUpdateDeleteModified;
  }

  public boolean isSourceTablesCompacted() {
    return sourceTablesCompacted;
  }

  public IncrementalRebuildMode getRebuildMode() {
    return rebuildMode;
  }

  public HiveRelOptMaterialization updateInvalidation(Materialization materialization) {
    return new HiveRelOptMaterialization(tableRel, queryRel, starRelOptTable, qualifiedTableName, scope,
        materialization.isSourceTablesUpdateDeleteModified(), materialization.isSourceTablesCompacted(), rebuildMode);
  }

  public HiveRelOptMaterialization copyToNewCluster(RelOptCluster optCluster) {
    final RelNode newViewScan = HiveMaterializedViewUtils.copyNodeNewCluster(optCluster, tableRel);
    return new HiveRelOptMaterialization(newViewScan, queryRel, null, qualifiedTableName, scope,
        sourceTablesUpdateDeleteModified, sourceTablesCompacted, rebuildMode);
  }

}
