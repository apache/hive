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
import org.apache.hadoop.hive.ql.parse.ASTNode;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.collections.CollectionUtils.intersection;

/**
 * Hive extension of {@link RelOptMaterialization}.
 */
public class HiveRelOptMaterialization extends RelOptMaterialization {

  public enum IncrementalRebuildMode {
    AVAILABLE,
    INSERT_ONLY,
    NOT_AVAILABLE,
    UNKNOWN
  }

  private final Set<RewriteAlgorithm> scope;
  private final boolean sourceTablesUpdateDeleteModified;
  private final boolean sourceTablesCompacted;
  private final IncrementalRebuildMode rebuildMode;
  private final ASTNode ast;

  public HiveRelOptMaterialization(RelNode tableRel, RelNode queryRel,
                                   RelOptTable starRelOptTable, List<String> qualifiedTableName,
                                   Set<RewriteAlgorithm> scope, IncrementalRebuildMode rebuildMode, ASTNode ast) {
    this(tableRel, queryRel, starRelOptTable, qualifiedTableName, scope, false, false, rebuildMode, ast);
  }

  private HiveRelOptMaterialization(RelNode tableRel, RelNode queryRel,
                                    RelOptTable starRelOptTable, List<String> qualifiedTableName,
                                    Set<RewriteAlgorithm> scope,
                                    boolean sourceTablesUpdateDeleteModified, boolean sourceTablesCompacted, IncrementalRebuildMode rebuildMode, ASTNode ast) {
    super(tableRel, queryRel, starRelOptTable, qualifiedTableName);
    this.scope = scope;
    this.sourceTablesUpdateDeleteModified = sourceTablesUpdateDeleteModified;
    this.sourceTablesCompacted = sourceTablesCompacted;
    this.rebuildMode = rebuildMode;
    this.ast = ast;
  }

  public Set<RewriteAlgorithm> getScope() {
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

  public ASTNode getAst() {
    return ast;
  }

  public HiveRelOptMaterialization updateInvalidation(Materialization materialization) {
    return new HiveRelOptMaterialization(tableRel, queryRel, starRelOptTable, qualifiedTableName, scope,
        materialization.isSourceTablesUpdateDeleteModified(), materialization.isSourceTablesCompacted(), rebuildMode, ast);
  }

  public HiveRelOptMaterialization copyToNewCluster(RelOptCluster optCluster) {
    final RelNode newViewScan = HiveMaterializedViewUtils.copyNodeNewCluster(optCluster, tableRel);
    return new HiveRelOptMaterialization(newViewScan, queryRel, null, qualifiedTableName, scope,
        sourceTablesUpdateDeleteModified, sourceTablesCompacted, rebuildMode, ast);
  }

}
