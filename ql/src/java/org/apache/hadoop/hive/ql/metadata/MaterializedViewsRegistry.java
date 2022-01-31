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

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.EnumSet;
import java.util.List;

public interface MaterializedViewsRegistry {

  /**
   * Adds a newly created materialized view to the registry.
   */
  void createMaterializedView(HiveConf conf, Table materializedViewTable);

  /**
   * Update the materialized view in the registry.
   */
  void refreshMaterializedView(HiveConf conf, Table materializedViewTable);

  /**
   * Update the materialized view in the registry (if existing materialized view matches).
   */
  void refreshMaterializedView(HiveConf conf, Table oldMaterializedViewTable, Table materializedViewTable);

  /**
   * Removes the materialized view from the registry (based on table object equality), if exists.
   */
  void dropMaterializedView(Table materializedViewTable);

  /**
   * Removes the materialized view from the cache (based on qualified name), if exists.
   */
  void dropMaterializedView(String dbName, String tableName);

  /**
   * Returns all the materialized views enabled for Calcite based rewriting in the registry.
   *
   * @return the collection of materialized views, or the empty collection if none
   */
  List<HiveRelOptMaterialization> getRewritingMaterializedViews();

  /**
   * Returns the materialized views in the registry for the given database.
   *
   * @return the collection of materialized views, or the empty collection if none
   */
  HiveRelOptMaterialization getRewritingMaterializedView(
          String dbName, String viewName, EnumSet<HiveRelOptMaterialization.RewriteAlgorithm> scope);

  /**
   * Returns the materialized views in the registry for the given mv definition sql query text.
   */
  List<HiveRelOptMaterialization> getRewritingMaterializedViews(String querySql);

  /**
   * The registry is empty or not.
   * @return true if empty, false otherwise
   */
  boolean isEmpty();
}
