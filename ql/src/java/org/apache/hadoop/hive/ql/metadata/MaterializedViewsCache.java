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

import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Collection for storing {@link HiveRelOptMaterialization}s.
 * Materialization can be lookup by
 * - the Materialized View fully qualified name
 * - query text.
 * This implementation contains two {@link ConcurrentHashMap} one for name based and one for query text based lookup.
 * The map contents are synchronized during each dml operation: Dml operations are performed initially on the map
 * which provides name based lookup. The map which provides query text based lookup is updated by lambda expressions
 * passed to {@link ConcurrentHashMap#compute(Object, BiFunction)}.
 */
public class MaterializedViewsCache {
  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewsCache.class);

  // Key is the database name. Value a map from the qualified name to the view object.
  private final ConcurrentMap<String, ConcurrentMap<String, HiveRelOptMaterialization>> materializedViews =
          new ConcurrentHashMap<>();
  // Map for looking up materialization by view query text
  private final Map<String, List<HiveRelOptMaterialization>> sqlToMaterializedView = new ConcurrentHashMap<>();


  public void putIfAbsent(Table materializedViewTable, HiveRelOptMaterialization materialization) {
    ConcurrentMap<String, HiveRelOptMaterialization> dbMap = ensureDbMap(materializedViewTable);

    // You store the materialized view
    dbMap.compute(materializedViewTable.getTableName(), (mvTableName, aMaterialization) -> {
      List<HiveRelOptMaterialization> materializationList = sqlToMaterializedView.computeIfAbsent(
              materializedViewTable.getViewExpandedText(), s -> new ArrayList<>());
      materializationList.add(materialization);
      return materialization;
    });

    LOG.debug("Materialized view {}.{} added to registry",
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  private ConcurrentMap<String, HiveRelOptMaterialization> ensureDbMap(Table materializedViewTable) {
    // We are going to create the map for each view in the given database
    ConcurrentMap<String, HiveRelOptMaterialization> dbMap =
            new ConcurrentHashMap<String, HiveRelOptMaterialization>();
    // If we are caching the MV, we include it in the cache
    final ConcurrentMap<String, HiveRelOptMaterialization> prevDbMap = materializedViews.putIfAbsent(
            materializedViewTable.getDbName(), dbMap);
    if (prevDbMap != null) {
      dbMap = prevDbMap;
    }
    return dbMap;
  }

  public void refresh(
          Table oldMaterializedViewTable, Table materializedViewTable, HiveRelOptMaterialization newMaterialization) {
    ConcurrentMap<String, HiveRelOptMaterialization> dbMap = ensureDbMap(materializedViewTable);

    dbMap.compute(materializedViewTable.getTableName(), (mvTableName, existingMaterialization) -> {
      List<HiveRelOptMaterialization> optMaterializationList = sqlToMaterializedView.computeIfAbsent(
          materializedViewTable.getViewExpandedText(), s -> new ArrayList<>());

      if (existingMaterialization == null) {
        // If it was not existing, we just create it
        optMaterializationList.add(newMaterialization);
        return newMaterialization;
      }
      Table existingMaterializedViewTable = HiveMaterializedViewUtils.extractTable(existingMaterialization);
      if (existingMaterializedViewTable.equals(oldMaterializedViewTable)) {
        // If the old version is the same, we replace it
        optMaterializationList.remove(existingMaterialization);
        optMaterializationList.add(newMaterialization);
        return newMaterialization;
      }
      // Otherwise, we return existing materialization
      return existingMaterialization;
    });

    if (LOG.isDebugEnabled()) {
      String oldViewName;
      if (oldMaterializedViewTable == null) {
        oldViewName = "";
      } else {
        oldViewName = String.format("%s.%s -> ",
            oldMaterializedViewTable.getDbName(), oldMaterializedViewTable.getTableName());
      }
      LOG.debug("Refreshed materialized view {}{}.{}",
          oldViewName,
          materializedViewTable.getDbName(), materializedViewTable.getTableName());
    }
  }

  public void remove(Table materializedViewTable) {
    ConcurrentMap<String, HiveRelOptMaterialization> dbMap = materializedViews.get(materializedViewTable.getDbName());
    if (dbMap != null) {
      // Delete only if the create time for the input materialized view table and the table
      // in the map match. Otherwise, keep the one in the map.
      dbMap.computeIfPresent(materializedViewTable.getTableName(), (mvTableName, oldMaterialization) -> {
        if (HiveMaterializedViewUtils.extractTable(oldMaterialization).equals(materializedViewTable)) {
          List<HiveRelOptMaterialization> materializationList =
                  sqlToMaterializedView.get(materializedViewTable.getViewExpandedText());
          materializationList.remove(oldMaterialization);
          return null;
        }
        return oldMaterialization;
      });
    }

    LOG.debug("Materialized view {}.{} removed from registry",
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  public void remove(String dbName, String tableName) {
    ConcurrentMap<String, HiveRelOptMaterialization> dbMap = materializedViews.get(dbName);
    if (dbMap != null) {
      dbMap.computeIfPresent(tableName, (mvTableName, materialization) -> {
        String queryText = HiveMaterializedViewUtils.extractTable(materialization).getViewExpandedText();
        List<HiveRelOptMaterialization> materializationList = sqlToMaterializedView.get(queryText);
        materializationList.remove(materialization);
        return null;
      });

      LOG.debug("Materialized view {}.{} removed from registry", dbName, tableName);
    }
  }

  public List<HiveRelOptMaterialization> values() {
    List<HiveRelOptMaterialization> result = new ArrayList<>();
    materializedViews.forEach((dbName, mvs) -> result.addAll(mvs.values()));
    return unmodifiableList(result);
  }

  HiveRelOptMaterialization get(String dbName, String viewName) {
    if (materializedViews.get(dbName) != null) {
      LOG.debug("Found materialized view {}.{} in registry", dbName, viewName);
      return materializedViews.get(dbName).get(viewName);
    }
    LOG.debug("Materialized view {}.{} not found in registry", dbName, viewName);
    return null;
  }

  public List<HiveRelOptMaterialization> get(String querySql) {
    List<HiveRelOptMaterialization> relOptMaterializationList = sqlToMaterializedView.get(querySql);
    if (relOptMaterializationList == null) {
      LOG.trace("No materialized view with query text '{}' found in registry.", querySql);
      LOG.debug("No materialized view with similar query text found in registry.");
      return emptyList();
    }
    LOG.trace("{} materialized view(s) found with query text '{}' in registry",
            relOptMaterializationList.size(), querySql);
    LOG.debug("{} materialized view(s) found with similar query text found in registry",
            relOptMaterializationList.size());
    return unmodifiableList(relOptMaterializationList);
  }
}
