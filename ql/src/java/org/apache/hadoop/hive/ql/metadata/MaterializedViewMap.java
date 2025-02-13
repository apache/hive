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
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public class MaterializedViewMap {
  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewMap.class);

  // Key is the database name. Value a map from the qualified name to the view object.
  private final ConcurrentMap<String, HiveRelOptMaterialization> materializedViews =
          new ConcurrentHashMap<>();
  // Map for looking up materialization by view query text
  private final Map<ASTKey, List<HiveRelOptMaterialization>> sqlToMaterializedView = new ConcurrentHashMap<>();


  public void putIfAbsent(Table materializedViewTable, HiveRelOptMaterialization materialization) {
    // You store the materialized view
    materializedViews.computeIfAbsent(genKey(materializedViewTable), mvTableName -> {
      List<HiveRelOptMaterialization> materializationList = sqlToMaterializedView.computeIfAbsent(
              new ASTKey(materialization.getAst()), s -> new ArrayList<>());
      materializationList.add(materialization);
      return materialization;
    });

    LOG.debug("Materialized view {}.{} added to registry",
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  private String genKey(Table materializedViewTable) {
    return genKey(materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  private String genKey(String dbName, String viewName) {
    return dbName + viewName;
  }

  void refresh(
          Table oldMaterializedViewTable, Table materializedViewTable, HiveRelOptMaterialization newMaterialization) {

    materializedViews.compute(genKey(materializedViewTable), (mvTableName, existingMaterialization) -> {
      List<HiveRelOptMaterialization> optMaterializationList = sqlToMaterializedView.computeIfAbsent(
          new ASTKey(newMaterialization.getAst()), s -> new ArrayList<>());

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
    // Delete only if the create time for the input materialized view table and the table
    // in the map match. Otherwise, keep the one in the map.
    materializedViews.computeIfPresent(genKey(materializedViewTable), (mvTableName, oldMaterialization) -> {
      Table oldTable = HiveMaterializedViewUtils.extractTable(oldMaterialization);
      if (materializedViewTable.equals(oldTable)) {
        remove(oldMaterialization, oldTable);
        return null;
      }
      return oldMaterialization;
    });

    LOG.debug("Materialized view {}.{} removed from registry",
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  private void remove(HiveRelOptMaterialization materialization, Table mvTable) {
    if (mvTable == null) {
      return;
    }

    ASTKey astKey = new ASTKey(materialization.getAst());
    List<HiveRelOptMaterialization> materializationList = sqlToMaterializedView.get(astKey);
    if (materializationList == null) {
      return;
    }

    materializationList.remove(materialization);
    if (materializationList.isEmpty()) {
      sqlToMaterializedView.remove(astKey);
    }
  }

  public void remove(String dbName, String tableName) {
    materializedViews.computeIfPresent(genKey(dbName, tableName), (mvTableName, materialization) -> {
      remove(materialization, HiveMaterializedViewUtils.extractTable(materialization));
      return null;
    });

    LOG.debug("Materialized view {}.{} removed from registry", dbName, tableName);
  }

  public Collection<HiveRelOptMaterialization> values() {
    return materializedViews.values();
  }

  HiveRelOptMaterialization get(String dbName, String viewName) {
    return materializedViews.get(genKey(dbName, viewName));
  }

  public Collection<HiveRelOptMaterialization> get(ASTNode astNode) {
    List<HiveRelOptMaterialization> relOptMaterializationList = sqlToMaterializedView.get(new ASTKey(astNode));
    if (relOptMaterializationList == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No materialized view with query text '{}' found in registry.", astNode.dump());
      }
      LOG.debug("No materialized view with similar query text found in registry.");
      return emptyList();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} materialized view(s) found with query text '{}' in registry",
              relOptMaterializationList.size(), astNode.dump());
    }
    LOG.debug("{} materialized view(s) found with similar query text found in registry",
            relOptMaterializationList.size());
    return unmodifiableList(relOptMaterializationList);
  }

  public boolean isEmpty() {
    return materializedViews.isEmpty();
  }


  private static class ASTKey {
    private final ASTNode root;

    public ASTKey(ASTNode root) {
      this.root = root;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ASTKey that = (ASTKey) o;
      return equals(root, that.root);
    }

    private boolean equals(ASTNode astNode1, ASTNode astNode2) {
      if (!(astNode1.getType() == astNode2.getType() &&
              astNode1.getText().equals(astNode2.getText()) &&
              astNode1.getChildCount() == astNode2.getChildCount())) {
        return false;
      }

      for (int i = 0; i < astNode1.getChildCount(); ++i) {
        if (!equals((ASTNode) astNode1.getChild(i), (ASTNode) astNode2.getChild(i))) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int hashCode() {
      return hashcodeOf(root);
    }

    private int hashcodeOf(ASTNode node) {
      int result = Objects.hash(node.getType(), node.getText());

      for (int i = 0; i < node.getChildCount(); ++i) {
        result = 31 * result + hashcodeOf((ASTNode) node.getChild(i));
      }

      return result;
    }
  }
}
