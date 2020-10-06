package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MaterializedViews {
  /* Key is the database name. Value a map from the qualified name to the view object. */
  private final ConcurrentMap<String, ConcurrentMap<String, RelOptMaterialization>> materializedViews =
          new ConcurrentHashMap<>();
  // Map for looking up materialization by view query text
  private final Map<String, RelOptMaterialization> sqlToMaterializedView = new ConcurrentHashMap<>();


  public void putIfAbsent(Table materializedViewTable, RelOptMaterialization materialization) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = ensureDbMap(materializedViewTable);

    // You store the materialized view
    dbMap.compute(materializedViewTable.getTableName(), (mvTableName, relOptMaterialization) -> {
      sqlToMaterializedView.put(materializedViewTable.getViewOriginalText(), materialization);
      return materialization;
    });
  }

  private ConcurrentMap<String, RelOptMaterialization> ensureDbMap(Table materializedViewTable) {
    // We are going to create the map for each view in the given database
    ConcurrentMap<String, RelOptMaterialization> dbMap =
            new ConcurrentHashMap<String, RelOptMaterialization>();
    // If we are caching the MV, we include it in the cache
    final ConcurrentMap<String, RelOptMaterialization> prevDbMap = materializedViews.putIfAbsent(
            materializedViewTable.getDbName(), dbMap);
    if (prevDbMap != null) {
      dbMap = prevDbMap;
    }
    return dbMap;
  }

  public void refresh(
          Table oldMaterializedViewTable, Table materializedViewTable, RelOptMaterialization newMaterialization) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = ensureDbMap(materializedViewTable);

    dbMap.compute(materializedViewTable.getTableName(), (mvTableName, existingMaterialization) -> {
      if (existingMaterialization == null) {
        // If it was not existing, we just create it
        sqlToMaterializedView.put(materializedViewTable.getViewOriginalText(), newMaterialization);
        return newMaterialization;
      }
      Table existingMaterializedViewTable = HiveMaterializedViewUtils.extractTable(existingMaterialization);
      if (existingMaterializedViewTable.equals(oldMaterializedViewTable)) {
        // If the old version is the same, we replace it
        sqlToMaterializedView.put(materializedViewTable.getViewOriginalText(), newMaterialization);
        return newMaterialization;
      }
      // Otherwise, we return existing materialization
      sqlToMaterializedView.put(materializedViewTable.getViewOriginalText(), existingMaterialization);
      return existingMaterialization;
    });
  }

  public void remove(Table materializedViewTable) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(materializedViewTable.getDbName());
    if (dbMap != null) {
      // Delete only if the create time for the input materialized view table and the table
      // in the map match. Otherwise, keep the one in the map.
      dbMap.computeIfPresent(materializedViewTable.getTableName(), (mvTableName, oldMaterialization) -> {
        if (HiveMaterializedViewUtils.extractTable(oldMaterialization).equals(materializedViewTable)) {
          sqlToMaterializedView.remove(materializedViewTable.getViewOriginalText());
          return null;
        }
        return oldMaterialization;
      });
    }
  }

  public void remove(String dbName, String tableName) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(dbName);
    if (dbMap != null) {
      dbMap.computeIfPresent(tableName, (mvTableName, relOptMaterialization) -> {
        String queryText = HiveMaterializedViewUtils.extractTable(relOptMaterialization).getViewOriginalText();
        sqlToMaterializedView.remove(queryText);
        return null;
      });
    }
  }

  public List<RelOptMaterialization> values() {
    List<RelOptMaterialization> result = new ArrayList<>();
    materializedViews.forEach((dbName, mvs) -> result.addAll(mvs.values()));
    return result;
  }

  RelOptMaterialization get(String dbName, String viewName) {
    if (materializedViews.get(dbName) != null) {
      return materializedViews.get(dbName).get(viewName);
    }
    return null;
  }

  public RelOptMaterialization get(String queryText) {
    return sqlToMaterializedView.get(queryText);
  }
}