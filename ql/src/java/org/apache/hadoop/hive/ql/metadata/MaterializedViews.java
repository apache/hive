package org.apache.hadoop.hive.ql.metadata;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MaterializedViews {
  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViews.class);

  /* Key is the database name. Value a map from the qualified name to the view object. */
  private final ConcurrentMap<String, ConcurrentMap<String, RelOptMaterialization>> materializedViews =
          new ConcurrentHashMap<>();
  // Map for looking up materialization by view query text
  private final Map<String, RelOptMaterialization> sqlToMaterializedView = new ConcurrentHashMap<>();


  public void putIfAbsent(Table materializedViewTable, RelOptMaterialization materialization) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = ensureDbMap(materializedViewTable);

    // You store the materialized view
    dbMap.compute(materializedViewTable.getTableName(), (mvTableName, relOptMaterialization) -> {
      sqlToMaterializedView.put(materializedViewTable.getViewExpandedText(), materialization);
      return materialization;
    });

    LOG.debug("Materialized view {}.{} added to registry",
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
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
        sqlToMaterializedView.put(materializedViewTable.getViewExpandedText(), newMaterialization);
        return newMaterialization;
      }
      Table existingMaterializedViewTable = HiveMaterializedViewUtils.extractTable(existingMaterialization);
      if (existingMaterializedViewTable.equals(oldMaterializedViewTable)) {
        // If the old version is the same, we replace it
        sqlToMaterializedView.put(materializedViewTable.getViewExpandedText(), newMaterialization);
        return newMaterialization;
      }
      // Otherwise, we return existing materialization
      sqlToMaterializedView.put(materializedViewTable.getViewExpandedText(), existingMaterialization);
      return existingMaterialization;
    });

    LOG.debug("Refreshed materialized view {}.{} -> {}.{}",
            oldMaterializedViewTable.getDbName(), oldMaterializedViewTable.getTableName(),
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  public void remove(Table materializedViewTable) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(materializedViewTable.getDbName());
    if (dbMap != null) {
      // Delete only if the create time for the input materialized view table and the table
      // in the map match. Otherwise, keep the one in the map.
      dbMap.computeIfPresent(materializedViewTable.getTableName(), (mvTableName, oldMaterialization) -> {
        if (HiveMaterializedViewUtils.extractTable(oldMaterialization).equals(materializedViewTable)) {
          sqlToMaterializedView.remove(materializedViewTable.getViewExpandedText());
          return null;
        }
        return oldMaterialization;
      });
    }

    LOG.debug("Materialized view {}.{} removed from registry",
            materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  public void remove(String dbName, String tableName) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(dbName);
    if (dbMap != null) {
      dbMap.computeIfPresent(tableName, (mvTableName, relOptMaterialization) -> {
        String queryText = HiveMaterializedViewUtils.extractTable(relOptMaterialization).getViewExpandedText();
        sqlToMaterializedView.remove(queryText);
        return null;
      });

      LOG.debug("Materialized view {}.{} removed from registry", dbName, tableName);
    }
  }

  public List<RelOptMaterialization> values() {
    List<RelOptMaterialization> result = new ArrayList<>();
    materializedViews.forEach((dbName, mvs) -> result.addAll(mvs.values()));
    return result;
  }

  RelOptMaterialization get(String dbName, String viewName) {
    if (materializedViews.get(dbName) != null) {
      LOG.debug("Found materialized view {}.{} in registry", dbName, viewName);
      return materializedViews.get(dbName).get(viewName);
    }
    LOG.debug("Materialized view {}.{} not found in registry", dbName, viewName);
    return null;
  }

  public RelOptMaterialization get(String queryText) {
    RelOptMaterialization relOptMaterialization = sqlToMaterializedView.get(queryText);
    if (relOptMaterialization == null) {
      LOG.debug("No materialized view with query text '{}' found in registry", queryText);
      return null;
    }
    LOG.debug("Found materialized view with query text '{}' in registry", queryText);
    return relOptMaterialization;
  }
}