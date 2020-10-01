package org.apache.hadoop.hive.ql.cache.view.materialized;

import javafx.util.Pair;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MaterializedViewCache {
  public static final MaterializedViewCache INSTANCE = new MaterializedViewCache();

  private final Map<String, Pair<String, String>> sqlToMaterializedView;

  public MaterializedViewCache() {
    sqlToMaterializedView = new ConcurrentHashMap<>();
  }

  public void add(String queryText, String dbName, String materializedViewName) {
    sqlToMaterializedView.putIfAbsent(queryText, new Pair<>(dbName, materializedViewName));
  }

  public RelOptMaterialization lookup(String queryText) {
    Pair<String, String> materializedViewName = sqlToMaterializedView.get(queryText);
    if (materializedViewName == null) {
      return null;
    }

    return HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(materializedViewName.getKey(), materializedViewName.getValue());
  }
}
