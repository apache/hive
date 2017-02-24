/**
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveVolcanoPlanner;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/** 
 * Registry for materialized views. The goal of this cache is to avoid parsing and creating
 * logical plans for the materialized views at query runtime. When a query arrives, we will
 * just need to consult this cache and extract the logical plans for the views (which had
 * already been parsed) from it.
 */
public final class HiveMaterializedViewsRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewsRegistry.class);

  /* Singleton */
  private static final HiveMaterializedViewsRegistry SINGLETON = new HiveMaterializedViewsRegistry();

  /* Key is the database name. Value a map from a unique identifier for the view comprising
   * the qualified name and the creation time, to the view object.
   * Since currently we cannot alter a materialized view, that should suffice to identify
   * whether the cached view is up to date or not.
   * Creation time is useful to ensure correctness in case multiple HS2 instances are used. */
  private final ConcurrentMap<String, ConcurrentMap<ViewKey, RelOptMaterialization>> materializedViews =
      new ConcurrentHashMap<String, ConcurrentMap<ViewKey, RelOptMaterialization>>();
  private final ExecutorService pool = Executors.newCachedThreadPool();

  private HiveMaterializedViewsRegistry() {
  }

  /**
   * Get instance of HiveMaterializedViewsRegistry.
   *
   * @return the singleton
   */
  public static HiveMaterializedViewsRegistry get() {
    return SINGLETON;
  }

  /**
   * Initialize the registry for the given database. It will extract the materialized views
   * that are enabled for rewriting from the metastore for the current user, parse them,
   * and register them in this cache.
   *
   * The loading process runs on the background; the method returns in the moment that the
   * runnable task is created, thus the views will still not be loaded in the cache when
   * it does.
   */
  public void init(final Hive db) {
    try {
      List<Table> tables = new ArrayList<Table>();
      for (String dbName : db.getAllDatabases()) {
        // TODO: We should enhance metastore API such that it returns only
        // materialized views instead of all tables
        tables.addAll(db.getAllTableObjects(dbName));
      }
      pool.submit(new Loader(tables));
    } catch (HiveException e) {
      LOG.error("Problem connecting to the metastore when initializing the view registry");
    }
  }

  private class Loader implements Runnable {
    private final List<Table> tables;

    private Loader(List<Table> tables) {
      this.tables = tables;
    }

    @Override
    public void run() {
      for (Table table : tables) {
        if (table.isMaterializedView()) {
          addMaterializedView(table);
        }
      }
    }
  }

  /**
   * Adds the materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  public RelOptMaterialization addMaterializedView(Table materializedViewTable) {
    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      return null;
    }
    ConcurrentMap<ViewKey, RelOptMaterialization> cq =
        new ConcurrentHashMap<ViewKey, RelOptMaterialization>();
    final ConcurrentMap<ViewKey, RelOptMaterialization> prevCq = materializedViews.putIfAbsent(
        materializedViewTable.getDbName(), cq);
    if (prevCq != null) {
      cq = prevCq;
    }
    // Bail out if it already exists
    final ViewKey vk = new ViewKey(
        materializedViewTable.getTableName(), materializedViewTable.getCreateTime());
    if (cq.containsKey(vk)) {
      return null;
    }
    // Add to cache
    final String viewQuery = materializedViewTable.getViewOriginalText();
    final RelNode tableRel = createTableScan(materializedViewTable);
    if (tableRel == null) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
              " ignored; error creating view replacement");
      return null;
    }
    final RelNode queryRel = parseQuery(viewQuery);
    if (queryRel == null) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
              " ignored; error parsing original query");
      return null;
    }
    RelOptMaterialization materialization = new RelOptMaterialization(tableRel, queryRel, null);
    cq.put(vk, materialization);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cached materialized view for rewriting: " + tableRel.getTable().getQualifiedName());
    }
    return materialization;
  }

  /**
   * Removes the materialized view from the cache.
   *
   * @param materializedViewTable the materialized view to remove
   */
  public void dropMaterializedView(Table materializedViewTable) {
    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      return;
    }
    final ViewKey vk = new ViewKey(
        materializedViewTable.getTableName(), materializedViewTable.getCreateTime());
    materializedViews.get(materializedViewTable.getDbName()).remove(vk);
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @param dbName the database
   * @return the collection of materialized views, or the empty collection if none
   */
  Collection<RelOptMaterialization> getRewritingMaterializedViews(String dbName) {
    if (materializedViews.get(dbName) != null) {
      return Collections.unmodifiableCollection(materializedViews.get(dbName).values());
    }
    return ImmutableList.of();
  }

  private static RelNode createTableScan(Table viewTable) {
    // 0. Recreate cluster
    final RelOptPlanner planner = HiveVolcanoPlanner.createPlanner(null);
    final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    // 1. Create column schema
    final RowResolver rr = new RowResolver();
    // 1.1 Add Column info for non partion cols (Object Inspector fields)
    StructObjectInspector rowObjectInspector;
    try {
      rowObjectInspector = (StructObjectInspector) viewTable.getDeserializer()
          .getObjectInspector();
    } catch (SerDeException e) {
      // Bail out
      return null;
    }
    List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
    ColumnInfo colInfo;
    String colName;
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (int i = 0; i < fields.size(); i++) {
      colName = fields.get(i).getFieldName();
      colInfo = new ColumnInfo(
          fields.get(i).getFieldName(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector()),
          null, false);
      rr.put(null, colName, colInfo);
      cInfoLst.add(colInfo);
    }
    ArrayList<ColumnInfo> nonPartitionColumns = new ArrayList<ColumnInfo>(cInfoLst);

    // 1.2 Add column info corresponding to partition columns
    ArrayList<ColumnInfo> partitionColumns = new ArrayList<ColumnInfo>();
    for (FieldSchema part_col : viewTable.getPartCols()) {
      colName = part_col.getName();
      colInfo = new ColumnInfo(colName,
          TypeInfoFactory.getPrimitiveTypeInfo(part_col.getType()), null, true);
      rr.put(null, colName, colInfo);
      cInfoLst.add(colInfo);
      partitionColumns.add(colInfo);
    }

    // 1.3 Build row type from field <type, name>
    RelDataType rowType;
    try {
      rowType = TypeConverter.getType(cluster, rr, null);
    } catch (CalciteSemanticException e) {
      // Bail out
      return null;
    }

    // 2. Build RelOptAbstractTable
    String fullyQualifiedTabName = viewTable.getDbName();
    if (fullyQualifiedTabName != null && !fullyQualifiedTabName.isEmpty()) {
      fullyQualifiedTabName = fullyQualifiedTabName + "." + viewTable.getTableName();
    }
    else {
      fullyQualifiedTabName = viewTable.getTableName();
    }
    RelOptHiveTable optTable = new RelOptHiveTable(null, fullyQualifiedTabName,
        rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<VirtualColumn>(),
        SessionState.get().getConf(), new HashMap<String, PrunedPartitionList>(),
        new AtomicInteger());
    RelNode tableRel;

    // 3. Build operator
    if (obtainTableType(viewTable) == TableType.DRUID) {
      // Build Druid query
      String address = HiveConf.getVar(SessionState.get().getConf(),
          HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
      String dataSource = viewTable.getParameters().get(Constants.DRUID_DATA_SOURCE);
      Set<String> metrics = new HashSet<>();
      List<RelDataType> druidColTypes = new ArrayList<>();
      List<String> druidColNames = new ArrayList<>();
      for (RelDataTypeField field : rowType.getFieldList()) {
        druidColTypes.add(field.getType());
        druidColNames.add(field.getName());
        if (field.getName().equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
          // timestamp
          continue;
        }
        if (field.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
          // dimension
          continue;
        }
        metrics.add(field.getName());
      }
      List<Interval> intervals = Arrays.asList(DruidTable.DEFAULT_INTERVAL);

      DruidTable druidTable = new DruidTable(new DruidSchema(address, address, false),
          dataSource, RelDataTypeImpl.proto(rowType), metrics, DruidTable.DEFAULT_TIMESTAMP_COLUMN, intervals);
      final TableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          optTable, viewTable.getTableName(), null, false, false);
      tableRel = DruidQuery.create(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          optTable, druidTable, ImmutableList.<RelNode>of(scan));
    } else {
      // Build Hive Table Scan Rel
      tableRel = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
          viewTable.getTableName(), null, false, false);
    }
    return tableRel;
  }

  private static RelNode parseQuery(String viewQuery) {
    try {
      final ASTNode node = ParseUtils.parse(viewQuery);
      final QueryState qs = new QueryState(SessionState.get().getConf());
      CalcitePlanner analyzer = new CalcitePlanner(qs);
      analyzer.initCtx(new Context(SessionState.get().getConf()));
      analyzer.init(false);
      return analyzer.genLogicalPlan(node);
    } catch (Exception e) {
      // We could not parse the view
      return null;
    }
  }

  private static class ViewKey {
    private String viewName;
    private int creationDate;

    private ViewKey(String viewName, int creationTime) {
      this.viewName = viewName;
      this.creationDate = creationTime;
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if((obj == null) || (obj.getClass() != this.getClass())) {
        return false;
      }
      ViewKey viewKey = (ViewKey) obj;
      return creationDate == viewKey.creationDate &&
          (viewName == viewKey.viewName || (viewName != null && viewName.equals(viewKey.viewName)));
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 31 * hash + creationDate;
      hash = 31 * hash + viewName.hashCode();
      return hash;
    }

    @Override
    public String toString() {
      return "ViewKey{" + viewName + "," + creationDate + "}";
    }
  }

  private static TableType obtainTableType(Table tabMetaData) {
    if (tabMetaData.getStorageHandler() != null &&
            tabMetaData.getStorageHandler().toString().equals(
                    Constants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
      return TableType.DRUID;
    }
    return TableType.NATIVE;
  }

  private enum TableType {
    DRUID,
    NATIVE
  }
}
