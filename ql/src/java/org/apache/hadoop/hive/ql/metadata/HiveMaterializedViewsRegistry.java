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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.MaterializedViewIncrementalRewritingRelVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.CBOPlan;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.QueryTables;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.google.common.collect.ImmutableList;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization.RewriteAlgorithm.ALL;
import static org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization.RewriteAlgorithm.TEXT;

/**
 * Registry for materialized views. The goal of this cache is to avoid parsing and creating
 * logical plans for the materialized views at query runtime. When a query arrives, we will
 * just need to consult this cache and extract the logical plans for the views (which had
 * already been parsed) from it. This cache lives in HS2.
 */
public final class HiveMaterializedViewsRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewsRegistry.class);
  private static final String CLASS_NAME = HiveMaterializedViewsRegistry.class.getName();

  public interface MaterializedViewsRegistry {

    /**
     * Adds a newly created materialized view to the cache.
     */
    default void createMaterializedView(HiveConf conf, Table materializedViewTable) { }

    /**
     * Update the materialized view in the registry (if existing materialized view matches).
     */
    default void refreshMaterializedView(HiveConf conf, Table materializedViewTable) {

    }

    /**
     * Update the materialized view in the registry (if existing materialized view matches).
     */
    default void refreshMaterializedView(HiveConf conf, Table oldMaterializedViewTable, Table materializedViewTable) {

    }

    /**
     * Removes the materialized view from the cache (based on table object equality), if exists.
     */
    default void dropMaterializedView(Table materializedViewTable) { }

    /**
     * Removes the materialized view from the cache (based on qualified name), if exists.
     */
    default void dropMaterializedView(String dbName, String tableName) { }

    /**
     * Returns all the materialized views enabled for Calcite based rewriting in the cache.
     *
     * @return the collection of materialized views, or the empty collection if none
     */
    default List<HiveRelOptMaterialization> getRewritingMaterializedViews() { return Collections.emptyList(); }

    /**
     * Returns the materialized views in the cache for the given database.
     *
     * @return the collection of materialized views, or the empty collection if none
     */
    default HiveRelOptMaterialization getRewritingMaterializedView(
            String dbName, String viewName, EnumSet<HiveRelOptMaterialization.RewriteAlgorithm> scope) {
      return null;
    }

    default List<HiveRelOptMaterialization> getRewritingMaterializedViews(String querySql) {
      return Collections.emptyList();
    }

    default boolean isEmpty() {
      return true;
    }
  }

  private HiveMaterializedViewsRegistry() {}

  /* Singleton */
  private static MaterializedViewsRegistry SINGLETON = null;

  /**
   * Get instance of HiveMaterializedViewsRegistry.
   *
   * @return the singleton
   */
  public static MaterializedViewsRegistry get() {
    if (SINGLETON == null) {
      init();
    }
    return SINGLETON;
  }

  /**
   * Initialize the registry for the given database. It will extract the materialized views
   * that are enabled for rewriting from the metastore for the current user, parse them,
   * and register them in this cache.
   *
   * The loading process runs on the background; the method returns in the moment that the
   * runnable task is created, thus the views will still not be loaded in the cache when
   * it returns.
   */
  private static void init() {
    try {
      // Create a new conf object to bypass metastore authorization, as we need to
      // retrieve all materialized views from all databases
      HiveConf conf = new HiveConf();
      conf.set(MetastoreConf.ConfVars.FILTER_HOOK.getVarname(),
          DefaultMetaStoreFilterHookImpl.class.getName());
      init(Hive.get(conf));
    } catch (HiveException e) {
      LOG.error("Problem connecting to the metastore when initializing the view registry", e);
    }
  }

  private static void init(Hive db) {
    final boolean dummy = db.getConf().get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname)
        .equals("DUMMY");
    if (dummy) {
      // Dummy registry does not cache information and forwards all requests to metastore
      SINGLETON = new MaterializedViewsRegistry() {};
      LOG.info("Using dummy materialized views registry");
    } else {
      SINGLETON = new InMemoryMaterializedViewsRegistry(HiveMaterializedViewsRegistry::createMaterialization);
      // We initialize the cache
      long period = HiveConf.getTimeVar(db.getConf(), ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_REFRESH, TimeUnit.SECONDS);
      if (period <= 0) {
        return;
      }
      
      ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("HiveMaterializedViewsRegistry-%d")
              .build());

      MaterializedViewObjects objects = db::getAllMaterializedViewObjectsForRewriting;
      pool.scheduleAtFixedRate(new Loader(db.getConf(), SINGLETON, objects), 0, period, TimeUnit.SECONDS);
    }
  }

  public interface MaterializedViewObjects {
    List<Table> getAllMaterializedViewObjectsForRewriting() throws HiveException;
  }

  public static class Loader implements Runnable {
    protected final HiveConf hiveConf;
    protected final MaterializedViewsRegistry materializedViewsRegistry;
    protected final MaterializedViewObjects materializedViewObjects;
    /* Whether the cache has been initialized or not. */

    Loader(HiveConf hiveConf,
            MaterializedViewsRegistry materializedViewsRegistry,
            MaterializedViewObjects materializedViewObjects) {
      this.hiveConf = hiveConf;
      this.materializedViewsRegistry = materializedViewsRegistry;
      this.materializedViewObjects = materializedViewObjects;
    }

    @Override
    public void run() {
      refresh();
    }

    public void refresh() {
      PerfLogger perfLogger = getPerfLogger();
      perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.MATERIALIZED_VIEWS_REGISTRY_REFRESH);
      try {
        List<Table> materializedViewObjects = this.materializedViewObjects.getAllMaterializedViewObjectsForRewriting();
        for (Table mvTable : materializedViewObjects) {
          RelOptMaterialization existingMV = materializedViewsRegistry.getRewritingMaterializedView(
                  mvTable.getDbName(), mvTable.getTableName(), ALL);
          if (existingMV != null) {
            // We replace if the existing MV is not newer
            Table existingMVTable = HiveMaterializedViewUtils.extractTable(existingMV);
            if (existingMVTable.getCreateTime() < mvTable.getCreateTime() ||
                    (existingMVTable.getCreateTime() == mvTable.getCreateTime() &&
                            existingMVTable.getMVMetadata().getMaterializationTime() <= mvTable.getMVMetadata().getMaterializationTime())) {
              materializedViewsRegistry.refreshMaterializedView(hiveConf, existingMVTable, mvTable);
            }
          } else {
            // Simply replace if it still does not exist
            materializedViewsRegistry.refreshMaterializedView(hiveConf, null, mvTable);
          }
        }

        for (HiveRelOptMaterialization materialization : materializedViewsRegistry.getRewritingMaterializedViews()) {
          Table mvTableInCache = HiveMaterializedViewUtils.extractTable(materialization);
          Table mvTableInHMS = materializedViewObjects.stream()
                  .filter(table -> table.getDbName().equals(mvTableInCache.getDbName())
                          && table.getTableName().equals(mvTableInCache.getTableName()))
                  .findAny()
                  .orElse(null);

          if (mvTableInHMS == null) {
            materializedViewsRegistry.dropMaterializedView(mvTableInCache);
          }
        }

        LOG.info("Materialized views registry has been refreshed");
      } catch (HiveException e) {
        LOG.error("Problem connecting to the metastore when refreshing the view registry", e);
      }
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.MATERIALIZED_VIEWS_REGISTRY_REFRESH);
    }

    private PerfLogger getPerfLogger() {
      SessionState ss = new SessionState(hiveConf);
      ss.setIsHiveServerQuery(true); // All is served from HS2, we do not need e.g. Tez sessions
      SessionState.start(ss);
      return SessionState.getPerfLogger();
    }
  }

  public static class InMemoryMaterializedViewsRegistry implements MaterializedViewsRegistry {

    private final MaterializedViewsCache materializedViewsCache = new MaterializedViewsCache();
    private final BiFunction<HiveConf, Table, HiveRelOptMaterialization> materializationFactory;

    public InMemoryMaterializedViewsRegistry(BiFunction<HiveConf, Table, HiveRelOptMaterialization> materializationFactory) {
      this.materializationFactory = materializationFactory;
    }

    /**
     * Adds a newly created materialized view to the cache.
     */
    @Override
    public void createMaterializedView(HiveConf conf, Table materializedViewTable) {
      // Bail out if it is not enabled for rewriting
      if (!materializedViewTable.isRewriteEnabled()) {
        LOG.debug("Materialized view " + materializedViewTable.getCompleteName() +
                " ignored; it is not rewrite enabled");
        return;
      }

      HiveRelOptMaterialization materialization = materializationFactory.apply(conf, materializedViewTable);
      if (materialization == null) {
        return;
      }

      materializedViewsCache.putIfAbsent(materializedViewTable, materialization);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Created materialized view for rewriting: " + materializedViewTable.getFullyQualifiedName());
      }
    }

    /**
     * Update the materialized view in the registry (if materialized view exists).
     */
    @Override
    public void refreshMaterializedView(HiveConf conf, Table materializedViewTable) {
      RelOptMaterialization cached = materializedViewsCache.get(
              materializedViewTable.getDbName(), materializedViewTable.getTableName());
      if (cached == null) {
        return;
      }
      Table cachedTable = HiveMaterializedViewUtils.extractTable(cached);
      refreshMaterializedView(conf, cachedTable, materializedViewTable);
    }

    /**
     * Update the materialized view in the registry (if existing materialized view matches).
     */
    @Override
    public void refreshMaterializedView(HiveConf conf, Table oldMaterializedViewTable, Table materializedViewTable) {
      // Bail out if it is not enabled for rewriting
      if (!materializedViewTable.isRewriteEnabled()) {
        dropMaterializedView(oldMaterializedViewTable);
        LOG.debug("Materialized view " + materializedViewTable.getCompleteName() +
                " dropped; it is not rewrite enabled");
        return;
      }

      final HiveRelOptMaterialization newMaterialization = materializationFactory.apply(conf, materializedViewTable);
      if (newMaterialization == null) {
        return;
      }
      materializedViewsCache.refresh(oldMaterializedViewTable, materializedViewTable, newMaterialization);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Materialized view refreshed: " + materializedViewTable.getFullyQualifiedName());
      }
    }

    /**
     * Removes the materialized view from the cache (based on table object equality), if exists.
     */
    @Override
    public void dropMaterializedView(Table materializedViewTable) {
      materializedViewsCache.remove(materializedViewTable);
    }

    /**
     * Removes the materialized view from the cache (based on qualified name), if exists.
     */
    @Override
    public void dropMaterializedView(String dbName, String tableName) {
      materializedViewsCache.remove(dbName, tableName);
    }

    /**
     * Returns all the materialized views enabled for Calcite based rewriting in the cache.
     *
     * @return the collection of materialized views, or the empty collection if none
     */
    @Override
    public List<HiveRelOptMaterialization> getRewritingMaterializedViews() {
      return materializedViewsCache.values().stream()
              .filter(materialization -> materialization.getScope().contains(HiveRelOptMaterialization.RewriteAlgorithm.CALCITE))
              .collect(toList());
    }

    /**
     * Returns the materialized views in the cache for the given database.
     *
     * @return the collection of materialized views, or the empty collection if none
     */
    @Override
    public HiveRelOptMaterialization getRewritingMaterializedView(String dbName, String viewName,
                                                                  EnumSet<HiveRelOptMaterialization.RewriteAlgorithm> scope) {
      HiveRelOptMaterialization materialization = materializedViewsCache.get(dbName, viewName);
      if (materialization == null) {
        return null;
      }
      if (!materialization.isSupported(scope)) {
        return null;
      }
      return materialization;
    }

    @Override
    public List<HiveRelOptMaterialization> getRewritingMaterializedViews(String querySql) {
      return materializedViewsCache.get(querySql);
    }

    @Override
    public boolean isEmpty() {
      return materializedViewsCache.isEmpty();
    }
  }

  /**
   * Parses and creates a materialization.
   */
  public static HiveRelOptMaterialization createMaterialization(HiveConf conf, Table materializedViewTable) {
    // First we parse the view query and create the materialization object
    final String viewQuery = materializedViewTable.getViewExpandedText();
    final RelNode viewScan = createMaterializedViewScan(conf, materializedViewTable);
    if (viewScan == null) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
              " ignored; error creating view replacement");
      return null;
    }
    final CBOPlan plan;
    try {
      plan = ParseUtils.parseQuery(conf, viewQuery);
    } catch (Exception e) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
              " ignored; error parsing original query; " + e);
      return null;
    }

    return new HiveRelOptMaterialization(viewScan, plan.getPlan(),
            null, viewScan.getTable().getQualifiedName(),
            isBlank(plan.getInvalidAutomaticRewritingMaterializationReason()) ?
                    EnumSet.allOf(HiveRelOptMaterialization.RewriteAlgorithm.class) : EnumSet.of(TEXT),
            determineIncrementalRebuildMode(plan.getPlan()));
  }

  private static HiveRelOptMaterialization.IncrementalRebuildMode determineIncrementalRebuildMode(RelNode definitionPlan) {
    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    visitor.go(definitionPlan);
    if (!visitor.isRewritingAllowed()) {
      return HiveRelOptMaterialization.IncrementalRebuildMode.NOT_AVAILABLE;
    }
    if (visitor.isContainsAggregate() && !visitor.hasCountStar()) {
      return HiveRelOptMaterialization.IncrementalRebuildMode.INSERT_ONLY;
    }
    return HiveRelOptMaterialization.IncrementalRebuildMode.AVAILABLE;
  }

  private static RelNode createMaterializedViewScan(HiveConf conf, Table viewTable) {
    // 0. Recreate cluster
    final RelOptPlanner planner = CalcitePlanner.createPlanner(conf);
    final RexBuilder rexBuilder = new RexBuilder(
        new JavaTypeFactoryImpl(
            new HiveTypeSystemImpl()));
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
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<>();
    for (StructField structField : fields) {
      colName = structField.getFieldName();
      colInfo = new ColumnInfo(
          structField.getFieldName(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(structField.getFieldObjectInspector()),
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
    List<String> fullyQualifiedTabName = new ArrayList<>();
    if (viewTable.getDbName() != null && !viewTable.getDbName().isEmpty()) {
      fullyQualifiedTabName.add(viewTable.getDbName());
    }
    fullyQualifiedTabName.add(viewTable.getTableName());

    RelNode tableRel;

    // 3. Build operator
    if (obtainTableType(viewTable) == TableType.DRUID) {
      // Build Druid query
      String address = HiveConf.getVar(conf,
          HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
      String dataSource = viewTable.getParameters().get(Constants.DRUID_DATA_SOURCE);
      Set<String> metrics = new HashSet<>();
      List<RelDataType> druidColTypes = new ArrayList<>();
      List<String> druidColNames = new ArrayList<>();
      //@NOTE this code is very similar to the code at org/apache/hadoop/hive/ql/parse/CalcitePlanner.java:2362
      //@TODO it will be nice to refactor it
      RelDataTypeFactory dtFactory = cluster.getRexBuilder().getTypeFactory();
      for (RelDataTypeField field : rowType.getFieldList()) {
        if (DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(field.getName())) {
          // Druid's time column is always not null.
          druidColTypes.add(dtFactory.createTypeWithNullability(field.getType(), false));
        } else {
          druidColTypes.add(field.getType());
        }
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

      List<Interval> intervals = Collections.singletonList(DruidTable.DEFAULT_INTERVAL);
      rowType = dtFactory.createStructType(druidColTypes, druidColNames);
      // We can pass null for Hive object because it is only used to retrieve tables
      // if constraints on a table object are existing, but constraints cannot be defined
      // for materialized views.
      RelOptHiveTable optTable = new RelOptHiveTable(null, cluster.getTypeFactory(), fullyQualifiedTabName,
          rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<>(),
          conf, null, new QueryTables(true), new HashMap<>(), new HashMap<>(), new AtomicInteger());
      DruidTable druidTable = new DruidTable(new DruidSchema(address, address, false),
          dataSource, RelDataTypeImpl.proto(rowType), metrics, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
          intervals, null, null);
      final TableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          optTable, viewTable.getTableName(), null, false, false);
      tableRel = DruidQuery.create(cluster, cluster.traitSetOf(BindableConvention.INSTANCE),
          optTable, druidTable, ImmutableList.<RelNode>of(scan), ImmutableMap.of());
    } else {
      // Build Hive Table Scan Rel.
      // We can pass null for Hive object because it is only used to retrieve tables
      // if constraints on a table object are existing, but constraints cannot be defined
      // for materialized views.
      RelOptHiveTable optTable = new RelOptHiveTable(null, cluster.getTypeFactory(), fullyQualifiedTabName,
          rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<>(),
          conf, null, new QueryTables(true), new HashMap<>(), new HashMap<>(), new AtomicInteger());
      tableRel = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
          viewTable.getTableName(), null, false, false);
    }

    return tableRel;
  }

  private static TableType obtainTableType(Table tabMetaData) {
    if (tabMetaData.getStorageHandler() != null) {
      final String storageHandlerStr = tabMetaData.getStorageHandler().toString();
      if (storageHandlerStr.equals(Constants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
        return TableType.DRUID;
      }

      if (storageHandlerStr.equals(Constants.JDBC_HIVE_STORAGE_HANDLER_ID)) {
        return TableType.JDBC;
      }

    }

    return TableType.NATIVE;
  }

  //@TODO this seems to be the same as org.apache.hadoop.hive.ql.parse.CalcitePlanner.TableType.DRUID do we really need both
  private enum TableType {
    DRUID,
    NATIVE,
    JDBC
  }

}
