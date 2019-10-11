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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import java.util.function.BiFunction;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
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
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
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

/**
 * Registry for materialized views. The goal of this cache is to avoid parsing and creating
 * logical plans for the materialized views at query runtime. When a query arrives, we will
 * just need to consult this cache and extract the logical plans for the views (which had
 * already been parsed) from it. This cache lives in HS2.
 */
public final class HiveMaterializedViewsRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewsRegistry.class);
  private static final String CLASS_NAME = HiveMaterializedViewsRegistry.class.getName();

  /* Singleton */
  private static final HiveMaterializedViewsRegistry SINGLETON = new HiveMaterializedViewsRegistry();

  /* Key is the database name. Value a map from the qualified name to the view object. */
  private final ConcurrentMap<String, ConcurrentMap<String, RelOptMaterialization>> materializedViews =
      new ConcurrentHashMap<String, ConcurrentMap<String, RelOptMaterialization>>();

  /* Whether the cache has been initialized or not. */
  private AtomicBoolean initialized = new AtomicBoolean(false);

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
   * it returns.
   */
  public void init() {
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

  public void init(Hive db) {
    final boolean dummy = db.getConf().get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname)
        .equals("DUMMY");
    if (dummy) {
      // Dummy registry does not cache information and forwards all requests to metastore
      initialized.set(true);
      LOG.info("Using dummy materialized views registry");
    } else {
      // We initialize the cache
      long period = HiveConf.getTimeVar(db.getConf(), ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_REFRESH, TimeUnit.SECONDS);
      ScheduledExecutorService pool = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("HiveMaterializedViewsRegistry-%d")
              .build());
      pool.scheduleAtFixedRate(new Loader(db), 0, period, TimeUnit.SECONDS);
    }
  }

  private class Loader implements Runnable {
    private final Hive db;

    private Loader(Hive db) {
      this.db = db;
    }

    @Override
    public void run() {
      SessionState ss = new SessionState(db.getConf());
      ss.setIsHiveServerQuery(true); // All is served from HS2, we do not need e.g. Tez sessions
      SessionState.start(ss);
      PerfLogger perfLogger = SessionState.getPerfLogger();
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MATERIALIZED_VIEWS_REGISTRY_REFRESH);
      try {
        if (initialized.get()) {
          for (Table mvTable : db.getAllMaterializedViewObjectsForRewriting()) {
            RelOptMaterialization existingMV = getRewritingMaterializedView(mvTable.getDbName(), mvTable.getTableName());
            if (existingMV != null) {
              // We replace if the existing MV is not newer
              Table existingMVTable = extractTable(existingMV);
              if (existingMVTable.getCreateTime() < mvTable.getCreateTime() ||
                  (existingMVTable.getCreateTime() == mvTable.getCreateTime() &&
                      existingMVTable.getCreationMetadata().getMaterializationTime() <= mvTable.getCreationMetadata().getMaterializationTime())) {
                refreshMaterializedView(db.getConf(), existingMVTable, mvTable);
              }
            } else {
              // Simply replace if it still does not exist
              refreshMaterializedView(db.getConf(), null, mvTable);
            }
          }
          LOG.info("Materialized views registry has been refreshed");
        } else {
          for (Table mvTable : db.getAllMaterializedViewObjectsForRewriting()) {
            refreshMaterializedView(db.getConf(), null, mvTable);
          }
          initialized.set(true);
          LOG.info("Materialized views registry has been initialized");
        }
      } catch (HiveException e) {
        if (initialized.get()) {
          LOG.error("Problem connecting to the metastore when refreshing the view registry", e);
        } else {
          LOG.error("Problem connecting to the metastore when initializing the view registry", e);
        }
      }
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MATERIALIZED_VIEWS_REGISTRY_REFRESH);
    }
  }

  public boolean isInitialized() {
    return initialized.get();
  }

  /**
   * Parses and creates a materialization.
   */
  public RelOptMaterialization createMaterialization(HiveConf conf, Table materializedViewTable) {
    // First we parse the view query and create the materialization object
    final String viewQuery = materializedViewTable.getViewExpandedText();
    final RelNode viewScan = createMaterializedViewScan(conf, materializedViewTable);
    if (viewScan == null) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
          " ignored; error creating view replacement");
      return null;
    }
    final RelNode queryRel;
    try {
      queryRel = ParseUtils.parseQuery(conf, viewQuery);
    } catch (Exception e) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
          " ignored; error parsing original query; " + e);
      return null;
    }

    return new RelOptMaterialization(viewScan, queryRel,
        null, viewScan.getTable().getQualifiedName());
  }

  /**
   * Adds a newly created materialized view to the cache.
   */
  public void createMaterializedView(HiveConf conf, Table materializedViewTable) {
    final boolean cache = !conf.get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname)
        .equals("DUMMY");
    if (!cache) {
      // Nothing to do, bail out
      return;
    }

    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      LOG.debug("Materialized view " + materializedViewTable.getCompleteName() +
          " ignored; it is not rewrite enabled");
      return;
    }

    // We are going to create the map for each view in the given database
    ConcurrentMap<String, RelOptMaterialization> dbMap =
        new ConcurrentHashMap<String, RelOptMaterialization>();
    // If we are caching the MV, we include it in the cache
    final ConcurrentMap<String, RelOptMaterialization> prevDbMap = materializedViews.putIfAbsent(
        materializedViewTable.getDbName(), dbMap);
    if (prevDbMap != null) {
      dbMap = prevDbMap;
    }

    RelOptMaterialization materialization = createMaterialization(conf, materializedViewTable);
    if (materialization == null) {
      return;
    }
    // You store the materialized view
    dbMap.put(materializedViewTable.getTableName(), materialization);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created materialized view for rewriting: " + materializedViewTable.getFullyQualifiedName());
    }
  }

  /**
   * Update the materialized view in the registry (if existing materialized view matches).
   */
  public void refreshMaterializedView(HiveConf conf, Table oldMaterializedViewTable, Table materializedViewTable) {
    final boolean cache = !conf.get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname)
        .equals("DUMMY");
    if (!cache) {
      // Nothing to do, bail out
      return;
    }

    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      dropMaterializedView(oldMaterializedViewTable);
      LOG.debug("Materialized view " + materializedViewTable.getCompleteName() +
          " dropped; it is not rewrite enabled");
      return;
    }

    // We are going to create the map for each view in the given database
    ConcurrentMap<String, RelOptMaterialization> dbMap =
        new ConcurrentHashMap<String, RelOptMaterialization>();
    // If we are caching the MV, we include it in the cache
    final ConcurrentMap<String, RelOptMaterialization> prevDbMap = materializedViews.putIfAbsent(
        materializedViewTable.getDbName(), dbMap);
    if (prevDbMap != null) {
      dbMap = prevDbMap;
    }
    final RelOptMaterialization newMaterialization = createMaterialization(conf, materializedViewTable);
    if (newMaterialization == null) {
      return;
    }
    dbMap.compute(materializedViewTable.getTableName(), new BiFunction<String, RelOptMaterialization, RelOptMaterialization>() {
      @Override
      public RelOptMaterialization apply(String tableName, RelOptMaterialization existingMaterialization) {
        if (existingMaterialization == null) {
          // If it was not existing, we just create it
          return newMaterialization;
        }
        Table existingMaterializedViewTable = extractTable(existingMaterialization);
        if (existingMaterializedViewTable.equals(oldMaterializedViewTable)) {
          // If the old version is the same, we replace it
          return newMaterialization;
        }
        // Otherwise, we return existing materialization
        return existingMaterialization;
      }
    });

    if (LOG.isDebugEnabled()) {
      LOG.debug("Materialized view refreshed: " + materializedViewTable.getFullyQualifiedName());
    }
  }

  /**
   * Removes the materialized view from the cache (based on table object equality), if exists.
   */
  public void dropMaterializedView(Table materializedViewTable) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(materializedViewTable.getDbName());
    if (dbMap != null) {
      // Delete only if the create time for the input materialized view table and the table
      // in the map match. Otherwise, keep the one in the map.
      dbMap.computeIfPresent(materializedViewTable.getTableName(), new BiFunction<String, RelOptMaterialization, RelOptMaterialization>() {
        @Override
        public RelOptMaterialization apply(String tableName, RelOptMaterialization oldMaterialization) {
          if (extractTable(oldMaterialization).equals(materializedViewTable)) {
            return null;
          }
          return oldMaterialization;
        }
      });
    }
  }

  /**
   * Removes the materialized view from the cache (based on qualified name), if exists.
   */
  public void dropMaterializedView(String dbName, String tableName) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(dbName);
    if (dbMap != null) {
      dbMap.remove(tableName);
    }
  }

  /**
   * Returns all the materialized views in the cache.
   *
   * @return the collection of materialized views, or the empty collection if none
   */
  List<RelOptMaterialization> getRewritingMaterializedViews() {
    List<RelOptMaterialization> result = new ArrayList<>();
    materializedViews.forEach((dbName, mvs) -> result.addAll(mvs.values()));
    return result;
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @return the collection of materialized views, or the empty collection if none
   */
  RelOptMaterialization getRewritingMaterializedView(String dbName, String viewName) {
    if (materializedViews.get(dbName) != null) {
      return materializedViews.get(dbName).get(viewName);
    }
    return null;
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

      List<Interval> intervals = Arrays.asList(DruidTable.DEFAULT_INTERVAL);
      rowType = dtFactory.createStructType(druidColTypes, druidColNames);
      // We can pass null for Hive object because it is only used to retrieve tables
      // if constraints on a table object are existing, but constraints cannot be defined
      // for materialized views.
      RelOptHiveTable optTable = new RelOptHiveTable(null, cluster.getTypeFactory(), fullyQualifiedTabName,
          rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<>(),
          conf, null, new HashMap<>(), new HashMap<>(), new HashMap<>(), new AtomicInteger());
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
          conf, null, new HashMap<>(), new HashMap<>(), new HashMap<>(), new AtomicInteger());
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

  private static Table extractTable(RelOptMaterialization materialization) {
    RelOptHiveTable cachedMaterializedViewTable;
    if (materialization.tableRel instanceof Project) {
      // There is a Project on top (due to nullability)
      cachedMaterializedViewTable = (RelOptHiveTable) materialization.tableRel.getInput(0).getTable();
    } else {
      cachedMaterializedViewTable = (RelOptHiveTable) materialization.tableRel.getTable();
    }
    return cachedMaterializedViewTable.getHiveTableMD();
  }

  //@TODO this seems to be the same as org.apache.hadoop.hive.ql.parse.CalcitePlanner.TableType.DRUID do we really need both
  private enum TableType {
    DRUID,
    NATIVE,
    JDBC
  }

}
