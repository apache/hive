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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
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

import com.google.common.collect.ImmutableList;

/**
 * Registry for materialized views. The goal of this cache is to avoid parsing and creating
 * logical plans for the materialized views at query runtime. When a query arrives, we will
 * just need to consult this cache and extract the logical plans for the views (which had
 * already been parsed) from it. This cache lives in HS2.
 */
public final class HiveMaterializedViewsRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewsRegistry.class);

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
      ExecutorService pool = Executors.newCachedThreadPool();
      pool.submit(new Loader(db));
      pool.shutdown();
    }
  }

  private class Loader implements Runnable {
    private final Hive db;

    private Loader(Hive db) {
      this.db = db;
    }

    @Override
    public void run() {
      try {
        SessionState.start(db.getConf());
        final boolean cache = !db.getConf()
            .get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname).equals("DUMMY");
        for (String dbName : db.getAllDatabases()) {
          for (Table mv : db.getAllMaterializedViewObjects(dbName)) {
            addMaterializedView(db.getConf(), mv, OpType.LOAD, cache);
          }
        }
        initialized.set(true);
        LOG.info("Materialized views registry has been initialized");
      } catch (HiveException e) {
        LOG.error("Problem connecting to the metastore when initializing the view registry", e);
      }
    }
  }

  public boolean isInitialized() {
    return initialized.get();
  }

  /**
   * Adds a newly created materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  public RelOptMaterialization createMaterializedView(HiveConf conf, Table materializedViewTable) {
    final boolean cache = !conf.get(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname)
        .equals("DUMMY");
    return addMaterializedView(conf, materializedViewTable, OpType.CREATE, cache);
  }

  /**
   * Adds the materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  private RelOptMaterialization addMaterializedView(HiveConf conf, Table materializedViewTable,
                                                    OpType opType, boolean cache) {
    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      LOG.debug("Materialized view " + materializedViewTable.getCompleteName() +
          " ignored; it is not rewrite enabled");
      return null;
    }

    // We are going to create the map for each view in the given database
    ConcurrentMap<String, RelOptMaterialization> cq =
        new ConcurrentHashMap<String, RelOptMaterialization>();
    if (cache) {
      // If we are caching the MV, we include it in the cache
      final ConcurrentMap<String, RelOptMaterialization> prevCq = materializedViews.putIfAbsent(
          materializedViewTable.getDbName(), cq);
      if (prevCq != null) {
        cq = prevCq;
      }
    }

    // Start the process to add MV to the cache
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

    RelOptMaterialization materialization = new RelOptMaterialization(viewScan, queryRel,
        null, viewScan.getTable().getQualifiedName());
    if (opType == OpType.CREATE) {
      // You store the materialized view
      cq.put(materializedViewTable.getTableName(), materialization);
    } else {
      // For LOAD, you only add it if it does exist as you might be loading an outdated MV
      cq.putIfAbsent(materializedViewTable.getTableName(), materialization);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created materialized view for rewriting: " + viewScan.getTable().getQualifiedName());
    }
    return materialization;
  }

  /**
   * Removes the materialized view from the cache.
   *
   * @param materializedViewTable the materialized view to remove
   */
  public void dropMaterializedView(Table materializedViewTable) {
    dropMaterializedView(materializedViewTable.getDbName(), materializedViewTable.getTableName());
  }

  /**
   * Removes the materialized view from the cache.
   *
   * @param dbName the db for the materialized view to remove
   * @param tableName the name for the materialized view to remove
   */
  public void dropMaterializedView(String dbName, String tableName) {
    ConcurrentMap<String, RelOptMaterialization> dbMap = materializedViews.get(dbName);
    if (dbMap != null) {
      dbMap.remove(tableName);
    }
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @param dbName the database
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
      RelOptHiveTable optTable = new RelOptHiveTable(null, cluster.getTypeFactory(), fullyQualifiedTabName,
          rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<>(),
          conf, new HashMap<>(), new HashMap<>(), new AtomicInteger());
      DruidTable druidTable = new DruidTable(new DruidSchema(address, address, false),
          dataSource, RelDataTypeImpl.proto(rowType), metrics, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
          intervals, null, null);
      final TableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          optTable, viewTable.getTableName(), null, false, false);
      tableRel = DruidQuery.create(cluster, cluster.traitSetOf(BindableConvention.INSTANCE),
          optTable, druidTable, ImmutableList.<RelNode>of(scan), ImmutableMap.of());
    } else {
      // Build Hive Table Scan Rel
      RelOptHiveTable optTable = new RelOptHiveTable(null, cluster.getTypeFactory(), fullyQualifiedTabName,
          rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<>(),
          conf, new HashMap<>(), new HashMap<>(), new AtomicInteger());
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

  private enum OpType {
    CREATE, //view just being created
    LOAD // already created view being loaded
  }

}