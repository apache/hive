/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Unit tests for {@link StatisticsManagementTask}, verifying that expired table-level column
 * statistics are deleted on schedule and that tables marked with the exclude property are left
 * untouched.
 */
@Category(MetastoreUnitTest.class)
public class TestStatisticsManagement {

  private IMetaStoreClient client;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    conf.setBoolean(ConfVars.MULTITHREADED.getVarname(), false);
    conf.setBoolean(ConfVars.HIVE_IN_TEST.getVarname(), true);

    // Enable stats auto deletion with a short retention so the threshold check triggers easily.
    MetastoreConf.setBoolVar(conf, ConfVars.COLUMN_STATISTICS_AUTO_DELETION, true);
    MetastoreConf.setTimeVar(conf, ConfVars.COLUMN_STATISTICS_RETENTION_PERIOD, 1, TimeUnit.DAYS);

    MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);

    client = new HiveMetaStoreClient(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (client != null) {
      // Drop any leftover databases, similar to TestPartitionManagement.java.
      List<String> dbs = client.getAllDatabases(DEFAULT_CATALOG_NAME);
      for (String db : dbs) {
        if (!db.equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
          client.dropDatabase(DEFAULT_CATALOG_NAME, db, true, false, true);
        }
      }
    }
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  @Test
  public void testExpiredTableColStatsAreDeleted() throws Exception {
    String dbName = "stats_db1";
    String tableName = "tbl1";
    createDbAndTable(dbName, tableName, false);
    writeTableLevelColStats(dbName, tableName, "c1");
    assertHasTableColStats(dbName, tableName, "c1");
    makeAllTableColStatsOlderThanRetention(dbName, tableName);

    runStatisticsManagementTask(conf);

    assertNoTableColStats(dbName, tableName, "c1");
  }

  @Test
  public void testExcludedTableStatsAreNotDeleted() throws Exception {
    String dbName = "stats_db2";
    String tableName = "tbl2";
    createDbAndTable(dbName, tableName, true);
    writeTableLevelColStats(dbName, tableName, "c1");
    assertHasTableColStats(dbName, tableName, "c1");
    makeAllTableColStatsOlderThanRetention(dbName, tableName);

    runStatisticsManagementTask(conf);

    // Stats must still be present because the table is marked as excluded.
    assertHasTableColStats(dbName, tableName, "c1");
  }

  /**
   * Verifies that setting {@value StatisticsManagementTask#STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY}
   * to {@code "false"} does not prevent deletion. Only an explicit {@code "true"} value excludes
   * a table; any other value (including {@code "false"}) is treated as not excluded.
   */
  @Test
  public void testExcludePropertySetToFalseDoesNotPreventDeletion() throws Exception {
    String dbName = "stats_db3";
    String tableName = "tbl3";
    createDbAndTable(dbName, tableName, false);

    // Explicitly set the exclude property to "false" — the task must still delete these stats.
    Table t = client.getTable(dbName, tableName);
    t.getParameters().put(
        StatisticsManagementTask.STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY, "false");
    client.alter_table(dbName, tableName, t);

    writeTableLevelColStats(dbName, tableName, "c1");
    assertHasTableColStats(dbName, tableName, "c1");
    makeAllTableColStatsOlderThanRetention(dbName, tableName);

    runStatisticsManagementTask(conf);

    // "false" is not a valid exclude signal; stats should be deleted.
    assertNoTableColStats(dbName, tableName, "c1");
  }

  /**
   * Creates a database (unless it is the default database) and a simple two-column test table.
   *
   * @param dbName    name of the database to create
   * @param tableName name of the table to create
   * @param exclude   if {@code true}, sets the auto-deletion exclude property on the table
   */
  private void createDbAndTable(String dbName, String tableName, boolean exclude) throws Exception {
    Database db;
    if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
      db = new DatabaseBuilder()
          .setName(dbName)
          .setCatalogName(DEFAULT_CATALOG_NAME)
          .create(client, conf);
    } else {
      db = client.getDatabase(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
    }

    TableBuilder tb = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("c1", "double")
        .addCol("c2", "string")
        .setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
        .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");

    Table t = tb.build(conf);
    if (exclude) {
      t.getParameters().put(
          StatisticsManagementTask.STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY, "true");
    }
    client.createTable(t);
    client.flushCache();
  }

  /**
   * Writes minimal table-level column statistics for the given column via the metastore client.
   *
   * @param db  database name
   * @param tbl table name
   * @param col column name
   */
  private void writeTableLevelColStats(String db, String tbl, String col) throws TException {
    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(col);
    obj.setColType("double");

    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setNumNulls(0);
    doubleData.setNumDVs(1);
    doubleData.setLowValue(1.0);
    doubleData.setHighValue(1.0);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setDoubleStats(doubleData);
    obj.setStatsData(data);

    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, db, tbl);
    desc.setCatName("hive");

    ColumnStatistics cs = new ColumnStatistics();
    cs.setStatsDesc(desc);
    cs.addToStatsObj(obj);

    client.updateTableColumnStatistics(cs);
  }

  /**
   * Asserts that at least one column statistics object exists for the specified column.
   *
   * @param db  database name
   * @param tbl table name
   * @param col column name
   */
  private void assertHasTableColStats(String db, String tbl, String col) throws TException {
    List<ColumnStatisticsObj> objs = client.getTableColumnStatistics(db, tbl, List.of(col), "hive");
    assertTrue("Expected stats for " + db + "." + tbl + "." + col, objs != null && !objs.isEmpty());
  }

  /**
   * Asserts that no column statistics exist for the specified column.
   * A {@link NoSuchObjectException} from the server is also treated as an acceptable absence signal.
   *
   * @param db  database name
   * @param tbl table name
   * @param col column name
   */
  private void assertNoTableColStats(String db, String tbl, String col) throws TException {
    try {
      List<ColumnStatisticsObj> objs = client.getTableColumnStatistics(db, tbl, List.of(col), "hive");
      assertTrue("Expected no stats for " + db + "." + tbl + "." + col, objs == null || objs.isEmpty());
    } catch (NoSuchObjectException e) {
      // Acceptable: server may throw if stats are absent depending on implementation.
    }
  }

  /**
   * Backdates the {@code lastAnalyzed} field of all {@code MTableColumnStatistics} rows for the
   * given table to 400 days ago, making them appear expired relative to any reasonable retention
   * period. Uses a fresh {@link ObjectStore} instance to bypass the proxy wrapper returned by
   * {@code HMSHandler.getMSForConf()}.
   *
   * @param db  database name
   * @param tbl table name
   */
  private void makeAllTableColStatsOlderThanRetention(String db, String tbl) throws Exception {
    // Instantiate ObjectStore directly; HMSHandler.getMSForConf() returns a proxy that
    // cannot be cast to ObjectStore and does not expose getPersistenceManager().
    ObjectStore os = new ObjectStore();
    os.setConf(conf);

    long oldSeconds = (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(400)) / 1000;
    PersistenceManager pm = os.getPersistenceManager();
    Query q = null;
    try {
      q = pm.newQuery(MTableColumnStatistics.class);
      q.setFilter("table.tableName == t && table.database.name == d");
      q.declareParameters("java.lang.String t, java.lang.String d");
      @SuppressWarnings("unchecked")
      List<MTableColumnStatistics> rows = (List<MTableColumnStatistics>) q.execute(tbl, db);
      for (MTableColumnStatistics r : rows) {
        r.setLastAnalyzed(oldSeconds);
      }
      pm.flush();
    } finally {
      if (q != null) {
        q.closeAll();
      }
      pm.close();
    }
  }

  /**
   * Instantiates and runs a {@link StatisticsManagementTask} with the given configuration.
   *
   * @param configuration the HMS configuration to pass to the task
   */
  private void runStatisticsManagementTask(Configuration configuration) {
    StatisticsManagementTask task = new StatisticsManagementTask();
    task.setConf(configuration);
    task.run();
  }
}
