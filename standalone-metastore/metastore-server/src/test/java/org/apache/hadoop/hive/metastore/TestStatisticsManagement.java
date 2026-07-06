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

import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
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
 * Unit tests for {@link StatisticsManagementTask}, verifying that expired table-level and
 * partition-level column statistics are deleted on schedule, and that tables marked with the
 * exclude property are left untouched.
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
    MetastoreConf.setTimeVar(conf, ConfVars.COLUMN_STATISTICS_MANAGEMENT_TASK_FREQUENCY, 1, TimeUnit.DAYS);
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
   * Verifies that expired partition-level column statistics are deleted when the task runs.
   */
  @Test
  public void testExpiredPartitionColStatsAreDeleted() throws Exception {
    String dbName = "stats_db4";
    String tableName = "tbl4";
    String partVal = "p1";
    createDbAndPartitionedTable(dbName, tableName, false);
    createPartition(dbName, tableName, partVal);

    String partName = "part_col=" + partVal;
    writePartitionLevelColStats(dbName, tableName, partName, "c1");
    assertHasPartitionColStats(dbName, tableName, partName, "c1");
    makeAllPartitionColStatsOlderThanRetention(dbName, tableName);

    runStatisticsManagementTask(conf);

    assertNoPartitionColStats(dbName, tableName, partName, "c1");
  }

  /**
   * Verifies that partition-level column statistics are not deleted when the table has the
   * exclude property set to {@code "true"}.
   */
  @Test
  public void testExcludedTablePartitionStatsAreNotDeleted() throws Exception {
    String dbName = "stats_db5";
    String tableName = "tbl5";
    String partVal = "p1";
    createDbAndPartitionedTable(dbName, tableName, true);
    createPartition(dbName, tableName, partVal);

    String partName = "part_col=" + partVal;
    writePartitionLevelColStats(dbName, tableName, partName, "c1");
    assertHasPartitionColStats(dbName, tableName, partName, "c1");
    makeAllPartitionColStatsOlderThanRetention(dbName, tableName);

    runStatisticsManagementTask(conf);

    // Stats must still be present because the table is marked as excluded.
    assertHasPartitionColStats(dbName, tableName, partName, "c1");
  }

  /**
   * Verifies that fresh table-level column statistics whose {@code lastAnalyzed} is within the
   * retention period are not deleted when the task runs.
   */
  @Test
  public void testFreshTableColStatsAreNotDeleted() throws Exception {
    String dbName = "stats_db6";
    String tableName = "tbl6";
    createDbAndTable(dbName, tableName, false);

    // Write stats but do NOT backdate lastAnalyzed — they are within the retention period.
    writeTableLevelColStats(dbName, tableName, "c1");
    assertHasTableColStats(dbName, tableName, "c1");

    runStatisticsManagementTask(conf);

    // Stats are fresh and must not be deleted.
    assertHasTableColStats(dbName, tableName, "c1");
  }

  /**
   * Verifies that fresh partition-level column statistics whose {@code lastAnalyzed} is within the
   * retention period are not deleted when the task runs.
   */
  @Test
  public void testFreshPartitionColStatsAreNotDeleted() throws Exception {
    String dbName = "stats_db7";
    String tableName = "tbl7";
    String partVal = "p1";
    createDbAndPartitionedTable(dbName, tableName, false);
    createPartition(dbName, tableName, partVal);

    // Write stats but do NOT backdate lastAnalyzed — they are within the retention period.
    String partName = "part_col=" + partVal;
    writePartitionLevelColStats(dbName, tableName, partName, "c1");
    assertHasPartitionColStats(dbName, tableName, partName, "c1");

    runStatisticsManagementTask(conf);

    // Stats are fresh and must not be deleted.
    assertHasPartitionColStats(dbName, tableName, partName, "c1");
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
   * Creates a database and a partitioned test table with one partition key {@code part_col}.
   *
   * @param dbName    name of the database to create
   * @param tableName name of the table to create
   * @param exclude   if {@code true}, sets the auto-deletion exclude property on the table
   */
  private void createDbAndPartitionedTable(String dbName, String tableName,
                                           boolean exclude) throws Exception {
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(DEFAULT_CATALOG_NAME)
        .create(client, conf);

    TableBuilder tb = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("c1", "double")
        .addCol("c2", "string")
        .addPartCol("part_col", "string")
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
   * Adds a single partition with the given value for {@code part_col} to the specified table.
   *
   * @param dbName    database name
   * @param tableName table name
   * @param partVal   value for the {@code part_col} partition key
   */
  private void createPartition(String dbName, String tableName, String partVal) throws Exception {
    Table t = client.getTable(dbName, tableName);
    Partition partition = new PartitionBuilder()
        .inTable(t)
        .addValue(partVal)
        .build(conf);
    client.add_partition(partition);
  }

  /**
   * Writes minimal table-level column statistics for the given column via the metastore client.
   *
   * @param db  database name
   * @param tbl table name
   * @param col column name
   */
  private void writeTableLevelColStats(String db, String tbl, String col) throws TException {
    ColumnStatisticsObj obj = buildDoubleColStatsObj(col);

    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, db, tbl);
    desc.setCatName(DEFAULT_CATALOG_NAME);

    ColumnStatistics cs = new ColumnStatistics();
    cs.setStatsDesc(desc);
    cs.addToStatsObj(obj);

    client.updateTableColumnStatistics(cs);
  }

  /**
   * Writes minimal partition-level column statistics for the given column via the metastore client.
   *
   * @param db       database name
   * @param tbl      table name
   * @param partName partition name (e.g. {@code "part_col=p1"})
   * @param col      column name
   */
  private void writePartitionLevelColStats(String db, String tbl, String partName,
                                           String col) throws TException {
    ColumnStatisticsObj obj = buildDoubleColStatsObj(col);

    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(false, db, tbl);
    desc.setCatName(DEFAULT_CATALOG_NAME);
    desc.setPartName(partName);

    ColumnStatistics cs = new ColumnStatistics();
    cs.setStatsDesc(desc);
    cs.addToStatsObj(obj);

    client.updatePartitionColumnStatistics(cs);
  }

  /**
   * Builds a minimal {@link ColumnStatisticsObj} for a double-typed column.
   *
   * @param col column name
   * @return a populated {@link ColumnStatisticsObj}
   */
  private ColumnStatisticsObj buildDoubleColStatsObj(String col) {
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setNumNulls(0);
    doubleData.setNumDVs(1);
    doubleData.setLowValue(1.0);
    doubleData.setHighValue(1.0);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setDoubleStats(doubleData);

    ColumnStatisticsObj obj = new ColumnStatisticsObj();
    obj.setColName(col);
    obj.setColType("double");
    obj.setStatsData(data);
    return obj;
  }

  /**
   * Asserts that at least one column statistics object exists for the specified column.
   *
   * @param db  database name
   * @param tbl table name
   * @param col column name
   */
  private void assertHasTableColStats(String db, String tbl, String col) throws TException {
    List<ColumnStatisticsObj> objs =
        client.getTableColumnStatistics(db, tbl, Collections.singletonList(col), "hive");
    assertTrue("Expected stats for " + db + "." + tbl + "." + col,
        objs != null && !objs.isEmpty());
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
      List<ColumnStatisticsObj> objs =
          client.getTableColumnStatistics(db, tbl, Collections.singletonList(col), "hive");
      assertTrue("Expected no stats for " + db + "." + tbl + "." + col,
          objs == null || objs.isEmpty());
    } catch (NoSuchObjectException e) {
      // Acceptable: server may throw if stats are absent depending on implementation.
    }
  }

  /**
   * Asserts that at least one partition-level column statistics object exists for the specified
   * column and partition.
   *
   * @param db       database name
   * @param tbl      table name
   * @param partName partition name
   * @param col      column name
   */
  private void assertHasPartitionColStats(String db, String tbl, String partName,
                                          String col) throws TException {
    Map<String, List<ColumnStatisticsObj>> statsMap = client.getPartitionColumnStatistics(
        db, tbl, Collections.singletonList(partName), Collections.singletonList(col), "hive");
    List<ColumnStatisticsObj> objs = statsMap == null ? null : statsMap.get(partName);
    assertTrue("Expected partition stats for " + db + "." + tbl + "/" + partName + "." + col,
        objs != null && !objs.isEmpty());
  }

  /**
   * Asserts that no partition-level column statistics exist for the specified column and partition.
   * A {@link NoSuchObjectException} from the server is also treated as an acceptable absence signal.
   *
   * @param db       database name
   * @param tbl      table name
   * @param partName partition name
   * @param col      column name
   */
  private void assertNoPartitionColStats(String db, String tbl, String partName,
                                         String col) throws TException {
    try {
      Map<String, List<ColumnStatisticsObj>> statsMap = client.getPartitionColumnStatistics(
          db, tbl, Collections.singletonList(partName), Collections.singletonList(col), "hive");
      List<ColumnStatisticsObj> objs = statsMap == null ? null : statsMap.get(partName);
      assertTrue("Expected no partition stats for " + db + "." + tbl + "/" + partName + "." + col,
          objs == null || objs.isEmpty());
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
    ObjectStore os = new ObjectStore();
    os.setConf(conf);

    long oldSeconds = (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(400)) / 1000;
    PersistenceManager pm = os.getPersistenceManager();
    try (Query q = pm.newQuery(MTableColumnStatistics.class)) {
      q.setFilter("table.tableName == t && table.database.name == d"
          + " && table.database.catalogName == c");
      q.declareParameters("java.lang.String t, java.lang.String d, java.lang.String c");
      @SuppressWarnings("unchecked")
      List<MTableColumnStatistics> rows =
          (List<MTableColumnStatistics>) q.execute(tbl, db, DEFAULT_CATALOG_NAME);
      for (MTableColumnStatistics r : rows) {
        r.setLastAnalyzed(oldSeconds);
      }
      pm.flush();
    } finally {
      pm.close();
    }
  }

  /**
   * Backdates the {@code lastAnalyzed} field of all {@code MPartitionColumnStatistics} rows for
   * the given table to 400 days ago. Uses a fresh {@link ObjectStore} instance directly.
   *
   * @param db  database name
   * @param tbl table name
   */
  private void makeAllPartitionColStatsOlderThanRetention(String db, String tbl) throws Exception {
    ObjectStore os = new ObjectStore();
    os.setConf(conf);

    long oldSeconds = (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(400)) / 1000;
    PersistenceManager pm = os.getPersistenceManager();
    try (Query q = pm.newQuery(MPartitionColumnStatistics.class)) {
      q.setFilter("partition.table.tableName == t && partition.table.database.name == d"
          + " && partition.table.database.catalogName == c");
      q.declareParameters("java.lang.String t, java.lang.String d, java.lang.String c");
      @SuppressWarnings("unchecked")
      List<MPartitionColumnStatistics> rows =
          (List<MPartitionColumnStatistics>) q.execute(tbl, db, DEFAULT_CATALOG_NAME);
      for (MPartitionColumnStatistics r : rows) {
        r.setLastAnalyzed(oldSeconds);
      }
      pm.flush();
    } finally {
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
