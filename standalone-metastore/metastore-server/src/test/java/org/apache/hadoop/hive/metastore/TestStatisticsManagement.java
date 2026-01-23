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
import static org.junit.Assert.assertEquals;
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
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

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

        // enable stats auto deletion, set up a short retention so threshold check triggers easily
        MetastoreConf.setBoolVar(conf, ConfVars.STATISTICS_AUTO_DELETION, true);
        MetastoreConf.setTimeVar(conf, ConfVars.STATISTICS_RETENTION_PERIOD, 1, TimeUnit.DAYS);

        MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
        TestTxnDbUtil.setConfValues(conf);
        TestTxnDbUtil.prepDb(conf);

        client = new HiveMetaStoreClient(conf);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            // Drop any leftover databases, similar to TestPartitionManagement.java
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
        // create a column stats entry (table-level)
        writeTableLevelColStats(dbName, tableName, "c1");
        // ensure stats exists
        assertHasTableColStats(dbName, tableName, "c1");
        // make lastAnalyzed older than the threshold
        makeAllTableColStatsOlderThanRetention(dbName, tableName);

        runStatisticsManagementTask(conf);
        assertNoTableColStats(dbName, tableName, "c1");
    }

    @Test
    public void testExcludedTableStatsAreNotDeleted() throws Exception {
        String dbName = "stats_db2";
        String tableName = "tbl2";
        // Create a database and table that ARE excluded from auto deletion.
        createDbAndTable(dbName, tableName, true);
        writeTableLevelColStats(dbName, tableName, "c1");
        assertHasTableColStats(dbName, tableName, "c1");

        // Manually set lastAnalyzed to a very old timestamp so it would normally be expired.
        makeAllTableColStatsOlderThanRetention(dbName, tableName);

        runStatisticsManagementTask(conf);

        // Verify that stats are still present because the table is excluded.
        assertHasTableColStats(dbName, tableName, "c1");
    }

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

        // Build a simple test table with two columns.
        TableBuilder tb = new TableBuilder()
                .inDb(db)
                .setTableName(tableName)
                .addCol("c1", "double")
                .addCol("c2", "string")
                .setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
                .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");

        Table t = tb.build(conf);

        // If requested, mark this table as excluded from automatic stats deletion.
        if (exclude) {
            t.getParameters().put(StatisticsManagementTask.STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY, "true");
        }
        client.createTable(t);
        client.flushCache();
    }

    private void writeTableLevelColStats(String db, String tbl, String col) throws TException {
        // Create a stats object for one column.
        ColumnStatisticsObj obj = new ColumnStatisticsObj();
        obj.setColName(col);
        obj.setColType("double");

        // Fill in minimal double-column statistics data.
        DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
        doubleData.setNumNulls(0);
        doubleData.setNumDVs(1);
        doubleData.setLowValue(1.0);
        doubleData.setHighValue(1.0);

        ColumnStatisticsData data = new ColumnStatisticsData();
        data.setDoubleStats(doubleData);
        obj.setStatsData(data);

        ColumnStatistics cs = new ColumnStatistics();
        ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, db, tbl);
        desc.setCatName("hive");
        cs.addToStatsObj(obj);

        client.updateTableColumnStatistics(cs);
    }

    private void assertHasTableColStats(String db, String tbl, String col) throws TException {
        List<ColumnStatisticsObj> objs = client.getTableColumnStatistics(db, tbl, List.of(col), "hive");
        assertTrue("Expected stats for " + db + "." + tbl + "." + col, objs != null && !objs.isEmpty());
    }

    private void assertNoTableColStats(String db, String tbl, String col) throws TException {
        try {
            List<ColumnStatisticsObj> objs = client.getTableColumnStatistics(db, tbl, List.of(col), "hive");
            assertTrue("Expected no stats for " + db + "." + tbl + "." + col, objs == null || objs.isEmpty());
        } catch (NoSuchObjectException e) {
            // acceptable: server may throw if stats absent depending on impl
        }
    }

    private void makeAllTableColStatsOlderThanRetention(String db, String tbl) throws Exception {
        // We update via ObjectStore/PM directly to avoid relying on params "lastAnalyzed".
        RawStore ms = HMSHandler.getMSForConf(conf);
        ObjectStore os = (ObjectStore) ms;
        os.setConf(conf);

        // Compute an old timestamp in seconds, here we use 400 days ago.
        long oldSeconds = (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(400)) / 1000;

        // NOTE: exact JDO classes/field paths sometimes vary; adjust filter if needed based on MTableColumnStatistics mapping
        PersistenceManager pm = os.getPersistenceManager();
        Query q = null;
        try {
            // Query MTableColumnStatistics rows for the target table.
            q = pm.newQuery(org.apache.hadoop.hive.metastore.model.MTableColumnStatistics.class);
            q.setFilter("table.tableName == t && table.database.name == d");
            q.declareParameters("java.lang.String t, java.lang.String d");
            @SuppressWarnings("unchecked")
            List<org.apache.hadoop.hive.metastore.model.MTableColumnStatistics> rows =
                    (List<org.apache.hadoop.hive.metastore.model.MTableColumnStatistics>) q.execute(tbl, db);
            // Make all matching column stats rows to look old/expired.
            for (org.apache.hadoop.hive.metastore.model.MTableColumnStatistics r : rows) {
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

    private void runStatisticsManagementTask(Configuration conf) {
        StatisticsManagementTask task = new StatisticsManagementTask();
        task.setConf(conf);
        task.run();
    }
}
