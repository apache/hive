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

package org.apache.hadoop.hive.ql;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class resides in itests to facilitate running query using Tez engine, since the jars are
 * fully loaded here, which is not the case if it stays in ql.
 */
public class TestDatabaseTableDefault {
    static final private Logger LOG = LoggerFactory.getLogger(TestDatabaseTableDefault.class);

    private HiveConf hiveConf;
    private IDriver d;
    private static final String database_with_default_table_type = "database_with_default_table_type";
    private static final String default_db = "default_db";
    private static final String table_type_managed = "MANAGED_TABLE";
    private static final String table_type_external = "EXTERNAL_TABLE";
    File ext_wh = null;
    File wh = null;
    protected static HiveMetaStoreClient client;
    protected static Configuration conf;
    private static enum TableNames {
        TRANSACTIONALTBL1("transactional_table_1"),
        TRANSACTIONALTBL2("transactional_table_2"),
        TRANSACTIONALTBL3("transactional_table_3"),
        TRANSACTIONALTBL4("transactional_table_4"),
        TRANSACTIONALTBL5("transactional_table_5"),
        TRANSACTIONALTBL6("transactional_table_6"),
        TRANSACTIONALTBL7("transactional_table_7"),
        EXTERNALTABLE1("external_table_1"),
        EXTERNALTABLE2("external_table_2"),
        EXTERNALTABLE3("external_table_3"),
        EXTERNALTABLE4("external_table_4"),
        EXTERNALTABLE5("external_table_5"),
        EXTERNALTABLE6("external_table_6"),
        NONACIDPART("nonAcidPart"),
        NONACIDNONBUCKET("nonAcidNonBucket");

        private final String name;
        @Override
        public String toString() {
            return name;
        }
        TableNames(String name) {
            this.name = name;
        }
    }

    @Before
    public void setUp() throws Exception {
        hiveConf = new HiveConfForTest(getClass());
        HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.CREATE_TABLES_AS_ACID, true);
        HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, true);
        HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
        hiveConf.set(HiveConf.ConfVars.HIVE_DEFAULT_MANAGED_FILEFORMAT.varname, "ORC");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());
        hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_CAPABILITIES.varname, "HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD");
        TestTxnDbUtil.setConfValues(hiveConf);
        TestTxnDbUtil.prepDb(hiveConf);

        SessionState.start(new SessionState(hiveConf));
        d = DriverFactory.newDriver(hiveConf);

        wh = new File(System.getProperty("java.io.tmpdir") + File.separator +
                "hive" + File.separator + "warehouse" + File.separator + "hive" + File.separator);
        wh.mkdirs();

        ext_wh = new File(System.getProperty("java.io.tmpdir") + File.separator +
                "hive" + File.separator + "warehouse" + File.separator + "hive-external" + File.separator);
        ext_wh.mkdirs();

        MetastoreConf.setVar(hiveConf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS,
                "org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer");
        MetastoreConf.setBoolVar(hiveConf, ConfVars.HIVE_IN_TEST, false);
        MetastoreConf.setVar(hiveConf, ConfVars.WAREHOUSE, wh.getCanonicalPath());
        MetastoreConf.setVar(hiveConf, ConfVars.WAREHOUSE_EXTERNAL, ext_wh.getCanonicalPath());
        MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
        client = new HiveMetaStoreClient(hiveConf);

        dropTables();
        runStatementOnDriver("create database " + database_with_default_table_type +" with DBPROPERTIES('defaultTableType'='EXTERNAL')");
        runStatementOnDriver("create database " + default_db);
    }

    /**
     * this is to test different types of Acid tables
     */
    String getTblProperties() {
        return "TBLPROPERTIES ('transactional'='true')";
    }

    private void dropTables() throws Exception {
        for(TableNames t : TableNames.values()) {
            runStatementOnDriver("drop table if exists " + t);
        }
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (d != null) {
                dropTables();
                runStatementOnDriver("drop database if exists " + database_with_default_table_type +" cascade");
                runStatementOnDriver("drop database if exists " + default_db + " cascade");
                d.close();
                d.destroy();
                d = null;
            }
        } finally {
            FileUtils.deleteDirectory(wh);
            FileUtils.deleteDirectory(ext_wh);
        }
    }

    /**
     * Tests the TestDatabaseTableDefault.testCreateManagedTablesInDBWithDefaultTableType_External method for creating managed tables in a special database.
     * @throws Exception If there is an error creating managed tables
     */
    @Test
    public void testCreateManagedTablesInDBWithDefaultTableType_External() throws Exception {
        runStatementOnDriver("use " + database_with_default_table_type);

        runStatementOnDriver("create table " + TableNames.TRANSACTIONALTBL1 + " (id int, name string) " + getTblProperties());
        Table managed_table_1 = client.getTable(database_with_default_table_type, TableNames.TRANSACTIONALTBL1.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_1.getTableType(), table_type_managed);

        runStatementOnDriver("create managed table " + TableNames.TRANSACTIONALTBL2 + " (id int, name string) ");
        Table managed_table_2 = client.getTable(database_with_default_table_type, TableNames.TRANSACTIONALTBL2.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_2.getTableType(), table_type_managed);

        runStatementOnDriver("create transactional table " + TableNames.TRANSACTIONALTBL3 + " (id int, name string) ");
        Table managed_table_3 = client.getTable(database_with_default_table_type, TableNames.TRANSACTIONALTBL3.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_3.getTableType(), table_type_managed);

        runStatementOnDriver("create transactional table " + TableNames.TRANSACTIONALTBL4 + " like " + TableNames.TRANSACTIONALTBL3);
        Table managed_table_4 = client.getTable(database_with_default_table_type, TableNames.TRANSACTIONALTBL4.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_4.getTableType(), table_type_managed);

        runStatementOnDriver("create transactional table " + TableNames.TRANSACTIONALTBL5 + " as select * from " + TableNames.TRANSACTIONALTBL3);
        Table managed_table_5 = client.getTable(database_with_default_table_type, TableNames.TRANSACTIONALTBL5.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_5.getTableType(), table_type_managed);

        LOG.info("Test execution complete:testCreateManagedTablesInDBWithDefaultTableType_External");
    }


    /**
     * Tests the TestDatabaseTableDefault.testCreateExternalTablesInDBWithDefaultTableType_External method for creating external tables in a special database.
     * @throws Exception If there is an error creating external tables
     */
    @Test
    public void testCreateExternalTablesInDBWithDefaultTableType_External() throws Exception {
        runStatementOnDriver("use " + database_with_default_table_type);

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE1 + " (id int, name string)");
        Table external_table_1 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE1.toString());
        assertEquals("Created table type is expected to be external but found to be managed", external_table_1.getTableType(), table_type_external);

        runStatementOnDriver("create external table " + TableNames.EXTERNALTABLE2 + " (id int, name string) ");
        Table external_table_2 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE2.toString());
        assertEquals("Created table type is expected to be external but found to be managed", external_table_2.getTableType(), table_type_external);

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE3 + " like " + TableNames.EXTERNALTABLE2);
        Table external_table_3 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE3.toString());
        assertEquals("Created table type is expected to be external but found to be managed", external_table_3.getTableType(), table_type_external);

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE4 + " as select * from " + TableNames.EXTERNALTABLE2);
        Table external_table_4 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE4.toString());
        assertEquals("Created table type is expected to be external but found to be managed", external_table_4.getTableType(), table_type_external);

        LOG.info("Test execution complete:testCreateExternalTablesInDBWithDefaultTableType_External");
    }

    /**
     * Tests the TestDatabaseTableDefault.testCreateTablesInAlterDBSetDefaultTableType method for creating external tables in altered database.
     * @throws Exception If there is an error creating managed tables
     */
    @Test
    public void testCreateTablesInAlterDBSetDefaultTableType() throws Exception {
        String altered_db = "altered_db";
        runStatementOnDriver("create database "+altered_db);
        runStatementOnDriver("use "+altered_db);

        runStatementOnDriver("create table " + TableNames.TRANSACTIONALTBL6 + " (id int, name string)");
        Table managed_table_6 = client.getTable(altered_db, TableNames.TRANSACTIONALTBL6.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_6.getTableType(), table_type_managed);

        runStatementOnDriver("alter database " + altered_db + " set DBPROPERTIES (\"defaultTableType\"=\"EXTERNAL\")");

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE5 + " (id int, name string)");
        Table external_table_5 = client.getTable(altered_db, TableNames.EXTERNALTABLE5.toString());
        assertEquals("Created table type is expected to be managed but found to be external", external_table_5.getTableType(), table_type_external);

        runStatementOnDriver("drop database " + altered_db + " cascade");
        LOG.info("Test execution complete:testAlterExternalTables");
    }

    /**
     * Tests the TestDatabaseTableDefault.testCreateTablesInDBWithDefaultTableTypeAcid method for creating acid tables in the database by default.
     * @throws Exception If there is an error creating managed tables
     */
    @Test
    public void testCreateTablesInDBWithDefaultTableTypeAcid() throws Exception {
        String acid_database = "acid_database";
        runStatementOnDriver("create database "+acid_database+" with DBPROPERTIES('defaultTableType'='ACID')");
        runStatementOnDriver("use "+acid_database);

        runStatementOnDriver("create table " + TableNames.TRANSACTIONALTBL7 + " (id int, name string)");
        Table managed_table_7 = client.getTable(acid_database, TableNames.TRANSACTIONALTBL7.toString());
        assertEquals("Created table type is expected to be managed but found to be external", managed_table_7.getTableType(), table_type_managed);

        runStatementOnDriver("create external table " + TableNames.EXTERNALTABLE6 + " (id int, name string)");
        Table external_table_6 = client.getTable(acid_database, TableNames.EXTERNALTABLE6.toString());
        assertEquals("Created table type is expected to be external but found to be managed", external_table_6.getTableType(), table_type_external);

        runStatementOnDriver("drop database " + acid_database + " cascade");
        LOG.info("Test execution complete:testCreateTablesInDBWithDefaultTableTypeAcid");
    }


    private void restartSessionAndDriver(HiveConf conf) throws Exception {
        SessionState ss = SessionState.get();
        if (ss != null) {
            ss.close();
        }
        if (d != null) {
            d.close();
            d.destroy();
        }

        SessionState.start(conf);
        d = DriverFactory.newDriver(conf);
    }

    private List<String> runStatementOnDriver(String stmt) throws Exception {
        d.run(stmt);
        List<String> rs = new ArrayList<String>();
        d.getResults(rs);
        return rs;
    }

    /**
     * Run statement with customized hive conf
     */
    public static List<String> runStatementOnDriver(String stmt, HiveConf conf)
            throws Exception {
        IDriver driver = DriverFactory.newDriver(conf);
        driver.setMaxRows(10000);
        driver.run(stmt);
        List<String> rs = new ArrayList<String>();
        driver.getResults(rs);
        return rs;
    }
}
