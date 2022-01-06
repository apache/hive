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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.lockmgr.TestDbTxnManager2;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.orc.OrcProto;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

//import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

/**
 * This class resides in itests to facilitate running query using Tez engine, since the jars are
 * fully loaded here, which is not the case if it stays in ql.
 */
public class TestDatabaseTableDefault {
    static final private Logger LOG = LoggerFactory.getLogger(TestDatabaseTableDefault.class);

    private HiveConf hiveConf;
    private IDriver d;
    private String database_with_default_table_type = "database_with_default_table_type";
    private String default_db = "default_db";
    private String table_type_managed = "MANAGED_TABLE";
    private String table_type_external = "EXTERNAL_TABLE";
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
        EXTERNALTABLE1("external_table_1"),
        EXTERNALTABLE2("external_table_2"),
        EXTERNALTABLE3("external_table_3"),
        EXTERNALTABLE4("external_table_4"),
        EXTERNALTABLE5("external_table_5"),
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
        hiveConf = new HiveConf(this.getClass());
        HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.CREATE_TABLES_AS_ACID, true);
        HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, true);
        HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
        hiveConf.set(HiveConf.ConfVars.HIVEDEFAULTMANAGEDFILEFORMAT.varname, "ORC");
        hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());
        hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_CAPABILITIES.varname, "HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,HIVEMANAGESTATS,HIVEMANAGEDINSERTWRITE,HIVEMANAGEDINSERTREAD");
        TestTxnDbUtil.setConfValues(hiveConf);
        TestTxnDbUtil.prepDb(hiveConf);

        SessionState.start(new SessionState(hiveConf));
        d = DriverFactory.newDriver(hiveConf);

        conf = MetastoreConf.newMetastoreConf();
        wh = new File(System.getProperty("java.io.tmpdir") + File.separator +
                "hive" + File.separator + "warehouse" + File.separator + "hive" + File.separator);
        wh.mkdirs();

        ext_wh = new File(System.getProperty("java.io.tmpdir") + File.separator +
                "hive" + File.separator + "warehouse" + File.separator + "hive-external" + File.separator);
        ext_wh.mkdirs();

        MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS,
                "org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer");
        MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, false);
        MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, wh.getCanonicalPath());
        MetastoreConf.setVar(conf, ConfVars.WAREHOUSE_EXTERNAL, ext_wh.getCanonicalPath());
        MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
        client = new HiveMetaStoreClient(conf);


        dropTables();
        runStatementOnDriver("create database " + database_with_default_table_type +" with DBPROPERTIES('defaultTableType'='EXTERNAL')");
        runStatementOnDriver("create database " + default_db);
    }

    /**
     * this is to test differety types of Acid tables
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
     * Tests the TestDatabaseTableDefault.testCreateManagedTables method for creating managed tables in a special database.
     * @throws Exception If there is an error creating managed tables
     */
    @Test
    public void testCreateManagedTables() throws Exception {
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

        LOG.info("Test execution complete:testCreateManagedTables");
    }


    /**
     * Tests the TestDatabaseTableDefault.testCreateExternalTables method for creating external tables in a special database.
     * @throws Exception If there is an error creating external tables
     */
    @Test
    public void testCreateExternalTables() throws Exception {
        runStatementOnDriver("use " + database_with_default_table_type);

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE1 + " (id int, name string)");
        Table external_table_1 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE1.toString());
        assertEquals("Created table type is expected to be managed but found to be external", external_table_1.getTableType(), table_type_external);

        runStatementOnDriver("create external table " + TableNames.EXTERNALTABLE2 + " (id int, name string) ");
        Table external_table_2 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE2.toString());
        assertEquals("Created table type is expected to be managed but found to be external", external_table_2.getTableType(), table_type_external);

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE3 + " like " + TableNames.EXTERNALTABLE2);
        Table external_table_3 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE3.toString());
        assertEquals("Created table type is expected to be managed but found to be external", external_table_3.getTableType(), table_type_external);

        runStatementOnDriver("create table " + TableNames.EXTERNALTABLE4 + " as select * from " + TableNames.EXTERNALTABLE2);
        Table external_table_4 = client.getTable(database_with_default_table_type, TableNames.EXTERNALTABLE4.toString());
        assertEquals("Created table type is expected to be managed but found to be external", external_table_4.getTableType(), table_type_external);

        LOG.info("Test execution complete:testCreateExternalTables");
    }

    /**
     * Tests the TestDatabaseTableDefault.testAlterExternalTables method for creating external tables in alter database.
     * @throws Exception If there is an error creating managed tables
     */
    @Test
    public void testAlterExternalTables() throws Exception {
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
