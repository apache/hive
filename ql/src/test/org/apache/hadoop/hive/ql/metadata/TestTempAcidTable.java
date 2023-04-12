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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.Map;

/**
 * TestTempAcidTable.
 *
 */
public class TestTempAcidTable {
  private static Hive hive;

  @BeforeClass
  public static void setUp() throws Exception {
    hive = Hive.get();
    HiveConf hiveConf = hive.getConf();
    hiveConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, true);
    hiveConf.setVar(ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    hiveConf.setVar(ConfVars.METASTORE_CLIENT_CAPABILITIES, "HIVEFULLACIDWRITE,HIVEMANAGEDINSERTWRITE");
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS,
        "org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer");
    SessionState ss = SessionState.start(hiveConf);
    ss.initTxnMgr(hive.getConf());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Hive.closeCurrent();
  }

  @Test
  public void testTempInsertOnlyTableTranslate() throws Throwable {
    try {
      String dbName = Warehouse.DEFAULT_DATABASE_NAME;
      String tableName = "temp_table";
      hive.dropTable(dbName, tableName);
      Table table = new Table(dbName, tableName);
      table.setInputFormatClass(OrcInputFormat.class);
      table.setOutputFormatClass(OrcOutputFormat.class);
      Map<String, String> params = table.getParameters();
      params.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
      params.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only");
      table.setTemporary(true);
      hive.createTable(table);

      Table table1 = hive.getTable(dbName, tableName);
      assertNotNull(table1);
      assertEquals(tableName, table1.getTableName());
      assertTrue(table.isTemporary());

      Table table2 = hive.getTranslateTableDryrun(table1.getTTable());
      assertNotNull(table2);
      assertEquals(table1.getTTable().getSd().getLocation(), table2.getTTable().getSd().getLocation());

      hive.dropTable(dbName, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTempInsertOnlyAcidTableTranslate() failed");
      throw e;
    }
  }

  @Test
  public void testTempFullAcidTableTranslate() throws Throwable {
    try {
      String dbName = Warehouse.DEFAULT_DATABASE_NAME;
      String tableName = "temp_table";
      hive.dropTable(dbName, tableName);
      Table table = new Table(dbName, tableName);
      table.setInputFormatClass(OrcInputFormat.class);
      table.setOutputFormatClass(OrcOutputFormat.class);
      Map<String, String> params = table.getParameters();
      params.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
      table.setTemporary(true);
      hive.createTable(table);

      Table table1 = hive.getTable(dbName, tableName);
      assertNotNull(table1);
      assertEquals(tableName, table1.getTableName());
      assertTrue(table.isTemporary());

      Table table2 = hive.getTranslateTableDryrun(table1.getTTable());
      assertNotNull(table2);
      assertEquals(table1.getTTable().getSd().getLocation(), table2.getTTable().getSd().getLocation());

      hive.dropTable(dbName, tableName);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTempFullAcidTableTranslate() failed");
      throw e;
    }
  }
}
