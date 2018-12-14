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

package org.apache.hadoop.hive.metastore;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestObjectStoreStatementVerify {
  private ObjectStore objectStore = null;
  private Configuration conf = null;

  private final String DB1 = "db1";
  private final String TBL1 = "db1_tbl1";

  @BeforeClass
  public static void oneTimeSetup() throws SQLException {
    DriverManager.registerDriver(new StatementVerifyingDerby());
  }

  private ObjectStore createObjectStore() throws MetaException, InvalidOperationException {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER, StatementVerifyingDerby.class.getName());
    String jdbcUrl = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    jdbcUrl = jdbcUrl.replace("derby","sderby");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY,jdbcUrl);
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    final ObjectStore objectStore = new ObjectStore();
    objectStore.setConf(conf);
    HiveMetaStore.HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
    return objectStore;
  }

  // This function is called during the prepare statement step of object retrieval through DN
  static void verifySql(final String sql) {
    if (sql.contains("SELECT DISTINCT 'org.apache.hadoop.hive.metastore.model.MTable' AS") ||
        sql.contains("SELECT 'org.apache.hadoop.hive.metastore.model.MTable' AS")) {
      verifyMTableDBFetchGroup(sql);
    }
  }

  private static void verifyMTableDBFetchGroup(final String sql) {
    // We want to validate that when an MTable is fetched, we join the DBS table and fetch
    // the database info as well. For example, if we don't use a proper fetch plan, the DN query
    // would be something like
    //
    // SELECT DISTINCT 'org.apache.hadoop.hive.metastore.model.MTable' AS
    //   NUCLEUS_TYPE, A0.CREATE_TIME, A0.TBL_ID, A0.LAST_ACCESS_TIME, A0.OWNER, A0.OWNER_TYPE,
    //   A0.RETENTION, A0.IS_REWRITE_ENABLED, A0.TBL_NAME, A0.TBL_TYPE, A0.WRITE_ID
    //   FROM TBLS A0
    //   LEFT OUTER JOIN DBS B0 ON A0.DB_ID = B0.DB_ID WHERE B0.CTLG_NAME = ?
    //
    // Note in the above query that we never pick anything from the DBS table!
    //
    // If you have a good fetch plan, your query should be something like
    //
    // SELECT DISTINCT 'org.apache.hadoop.hive.metastore.model.MTable' AS
    //   NUCLEUS_TYPE, A0.CREATE_TIME, C0.CTLG_NAME, C0."DESC", C0.DB_LOCATION_URI, C0."NAME",
    //   C0.OWNER_NAME, C0.OWNER_TYPE, C0.DB_ID, A0.TBL_ID, A0.LAST_ACCESS_TIME, A0.OWNER,
    //   A0.OWNER_TYPE, A0.RETENTION, A0.IS_REWRITE_ENABLED, A0.TBL_NAME, A0.TBL_TYPE, A0.WRITE_ID
    //   FROM TBLS A0 LEFT OUTER JOIN DBS B0 ON A0.DB_ID = B0.DB_ID
    //   LEFT OUTER JOIN DBS C0 ON A0.DB_ID = C0.DB_ID WHERE B0.CTLG_NAME = ?
    //
    // Notice that we pick the DB_ID, OWNER_TYPE, NAME, DESC etc from the DBS table. This is the
    // correct behavior.

    // Step 1. Find the identifiers for the DBS database by matching on "JOIN DBS (xx) ON"
    Pattern sqlPatternDb = Pattern.compile("JOIN\\ DBS\\ ([a-zA-Z0-9]+)\\ ON");
    Matcher matcher = sqlPatternDb.matcher(sql);
    List<String> dbIdentifiers = new ArrayList<>();
    while (matcher.find()) {
      dbIdentifiers.add(matcher.group(1));
    }
    // Step 2. Now there should a string with the db identifier which picks the NAME field from
    // databases. If we don't find this, then we did not join in the database info.
    boolean confirmedDbNameRetrieval = false;
    for (String dbIdenfier : dbIdentifiers) {
      if (sql.contains(dbIdenfier + ".\"NAME\"")) {
        confirmedDbNameRetrieval = true;
        break;
      }
    }
    Assert.assertTrue("The Db info should be retrieved as part of MTable fetch", confirmedDbNameRetrieval);
  }

  @Test
  public void testGetTableMetaFetchGroup() throws MetaException, InvalidObjectException,
      InvalidOperationException {
    objectStore = createObjectStore();

    Database db = new Database(DB1, "description", "locurl", null);
    db.setCatalogName("hive");

    objectStore.createDatabase(db);
    objectStore.createTable(makeTable(DB1, TBL1));

    List<TableMeta> tableMeta = objectStore.getTableMeta("hive", "*", "*", Collections.emptyList());
    Assert.assertEquals("Number of items for tableMeta is incorrect", 1, tableMeta.size());
    Assert.assertEquals("Table name incorrect", TBL1, tableMeta.get(0).getTableName());
    Assert.assertEquals("Db name incorrect", DB1, tableMeta.get(0).getDbName());
  }

  private Table makeTable(String dbName, String tblName) throws MetaException {
    return new TableBuilder()
        .setTableName(tblName)
        .setDbName(dbName)
        .setOwner("owner")
        .setCreateTime(1)
        .setLastAccessTime(2)
        .setRetention(3)
        .addTableParam("EXTERNAL", "false")
        .setType("MANAGED_TABLE")
        .setLocation("location")
        .setCompressed(false)
        .setNumBuckets(0)
        .setSerdeName("SerDeName")
        .setSerdeLib("serializationLib")
        .addCol("some_col", "double", null)
        .build(conf);
  }
}