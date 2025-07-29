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
package org.apache.iceberg.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.apache.hadoop.hive.ql.exec.PartitionUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/* 
  * This test is an integration test for the hive-iceberg REST Catalog client and HMS REST Catalog Server.
  * It uses the HiveMetaStoreClient backed by hive-iceberg REST catalog adapter to connect to the HMS RESTCatalog Server.
  * The flow is as follows:
  * Hive ql wrapper --> HiveMetaStoreClient --> HiveIcebergRESTCatalogClientAdapter --> HMS RESTCatalog Server --> HMS
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHMSIcebergRESTClientIntegration {

  private static final String DB_NAME = "ice_db";
  private static final String TABLE_NAME = "ice_tbl";
  
  private Configuration conf;
  private HiveConf hiveConf;
  private Hive hive;

  private IMetaStoreClient msClient;

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(ServletSecurity.AuthType.NONE)
          .build();

  @BeforeAll
  public void setup() throws Exception {
    // Starting msClient with Iceberg REST Catalog client underneath
    conf = REST_CATALOG_EXTENSION.getConf();
    conf.set(HiveConf.ConfVars.HIVE_ICEBERG_CATALOG_TYPE.varname, "rest");
    conf.set("iceberg.rest-catalog.uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    msClient = new HiveMetaStoreClient(conf);

    hiveConf = new HiveConf(REST_CATALOG_EXTENSION.getConf(), HiveConf.class);
    hive = Hive.get(hiveConf);
  }

  @AfterAll public void tearDown() {
    if (msClient != null) {
      msClient.close();
    }
  }

  @Test
  public void testIceberg() throws Exception {

    // --- Create Database ---
    Database db = new Database();
    db.setName(DB_NAME);
    db.setOwnerType(PrincipalType.USER);
    db.setOwnerName(System.getProperty("user.name"));
    String warehouseDir = MetastoreConf.get(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname());
    db.setLocationUri(warehouseDir + "/" + DB_NAME + ".db");
    hive.createDatabase(db, true);

    // --- Get Database ---
    assertEquals(DB_NAME, hive.getDatabase(DB_NAME).getName());

    // --- Create Table ---
    org.apache.hadoop.hive.metastore.api.Table tTable = PartitionUtil.createPartitionedTable(msClient, "hive",
        DB_NAME, TABLE_NAME);
    assertNotNull(tTable);
    assertEquals(HiveMetaHook.ICEBERG, tTable.getParameters().get(HiveMetaHook.TABLE_TYPE));

    // --- Get Table ---
    Table table = hive.getTable(DB_NAME, TABLE_NAME);
    assertEquals(DB_NAME, table.getDbName());
    assertEquals(TABLE_NAME, table.getTableName());
  }
}
