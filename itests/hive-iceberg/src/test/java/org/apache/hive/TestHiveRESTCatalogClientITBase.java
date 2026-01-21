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
package org.apache.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

/* 
  * This is an integration test for the HiveMetaStoreClient and HMS REST Catalog Server. It creates and uses the 
  * HMS IMetaStoreClient backed by HiveMetaStoreClient adapter to connect to the HMS RESTCatalog Server.
  * The flow is as follows:
  * Hive ql wrapper --> HiveMetaStoreClient --> HiveRESTCatalogClient --> HMS RESTCatalog Server --> HMS
 */
public abstract class TestHiveRESTCatalogClientITBase {

  static final String DB_NAME = "ice_db";
  static final String TABLE_NAME = "ice_tbl";
  static final String CATALOG_NAME = "ice01";
  static final String HIVE_ICEBERG_STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  static final String REST_CATALOG_PREFIX = String.format("%s%s.", IcebergCatalogProperties.CATALOG_CONFIG_PREFIX, 
      CATALOG_NAME);

  HiveConf hiveConf;
  Configuration conf;
  Hive hive;
  IMetaStoreClient msClient;
  
  abstract HiveRESTCatalogServerExtension getHiveRESTCatalogServerExtension();

  public void setupConf() {
    HiveRESTCatalogServerExtension restCatalogExtension = getHiveRESTCatalogServerExtension();

    conf = restCatalogExtension.getConf();

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL,
        "org.apache.iceberg.hive.client.HiveRESTCatalogClient");
    conf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getVarname(), CATALOG_NAME);
    conf.set(REST_CATALOG_PREFIX + "uri", restCatalogExtension.getRestEndpoint());
    conf.set(REST_CATALOG_PREFIX + "type", CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
  }

  @BeforeEach
  void setup() throws Exception {
    setupConf();

    HiveMetaHookLoader hookLoader = tbl -> {
      HiveStorageHandler storageHandler;
      try {
        storageHandler = HiveUtils.getStorageHandler(conf, HIVE_ICEBERG_STORAGE_HANDLER);
      } catch (HiveException e) {
        throw new MetaException(e.getMessage());
      }
      return storageHandler == null ? null : storageHandler.getMetaHook();
    };

    msClient = new HiveMetaStoreClient(conf, hookLoader);
    hiveConf = new HiveConf(conf, HiveConf.class);
    hive = Hive.get(hiveConf);
  }

  @AfterEach
  public void tearDown() {
    if (msClient != null) {
      msClient.close();
    }
  }
  
  @Test
  public void testIceberg() throws Exception {

    // --- Create Database ---
    Database db = new Database();
    db.setCatalogName(CATALOG_NAME);
    db.setName(DB_NAME);
    db.setOwnerType(PrincipalType.USER);
    db.setOwnerName(System.getProperty("user.name"));
    String warehouseDir = MetastoreConf.get(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname());
    db.setLocationUri(warehouseDir + "/" + DB_NAME + ".db");
    hive.createDatabase(db, true);

    // --- Get Database ---
    Database retrievedDB = hive.getDatabase(CATALOG_NAME, DB_NAME);
    Assertions.assertEquals(DB_NAME, retrievedDB.getName());
    Assertions.assertEquals(CATALOG_NAME, retrievedDB.getCatalogName());

    // --- Get Databases ---
    List<String> dbs = msClient.getDatabases(CATALOG_NAME, "ice_*");
    Assertions.assertEquals(1, dbs.size());
    Assertions.assertEquals(DB_NAME, dbs.getFirst());

    // --- Get All Databases ---
    List<String> allDbs = msClient.getAllDatabases(CATALOG_NAME);
    Assertions.assertEquals(2, allDbs.size());
    Assertions.assertTrue(allDbs.contains("default"));
    Assertions.assertTrue(allDbs.contains(DB_NAME));

    // --- Create Table ---
    Table tTable = createPartitionedTable(msClient,
        CATALOG_NAME, DB_NAME, TABLE_NAME, new java.util.HashMap<>());
    Assertions.assertNotNull(tTable);
    Assertions.assertEquals(HiveMetaHook.ICEBERG, tTable.getParameters().get(HiveMetaHook.TABLE_TYPE));

    // --- Create Table --- with an invalid catalog name in table parameters (should fail)
    Map<String, String> tableParameters = new java.util.HashMap<>();
    tableParameters.put(IcebergCatalogProperties.CATALOG_NAME, "some_missing_catalog");
    assertThrows(IllegalArgumentException.class, () -> 
        createPartitionedTable(msClient, CATALOG_NAME, DB_NAME, TABLE_NAME + "_2", tableParameters));

    // --- tableExists ---
    Assertions.assertTrue(msClient.tableExists(CATALOG_NAME, DB_NAME, TABLE_NAME));

    // --- Get Table ---
    GetTableRequest getTableRequest = new GetTableRequest();
    getTableRequest.setCatName(CATALOG_NAME);
    getTableRequest.setDbName(DB_NAME);
    getTableRequest.setTblName(TABLE_NAME);
        
    Table table = msClient.getTable(getTableRequest);
    Assertions.assertEquals(DB_NAME, table.getDbName());
    Assertions.assertEquals(TABLE_NAME, table.getTableName());
    Assertions.assertEquals(HIVE_ICEBERG_STORAGE_HANDLER, table.getParameters().get("storage_handler"));
    Assertions.assertNotNull(table.getParameters().get(TableProperties.DEFAULT_PARTITION_SPEC));
    Assertions.assertEquals(1, table.getPartitionKeys().size());
    Assertions.assertEquals("city", table.getPartitionKeys().getFirst().getName());
    
    // --- Get Tables ---
    List<String> tables = msClient.getTables(CATALOG_NAME, DB_NAME, "ice_*");
    Assertions.assertEquals(1, tables.size());
    Assertions.assertEquals(TABLE_NAME, tables.getFirst());

    // --- Get All Tables ---
    List<String> allTables = msClient.getAllTables(CATALOG_NAME, DB_NAME);
    Assertions.assertEquals(1, allTables.size());
    Assertions.assertEquals(TABLE_NAME, allTables.getFirst());

    // --- Drop Table ---
    msClient.dropTable(CATALOG_NAME, DB_NAME, TABLE_NAME);
    Assertions.assertFalse(msClient.tableExists(CATALOG_NAME, DB_NAME, TABLE_NAME));

    // --- Drop Database ---
    msClient.dropDatabase(DB_NAME);
    Assertions.assertFalse(msClient.getAllDatabases(CATALOG_NAME).contains(DB_NAME));
  }

  private static Table createPartitionedTable(IMetaStoreClient db, String catName, String dbName, String tableName, 
      Map<String, String> tableParameters) throws Exception {
    db.dropTable(catName, dbName, tableName);
    Table table = new Table();
    table.setCatName(catName);
    table.setDbName(dbName);
    table.setTableName(tableName);
    
    FieldSchema col1 = new FieldSchema("key", "string", "");
    FieldSchema col2 = new FieldSchema("value", "int", "");
    FieldSchema col3 = new FieldSchema("city", "string", "");
    List<FieldSchema> cols = Arrays.asList(col1, col2, col3);
    
    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setInputFormat(TextInputFormat.class.getCanonicalName());
    sd.setOutputFormat(HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    sd.setCols(cols);
    sd.getSerdeInfo().setParameters(new java.util.HashMap<>());
    table.setSd(sd);
    
    Schema schema = HiveSchemaUtil.convert(cols, Collections.emptyMap(), false);
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("city").build();
    String specString = PartitionSpecParser.toJson(spec);
    table.setParameters(new java.util.HashMap<>());
    table.getParameters().putAll(tableParameters);
    table.getParameters().put(TableProperties.DEFAULT_PARTITION_SPEC, specString);
    
    db.createTable(table);

    GetTableRequest getTableRequest = new GetTableRequest();
    getTableRequest.setCatName(CATALOG_NAME);
    getTableRequest.setDbName(DB_NAME);
    getTableRequest.setTblName(TABLE_NAME);
    
    return db.getTable(getTableRequest);
  }
}
