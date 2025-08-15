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
package iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
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
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;

/* 
  * This test is an integration test for the hive-iceberg REST Catalog client and HMS REST Catalog Server.
  * It uses the HiveMetaStoreClient backed by hive-iceberg REST catalog adapter to connect to the HMS RESTCatalog Server.
  * The flow is as follows:
  * Hive ql wrapper --> HiveMetaStoreClient --> HiveRESTCatalogClient --> HMS RESTCatalog Server --> HMS
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHiveRESTCatalogClientIT {

  private static final String DB_NAME = "ice_db";
  private static final String TABLE_NAME = "ice_tbl";
  private static final String WAREHOUSE_NAME = "hive";
  private static final String HIVE_ICEBERG_STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  
  private Configuration conf;
  private HiveConf hiveConf;
  private Hive hive;

  private IMetaStoreClient msClient;

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(ServletSecurity.AuthType.NONE)
          .addMetaStoreSchemaClassName(HiveUnitMetaStoreCatalogSchemaInfo.class)
          .build();

  @BeforeAll
  public void setup() throws Exception {
    // Starting msClient with Iceberg REST Catalog client underneath
    conf = REST_CATALOG_EXTENSION.getConf();
    conf.set(MetastoreConf.ConfVars.HIVE_ICEBERG_CATALOG_TYPE.getVarname(), "rest");
    conf.set(MetastoreConf.ConfVars.METASTORE_CLIENT_IMPL.getVarname(),
        "org.apache.iceberg.hive.client.HiveRESTCatalogClient");
    conf.set(InputFormatConfig.CATALOG_REST_CONFIG_PREFIX + ".uri", REST_CATALOG_EXTENSION.getRestEndpoint());
    conf.set(InputFormatConfig.CATALOG_REST_CONFIG_PREFIX + ".warehouse", WAREHOUSE_NAME);

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
    Assertions.assertEquals(DB_NAME, hive.getDatabase(DB_NAME).getName());

    // --- Create Table ---
    org.apache.hadoop.hive.metastore.api.Table tTable = createPartitionedTable(msClient,
        WAREHOUSE_NAME, DB_NAME, TABLE_NAME);
    Assertions.assertNotNull(tTable);
    Assertions.assertEquals(HiveMetaHook.ICEBERG, tTable.getParameters().get(HiveMetaHook.TABLE_TYPE));

    // --- Get Table ---
    org.apache.hadoop.hive.metastore.api.Table table = msClient.getTable(WAREHOUSE_NAME, DB_NAME, TABLE_NAME);
    Assertions.assertEquals(DB_NAME, table.getDbName());
    Assertions.assertEquals(TABLE_NAME, table.getTableName());
    Assertions.assertEquals(HIVE_ICEBERG_STORAGE_HANDLER, table.getParameters().get("storage_handler"));
    Assertions.assertNotNull(table.getParameters().get(TableProperties.DEFAULT_PARTITION_SPEC));
  }

  public static Table createPartitionedTable(IMetaStoreClient db, String catName, String dbName, String tableName)
      throws Exception {
    try {
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
      table.setSd(sd);
      
      Schema schema = HiveSchemaUtil.convert(cols, false);
      PartitionSpec spec = PartitionSpec.builderFor(schema).identity("city").build();
      String specString = PartitionSpecParser.toJson(spec);
      table.setParameters(new java.util.HashMap<>());
      table.getParameters().put(InputFormatConfig.PARTITION_SPEC, specString);
      
      db.createTable(table);
      return db.getTable(catName, dbName, tableName);
    } catch (Exception exception) {
      Assertions.fail("Unable to drop and create table " + StatsUtils.getFullyQualifiedTableName(dbName, tableName) + " because "
          + StringUtils.stringifyException(exception));
      throw exception;
    }
  }
}
