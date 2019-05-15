package org.apache.hadoop.hive.metastore;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.MetaStoreClientTest;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestTransactionalValidationListener extends MetaStoreClientTest {

  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;
  private boolean createdCatalogs = false;

  @BeforeClass
  public static void startMetaStores() {
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<MetastoreConf.ConfVars, String>();

    // Enable TransactionalValidationListener + create.as.acid
    Map<String, String> extraConf = new HashMap<>();
    extraConf.put("metastore.create.as.acid", "true");
    extraConf.put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    extraConf.put("hive.support.concurrency", "true");
    startMetaStores(msConf, extraConf);
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();
    if (!createdCatalogs) {
      createCatalogs();
      createdCatalogs = true;
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      client = null;
    }
  }

  public TestTransactionalValidationListener(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
  }

  private void createCatalogs() throws Exception {
    String[] catNames = {"spark", "myapp"};
    String[] location = {MetaStoreTestUtils.getTestWarehouseDir("spark"),
                         MetaStoreTestUtils.getTestWarehouseDir("myapp")};

    for (int i = 0; i < catNames.length; i++) {
      Catalog cat = new CatalogBuilder()
          .setName(catNames[i])
          .setLocation(location[i])
          .build();
      client.createCatalog(cat);
      File dir = new File(cat.getLocationUri());
      Assert.assertTrue(dir.exists() && dir.isDirectory());
    }
  }

  private Table createOrcTable(String catalog) throws Exception {
    Table table = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = new ArrayList<>();

    table.setDbName("default");
    table.setTableName("test_table");
    cols.add(new FieldSchema("column_name", "int", null));
    sd.setCols(cols);
    sd.setSerdeInfo(new SerDeInfo());
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    table.setSd(sd);
    table.setCatName(catalog);
    table.setTableType("MANAGED_TABLE");

    client.createTable(table);
    Table createdTable = client.getTable(catalog, table.getDbName(), table.getTableName());
    return createdTable;
  }

  @Test
  public void testCreateAsAcid() throws Exception {
    // Table created in hive catalog should have been automatically set to transactional
    Table createdTable = createOrcTable("hive");
    assertTrue(AcidUtils.isTransactionalTable(createdTable));

    // Non-hive catalogs should not be transactional
    createdTable = createOrcTable("spark");
    assertFalse(AcidUtils.isTransactionalTable(createdTable));

    createdTable = createOrcTable("myapp");
    assertFalse(AcidUtils.isTransactionalTable(createdTable));
  }
}
