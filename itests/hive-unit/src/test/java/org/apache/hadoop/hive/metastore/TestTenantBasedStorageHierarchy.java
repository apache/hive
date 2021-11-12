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

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.client.builder.GetTablesRequestBuilder;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ExtendedTableInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTablesExtRequestFields;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_NONE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READONLY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.parse.WarehouseInstance;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTenantBasedStorageHierarchy {
  private static final Logger LOG = LoggerFactory.getLogger(TestTenantBasedStorageHierarchy.class);
  protected static HiveMetaStoreClient client;
  protected static Configuration conf;
  File ext_wh = null;
  File wh = null;

  protected static boolean isThriftClient = true;
  private static final String CAPABILITIES_KEY = "OBJCAPABILITIES";
  private static final String DATABASE_WAREHOUSE_SUFFIX = ".db";

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    wh = new File(System.getProperty("java.io.tmpdir") + File.separator +
        "hive" + File.separator + "warehouse" + File.separator + "hive" + File.separator);
    wh.mkdirs();

    ext_wh = new File(System.getProperty("java.io.tmpdir") + File.separator +
        "hive" + File.separator + "warehouse" + File.separator + "hive-external" + File.separator);
    ext_wh.mkdirs();

    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS,
        "org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer");
    MetastoreConf.setBoolVar(conf, ConfVars.ALLOW_TENANT_BASED_STORAGE, true);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, false);
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, wh.getCanonicalPath());
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE_EXTERNAL, ext_wh.getCanonicalPath());
    client = new HiveMetaStoreClient(conf);
  }

  private static void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException|InvalidOperationException e) {
      // NOP
    }
  }

  private void resetHMSClient() {
    client.setProcessorIdentifier(null);
    client.setProcessorCapabilities(null);
  }

  private void setHMSClient(String id, String[] caps) {
    client.setProcessorIdentifier(id);
    client.setProcessorCapabilities(caps);
  }

  private File getManagedRootForTenant(String tenant) {
    return new File(System.getProperty("java.io.tmpdir") + File.separator +
        tenant + File.separator +
        "hive" + File.separator + "warehouse" + File.separator + "managed" + File.separator);
  }

  private File getExternalRootForTenant(String tenant) {
    return new File(System.getProperty("java.io.tmpdir") + File.separator +
        tenant + File.separator +
        "hive" + File.separator + "warehouse" + File.separator + "external" + File.separator);
  }

  @Test
  public void testCreateDatabaseOldSyntax() throws Exception {
    try {
      resetHMSClient();
      final String dbName = "db1";
      String basetblName = "oldstyletable";
      Map<String, Object> tProps = new HashMap<>();

      Database db = createDatabase("hive", dbName, null, null);

      Database db2 = client.getDatabase("hive", dbName);
      assertNull(db2.getManagedLocationUri());
      assertNotNull("Database's locationUri is expected to be not null:actual=" + db2.getLocationUri());

      String tblName = "ext_" + basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      setHMSClient("testCreateDatabaseOldSyntax", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertTrue("Database location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));
      resetHMSClient();

      tblName = "mgd_" + basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", properties.toString());

      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertTrue("Database location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE.getVarname())));
    } catch (Exception e) {
      fail("testCreateDatabaseOldSyntax failed with " + e.getMessage());
    } finally {
      silentDropDatabase("db1");
      resetHMSClient();
    }
  }

  @Test
  public void testCreateDatabaseWithOldLocation() throws Exception {
    try {
      resetHMSClient();
      final String dbName = "dbx";
      String basetblName = "oldstyletable";
      Map<String, Object> tProps = new HashMap<>();

      String location = ext_wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(DATABASE_WAREHOUSE_SUFFIX);
      Database db = createDatabase("hive", dbName, location, null);

      Database db2 = client.getDatabase("hive", dbName);
      assertNull(db2.getManagedLocationUri());
      assertNotNull("Database's locationUri is expected to be not null:actual=" + db2.getLocationUri());

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      setHMSClient("testCreateDatabaseWithOldLocation", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertTrue("External table location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));

      tblName = "mgd_" + basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", properties.toString());

      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("Database's locationUri is expected to be equal to set value",
          Path.getPathWithoutSchemeAndAuthority(new Path(location)),
          Path.getPathWithoutSchemeAndAuthority(new Path(db2.getLocationUri())));
      assertTrue("Managed table location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE.getVarname())));
    } catch (Exception e) {
      fail("testCreateDatabaseWithOldLocation failed with " + e.getMessage());
    } finally {
      silentDropDatabase("dbx");
      resetHMSClient();
    }
  }

  @Test
  public void testCreateDatabaseWithNewLocation() throws Exception {
    try {
      resetHMSClient();
      String dbName = "dbx";
      String basetblName = "newstyletable";
      Map<String, Object> tProps = new HashMap<>();
      String tenant1 = "tenant1";
      String tenant2 = "tenant2";

      String location = getExternalRootForTenant(tenant1).getAbsolutePath();
      Database db = createDatabase("hive", dbName, location, null);

      Database db2 = client.getDatabase("hive", dbName);
      assertNull(db2.getManagedLocationUri());
      assertNotNull("Database's locationUri is expected to be not null:actual=" + db2.getLocationUri());
      assertEquals("Expected location is different from actual location",
          Path.getPathWithoutSchemeAndAuthority(new Path(db2.getLocationUri())),
          Path.getPathWithoutSchemeAndAuthority(new Path(location)));

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      setHMSClient("testCreateDatabaseWithNewLocation", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertTrue("External table location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));

      dbName = "dbm";
      String mgdLocation = getManagedRootForTenant(tenant2).getAbsolutePath();
      location = getExternalRootForTenant(tenant2).getAbsolutePath();
      db = createDatabase("hive", dbName, location, mgdLocation);

      db2 = client.getDatabase("hive", dbName);
      assertNotNull("Database's managedLocationUri is expected to be not null" + db2.getManagedLocationUri());
      assertNotNull("Database's locationUri is expected to be not null" + db2.getLocationUri());
      assertEquals("Expected location is different from actual location",
          Path.getPathWithoutSchemeAndAuthority(new Path(location)),
          Path.getPathWithoutSchemeAndAuthority(new Path(db2.getLocationUri())));

      tblName = "mgd_" + basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", properties.toString());

      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertTrue("Managed table location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE.getVarname())));
    } catch (Exception e) {
      fail("testCreateDatabaseWithNewLocation failed with " + e.getMessage());
    } finally {
      silentDropDatabase("dbx");
      resetHMSClient();
    }
  }

  @Test
  public void testCreateDatabaseWithExtAndManagedLocations() throws Exception {
    try {
      resetHMSClient();
      final String dbName = "dbxm";
      String basetblName = "newstyletable";
      Map<String, Object> tProps = new HashMap<>();

      String location = ext_wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(DATABASE_WAREHOUSE_SUFFIX);
      String mgdLocation = wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(DATABASE_WAREHOUSE_SUFFIX);
      Database db = createDatabase("hive", dbName, location, mgdLocation);

      Database db2 = client.getDatabase("hive", dbName);
      assertNotNull("Database's managedLocationUri is expected to be not null" + db2.getManagedLocationUri());
      assertNotNull("Database's locationUri is expected to be not null" + db2.getLocationUri());

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      setHMSClient("testCreateDatabaseWithLocation", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertTrue("External table location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));

      tblName = "mgd_" + basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", properties.toString());

      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("Database's locationUri is expected to be equal to set value",
          Path.getPathWithoutSchemeAndAuthority(new Path(location)),
          Path.getPathWithoutSchemeAndAuthority(new Path(db2.getLocationUri())));
      assertTrue("Managed table location not as expected:actual=" + db2.getLocationUri(),
          tbl2.getSd().getLocation().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE.getVarname())));
    } catch (Exception e) {
      fail("testCreateDatabaseWithLocation failed with " + e.getMessage());
    } finally {
      silentDropDatabase("dbxm");
      resetHMSClient();
    }
  }

  @Test
  public void testAlterDatabase() throws Exception {
    try {
      resetHMSClient();
      final String dbName = "dbalter";

      Database db = createDatabase("hive", dbName, null, null);
      Database db2 = client.getDatabase("hive", dbName);
      assertNull(db2.getManagedLocationUri());
      assertNotNull("Database's locationUri is expected to be not null:actual=" + db2.getLocationUri());

      String mgdLocation = wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(DATABASE_WAREHOUSE_SUFFIX);
      db.setManagedLocationUri(mgdLocation);
      client.alterDatabase(dbName, db);
      db2 = client.getDatabase("hive", dbName);
      assertNotNull("Database's managedLocationUri is expected to be not null" + db2.getManagedLocationUri());
      assertEquals("Database's managed location is expected to be equal", db2.getManagedLocationUri(), mgdLocation);

      String location = ext_wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(DATABASE_WAREHOUSE_SUFFIX);
      db.setLocationUri(location);
      db2 = client.getDatabase("hive", dbName);
      assertEquals("Database's managed location is expected to be equal",
          Path.getPathWithoutSchemeAndAuthority(new Path(db2.getManagedLocationUri())),
          Path.getPathWithoutSchemeAndAuthority(new Path(mgdLocation)));
      assertEquals("Database's location is expected to be equal",
          Path.getPathWithoutSchemeAndAuthority(new Path(db2.getLocationUri())),
          Path.getPathWithoutSchemeAndAuthority(new Path(location)));
    } catch (Exception e) {
      System.err.println(org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.err.println("testAlterDatabase() failed.");
      fail("testAlterDatabase failed:" + e.getMessage());
    } finally {
      silentDropDatabase("dbalter");
      resetHMSClient();
    }
  }

  private Database createDatabase(String catName, String dbName, String location, String managedLocation) throws Exception {
    if (catName == null)
      catName = "hive";

    DatabaseBuilder builder = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName);

    if (location != null)
      builder.setLocation(location);
    if (managedLocation != null)
      builder.setManagedLocation(managedLocation);
    return builder.create(client, conf);
  }

  private Table createTableWithCapabilities(Map<String, Object> props) throws Exception {
    String catalog = (String)props.getOrDefault("CATALOG", MetaStoreUtils.getDefaultCatalog(conf));
    String dbName = (String)props.getOrDefault("DBNAME", "simpdb");
    String tblName = (String)props.getOrDefault("TBLNAME", "test_table");
    TableType type = (TableType)props.getOrDefault("TBLTYPE", TableType.MANAGED_TABLE);
    int buckets = ((Integer)props.getOrDefault("BUCKETS", -1)).intValue();
    String properties = (String)props.getOrDefault("PROPERTIES", "");
    String location = (String)(props.get("LOCATION"));
    boolean dropDb = ((Boolean)props.getOrDefault("DROPDB", Boolean.TRUE)).booleanValue();
    int partitionCount = ((Integer)props.getOrDefault("PARTITIONS", 0)).intValue();

    final String typeName = "Person";

    if (type == TableType.EXTERNAL_TABLE) {
      if (!properties.contains("EXTERNAL=TRUE")) {
        properties.concat(";EXTERNAL=TRUE;");
      }
    }

    Map<String,String> table_params = new HashMap();
    if (properties.length() > 0) {
      String[] propArray = properties.split(";");
      for (String prop : propArray) {
        String[] keyValue = prop.split("=");
        table_params.put(keyValue[0], keyValue[1]);
      }
    }

    Catalog cat = null;
    try {
      cat = client.getCatalog(catalog);
    } catch (NoSuchObjectException e) {
      LOG.debug("Catalog does not exist, creating a new one");
      try {
        if (cat == null) {
          cat = new Catalog();
          cat.setName(catalog.toLowerCase());
          Warehouse wh = new Warehouse(conf);
          cat.setLocationUri(wh.getWhRootExternal().toString() + File.separator + catalog);
          cat.setDescription("Non-hive catalog");
          client.createCatalog(cat);
          LOG.debug("Catalog " + catalog + " created");
        }
      } catch (Exception ce) {
        LOG.warn("Catalog " + catalog + " could not be created");
      }
    } catch (Exception e) {
      LOG.error("Creation of a new catalog failed, aborting test");
      throw e;
    }

    try {
      client.dropTable(dbName, tblName);
    } catch (Exception e) {
      LOG.info("Drop table failed for " + dbName + "." + tblName);
    }

    try {
      if (dropDb)
        silentDropDatabase(dbName);
    } catch (Exception e) {
      LOG.info("Drop database failed for " + dbName);
    }

    if (dropDb)
      new DatabaseBuilder()
          .setName(dbName)
          .setCatalogName(catalog)
          .create(client, conf);

    try {
      client.dropType(typeName);
    } catch (Exception e) {
      LOG.info("Drop type failed for " + typeName);
    }

    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(2));
    typ1.getFields().add(
        new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
    typ1.getFields().add(
        new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    client.createType(typ1);

    TableBuilder builder = new TableBuilder()
        .setCatName(catalog)
        .setDbName(dbName)
        .setTableName(tblName)
        .setCols(typ1.getFields())
        .setType(type.name())
        .setLocation(location)
        .setNumBuckets(buckets)
        .setTableParams(table_params)
        .addBucketCol("name")
        .addStorageDescriptorParam("test_param_1", "Use this for comments etc");

    if (location != null)
      builder.setLocation(location);

    if (buckets > 0)
      builder.setNumBuckets(buckets).addBucketCol("name");

    if (partitionCount > 0) {
      builder.addPartCol("partcol", "string");
    }

    if (type == TableType.MANAGED_TABLE) {
      if (properties.contains("transactional=true") && !properties.contains("transactional_properties=insert_only")) {
        builder.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        builder.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
        builder.setSerdeLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        builder.addStorageDescriptorParam("inputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        builder.addStorageDescriptorParam("outputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
      }
    }

    Table tbl = builder.create(client, conf);
    LOG.info("Table " + tbl.getTableName() + " created:type=" + tbl.getTableType());

    if (partitionCount > 0) {
      List<Partition> partitions = new ArrayList<>();

      List<List<String>> partValues = new ArrayList<>();
      for (int i = 1; i <= partitionCount; i++) {
        partValues.add(Lists.newArrayList("" + i));
      }

      for(List<String> vals : partValues){
        addPartition(client, tbl, vals);
      }
    }

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(catalog, dbName, tblName);
      LOG.info("Fetched Table " + tbl.getTableName() + " created:type=" + tbl.getTableType());
    }
    return tbl;
  }

  private void addPartition(IMetaStoreClient client, Table table, List<String> values)
      throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    Partition p = partitionBuilder.build(conf);
    p.getSd().setNumBuckets(-1); // PartitionBuilder uses 0 as default whereas we use -1 for Tables.
    client.add_partition(p);
  }
}


