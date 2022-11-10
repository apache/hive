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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;

import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
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
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_NONE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READONLY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READWRITE;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHiveMetastoreTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetastoreTransformer.class);
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

  @Test
  public void testTransformerWithOldTables() throws Exception {
    try {
      resetHMSClient();
      final String dbName = "db1";
      String basetblName = "oldstyletable";
      Map<String, Object> tProps = new HashMap<>();
      int buckets = 32;

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("BUCKETS", buckets);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      setHMSClient("testTranformerWithOldTables", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      LOG.info("Test execution complete:testTransformerWithOldTables");
    } catch (Exception e) {
      fail("testTransformerWithOldTables failed with " + e.getMessage());
    } finally {
      resetHMSClient();
    }

  }

  /**
   * EXTERNAL_TABLE
   *   1) Old table (name in upper case) with no capabilities
   *   2) Old table with no capabilities
   *   3a) New table with capabilities with no client requirements
   *   3b) New table with capabilities with no matching client requirements
   *   3c) New table with capabilities with partial match requirements
   *   3d) New table with capabilities with full match requirements
   */
  @Test
  public void testTransformerExternalTable() throws Exception {
    try {
      final String dbName = "db1";
      String basetblName = "oldstyleexttable";
      Map<String, Object> tProps = new HashMap<>();
      int buckets = 32;

      // create external table with uppercase
      String tblNameUpper = "TAB_EXT1";
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblNameUpper);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("BUCKETS", buckets);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      Table hiveTbl = client.getTable(dbName, tblNameUpper.toLowerCase(Locale.ROOT));
      Path actualPath = new Path(hiveTbl.getSd().getLocation());
      Database db = client.getDatabase(dbName);
      LOG.info("Table=" + tblNameUpper + ",Table Details=" + hiveTbl);
      assertEquals("Created and retrieved tables do not match:" + hiveTbl.getTableName() + ":" +
              tblNameUpper.toLowerCase(Locale.ROOT), hiveTbl.getTableName(),
          tblNameUpper.toLowerCase(Locale.ROOT));
      Path expectedTablePath = new Path(db.getLocationUri(), tblNameUpper.toLowerCase(Locale.ROOT));
      assertEquals(String.format("Table location %s is not a subset of the database location %s",
          actualPath.toString(), db.getLocationUri()), expectedTablePath.toString(), actualPath.toString());

      String tblName = basetblName;
      tProps = new HashMap<>();
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("BUCKETS", buckets);
      properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      LOG.info("Table=" + tblName + ",Access=" + tbl2.getAccessType());
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("TableType mismatch", TableType.EXTERNAL_TABLE.name(), tbl2.getTableType());
      assertEquals(buckets, tbl2.getSd().getNumBuckets()); // no transformation
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // old client, AccessType not set
      assertNull(tbl2.getRequiredReadCapabilities());

      setHMSClient("testTransformerExternalTable", (new String[] { "HIVEBUCKET2" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Expected buckets does not match:", buckets, tbl2.getSd().getNumBuckets()); // no transformation
      assertEquals("Table access type does not match expected value:" + tblName,
              ACCESSTYPE_READWRITE, tbl2.getAccessType()); // RW with HIVEBUCKET2 but no EXTWRITE
      resetHMSClient();

      setHMSClient("testTransformerAcceptsUnmodifiedMetadata", (new String[] { "ACCEPTS_UNMODIFIED_METADATA" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Expected buckets does not match:", buckets, tbl2.getSd().getNumBuckets()); // no transformation
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // RO without HIVEBUCKET2 but with ACCEPTS_UNMODIFIED_METADATA
      resetHMSClient();

      tblName = "test_ext_bucketed_wc";
      properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("HIVEBUCKET2,EXTREAD,EXTWRITE");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType());
      assertEquals(buckets, tbl2.getSd().getNumBuckets()); // no tranformation

      setHMSClient("testTranformerExternalTable", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      assertEquals(buckets, tbl2.getSd().getNumBuckets()); // client has the HIVEBUCKET2 capability, retain bucketing info
      assertNull(tbl2.getRequiredWriteCapabilities());
      assertNull(tbl2.getRequiredReadCapabilities());
      resetHMSClient();

      setHMSClient("testTransformerExternalTableRO", (new String[] { "EXTREAD", "EXTWRITE"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      assertEquals(-1, tbl2.getSd().getNumBuckets()); // client has no HIVEBUCKET2 capability, remove bucketing info
      assertNotNull("Required write capabilities is null",
              tbl2.getRequiredWriteCapabilities());
      assertTrue("Returned required capabilities list does not contain HIVEBUCKET2",
              tbl2.getRequiredWriteCapabilities().contains("HIVEBUCKET2"));
      resetHMSClient();

      setHMSClient("testTransformerExternalTableROwAUM", (new String[] { "EXTREAD", "EXTWRITE", "ACCEPTS_UNMODIFIED_METADATA"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      assertEquals(buckets, tbl2.getSd().getNumBuckets()); // client has no HIVEBUCKET2 capability, but has ACCEPTS_UNMODIFIED_METADATA
      assertNotNull("Required write capabilities is null",
          tbl2.getRequiredWriteCapabilities());
      assertTrue("Returned required capabilities list does not contain HIVEBUCKET2",
          tbl2.getRequiredWriteCapabilities().contains("HIVEBUCKET2"));
      resetHMSClient();

      tblName = "test_ext_unbucketed_wc";
      properties = new StringBuilder();
      properties.append(CAPABILITIES_KEY).append("=").append("EXTREAD,EXTWRITE");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tProps.remove("BUCKETS");
      tbl = createTableWithCapabilities(tProps);

      setHMSClient("TestTransformerExternalUnbucketed", (new String[] {"EXTREAD", "EXTWRITE"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      assertNull("Required read capabilities are not null", tbl2.getRequiredReadCapabilities());
      assertNull("Required write capabilities are not null", tbl2.getRequiredWriteCapabilities());

      resetHMSClient();

      tblName = "test_ext_sparkbucketed_wc";
      properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("SPARKDECIMAL,SPARKBUCKET,EXTREAD,EXTWRITE");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tProps.put("BUCKETS", buckets);
      tbl = createTableWithCapabilities(tProps);

      setHMSClient("testTransformerExternalTableSpark", (new String[] { "HIVEFULLACIDREAD", "CONNECTORREAD"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // requires EXTREAD for RO
      assertEquals(buckets, tbl2.getSd().getNumBuckets());
      assertNotNull("Required read capabilities is null", tbl2.getRequiredReadCapabilities());
      assertNotNull("Required write capabilities is null", tbl2.getRequiredWriteCapabilities());
      assertTrue("Required read capabilities does not contain EXTREAD",
              tbl2.getRequiredReadCapabilities().contains("EXTREAD"));
      assertTrue("Required write capabilities does not contain EXTWRITE",
              tbl2.getRequiredWriteCapabilities().contains("EXTWRITE"));

      setHMSClient("testTransformerExternalTableSpark", (new String[] { "EXTREAD", "CONNECTORREAD"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // requires EXTREAD for RO
      assertEquals(buckets, tbl2.getSd().getNumBuckets());
      assertNotNull("Required write capabilities is null", tbl2.getRequiredWriteCapabilities());
      assertTrue("Required write capabilities does not contain EXTWRITE",
              tbl2.getRequiredWriteCapabilities().contains("EXTWRITE"));


      setHMSClient("testTransformerExternalTableSpark", (new String[] { "SPARKBUCKET", "SPARKDECIMAL", "EXTREAD", "EXTWRITE" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      assertEquals(buckets, tbl2.getSd().getNumBuckets()); // client has SPARKBUCKET capability
      resetHMSClient();

      LOG.info("Test execution complete:testTransformerExternalTable");
    } catch (Exception e) {
      fail("testTransformerExternalTable failed with " + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerManagedTable() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "db1";
      String basetblName = "oldstylemgdtable";
      Map<String, Object> tProps = new HashMap<>();

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      // create unbucketed managed table with no capabilities
      Table tbl = createTableWithCapabilities(tProps);

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      LOG.info("Table=" + tblName + ",Access=" + tbl2.getAccessType());
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("TableType mismatch", TableType.EXTERNAL_TABLE.name(), tbl2.getTableType()); // transformed
      assertEquals(-1, tbl2.getSd().getNumBuckets());
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // no translation to be done, so accessType not set
      assertNull("Required read capabilities not null", tbl2.getRequiredReadCapabilities());
      assertNull("Required write capabilities not null", tbl2.getRequiredWriteCapabilities());

      // managed table with no capabilities
      tblName = "test_mgd_insert_woc";
      StringBuilder properties = new StringBuilder();
      tProps.put("TBLNAME", tblName);
      properties.append("transactional=true");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", properties.toString());

      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);
      resetHMSClient();

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // no transformation

      setHMSClient("testMGDwithConnectorRead", new String[] {"CONNECTORREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      assertNotNull("Required write capabilities are null", tbl2.getRequiredWriteCapabilities());
      assertTrue("Required write capabilities does not contain CONNECTORWRITE",
              tbl2.getRequiredWriteCapabilities().contains("CONNECTORWRITE"));
      resetHMSClient();

      setHMSClient("testMGDwithInsertRead", new String[] {"HIVEMANAGEDINSERTREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      assertNotNull("Required write capabilities are null", tbl2.getRequiredWriteCapabilities());
      assertTrue("Required write capabilities does not contain CONNECTORWRITE",
              tbl2.getRequiredWriteCapabilities().contains("CONNECTORWRITE"));
      resetHMSClient();

      setHMSClient("testMGDwithInsertWrite", new String[] {"HIVEMANAGEDINSERTWRITE"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      assertNull("Required read capabilities are not null", tbl2.getRequiredReadCapabilities());
      assertNull("Required write capabilities are not null", tbl2.getRequiredWriteCapabilities());
      resetHMSClient();

      setHMSClient("testMGDwithConnectorWrite", new String[] {"CONNECTORWRITE"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      assertNull("Required read capabilities are not null", tbl2.getRequiredReadCapabilities());
      resetHMSClient();

      // bucketed table with no capabilities
      tblName = "test_mgd_insert_wc";
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("HIVEMANAGEDINSERTREAD,HIVEMANAGEDINSERTWRITE,HIVECACHEINVALIDATE,")
                .append("HIVEMANAGEDSTATS,CONNECTORREAD,CONNECTORWRITE");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE" ,"HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);
      resetHMSClient();

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType());

      setHMSClient("testMGDwithAllWrites", new String[] { "HIVEMANAGEDINSERTWRITE", "HIVECACHEINVALIDATE",
          "HIVEMANAGEDSTATS" , "CONNECTORWRITE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      assertNull("Required read capabilities are not null", tbl2.getRequiredReadCapabilities());
      assertNull("Required write capabilities are not null", tbl2.getRequiredWriteCapabilities());
      resetHMSClient();

      setHMSClient("testMGDwith1MissingWrite", new String[] {"HIVEMANAGEDINSERTREAD", "HIVEMANAGEDINSERTWRITE",
          "HIVECACHEINVALIDATE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      assertNull("Required read capabilities are not null", tbl2.getRequiredReadCapabilities());
      assertNotNull("Required write capabilities are null", tbl2.getRequiredWriteCapabilities());
      assertTrue("Required write capabilities contains HIVEMANAGEDSTATS",
              tbl2.getRequiredWriteCapabilities().contains("HIVEMANAGEDSTATS"));
      resetHMSClient();

      setHMSClient("testMGDwith1Write", new String[] {"CONNECTORWRITE"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithConnectorRead", new String[] {"CONNECTORREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      resetHMSClient();

      // managed tables with no capabilities
      tblName = "test_mgd_full_woc";
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      tProps.put("PROPERTIES", properties.toString());
      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);
      resetHMSClient();

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // translation skipped due to no capabilities

      setHMSClient("testMGDwithRAWMETADATA", new String[] {"MANAGERAWMETADATA"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithConnectorRead", new String[] {"CONNECTORREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithFullACIDRead", new String[] {"HIVEFULLACIDREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwith1Write", new String[] {"HIVEFULLACIDWRITE"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithConnectorWrite", new String[] {"CONNECTORWRITE"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      // bucketed table with no capabilities
      tblName = "test_mgd_full_wc";
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,")
                .append("HIVEMANAGEDSTATS,CONNECTORREAD,CONNECTORWRITE");
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      tProps.put("PROPERTIES", properties.toString());
      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE", "HIVEFULLACIDWRITE"});
      tbl = createTableWithCapabilities(tProps);
      resetHMSClient();

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType());

      setHMSClient("testMGDwithRAWMETADATA", new String[] {"MANAGERAWMETADATA"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDFULLwithAllWrites", new String[] { "HIVEFULLACIDWRITE", "HIVECACHEINVALIDATE",
          "HIVEMANAGEDSTATS", "CONNECTORWRITE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDFULLwith1MissingWrite", new String[] { "HIVECACHEINVALIDATE",
          "HIVEMANAGEDSTATS", "CONNECTORWRITE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDFULLwith1Write", new String[] { "CONNECTORWRITE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwith1MissingWrite", new String[] {"HIVEFULLACIDREAD", "HIVEFULLACIDWRITE",
          "HIVECACHEINVALIDATE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithConnectorRead", new String[] {"CONNECTORREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testTranformerExternalTable", (new String[] { "HIVEBUCKET2", "EXTREAD", "EXTWRITE"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType());

      resetHMSClient();

      LOG.info("Test execution complete:testTransformerManagedTable");
    } catch (Exception e) {
      e.printStackTrace();
      fail("testTransformerManagedTable failed with " + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerVirtualView() throws Exception {
    try {
      final String dbName = "db1";
      String basetblName = "oldstyleview";
      Map<String, Object> tProps = new HashMap<>();

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.VIRTUAL_VIEW);
      StringBuilder properties = new StringBuilder();
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      LOG.info("View=" + tblName + ",Access=" + tbl2.getAccessType());
      assertEquals("Created and retrieved views do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("TableType mismatch", TableType.VIRTUAL_VIEW.name(), tbl2.getTableType());
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // old client, AccessType not set

      // table has capabilities
      tblName = "test_view_wc";
      properties = new StringBuilder();
      properties.append(CAPABILITIES_KEY).append("=").append("HIVESQL");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // old client, no transformation

      setHMSClient("testTransformerVirtualView", (new String[] { "HIVESQL" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // RO accessonly for all views

      setHMSClient("testTransformerVirtualView", (new String[] { "EXTWRITE", "HIVEFULLACIDWRITE" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // missing HIVESQL

      resetHMSClient();

      // table does not capabilities but client is newer
      tblName = "test_view_woc";
      properties = new StringBuilder();
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      setHMSClient("testTransformerVirtualView", (new String[] { "HIVESQL" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // RO accessonly for all views

      setHMSClient("testTransformerVirtualView", (new String[] { "EXTWRITE", "HIVEFULLACIDWRITE" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // missing HIVESQL

      LOG.info("Test execution complete:testTransformerVirtualView");
    } catch (Exception e) {
      fail("testTransformerVirtualView failed with " + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerMaterializedView() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "db1";
      String basetblName = "oldstylemqtview";
      Map<String, Object> tProps = new HashMap<>();

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MATERIALIZED_VIEW);
      StringBuilder properties = new StringBuilder();
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      LOG.info("View=" + tblName + ",Access=" + tbl2.getAccessType());
      assertEquals("Created and retrieved views do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("TableType mismatch", TableType.MATERIALIZED_VIEW.name(), tbl2.getTableType());
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // old client, AccessType not set

      // table has capabilities
      tblName = "test_mqtview_wc";
      properties = new StringBuilder();
      properties.append(CAPABILITIES_KEY).append("=").append("HIVEFULLACIDREAD,HIVEONLYMQTWRITE,HIVEMANAGESTATS,HIVEMQT,CONNECTORREAD");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // View has capabilities, processor doesnt, no tranformation

      setHMSClient("testTransformerMQTFullSet", (new String[] { "HIVEFULLACIDREAD", "HIVEONLYMQTWRITE",
          "HIVEMANAGESTATS", "HIVEMQT", "CONNECTORREAD" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // RO accessonly for all views

      setHMSClient("testTransformerMQTFullRead", (new String[] { "HIVEFULLACIDREAD", "HIVEMQT" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType());

      setHMSClient("testTransformerMQTConnRead", (new String[] { "CONNECTORREAD", "HIVEMQT" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // RO accessonly for all views

      setHMSClient("testTransformerMQTDummySet", (new String[] { "EXTWRITE", "HIVEFULLACIDWRITE" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // missing HIVEMQT + *READ

      resetHMSClient();

      // table does not capabilities but client is newer
      tblName = "test_mqtview_woc";
      properties = new StringBuilder();
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      setHMSClient("testTransformerVirtualView", (new String[] { "HIVEFULLACIDREAD", "HIVEMQT" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // RO accessonly for all views

      setHMSClient("testTransformerMQTConnRead", (new String[] { "CONNECTORREAD", "HIVEMQT" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // RO accessonly for all views

      setHMSClient("testTransformerVirtualView", (new String[] { "EXTWRITE, HIVEFULLACIDWRITE" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // missing HIVEMQT + *READ

      LOG.info("Test execution complete:testTransformerMaterializedView");
    } catch (Exception e) {
      fail("testTransformerMaterializedVirtualView failed with " + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testGetTablesExt() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "dbext";
      String tblName = "test_get_tables_ext";
      int count = 10;
      Map<String, Object> tProps = new HashMap<>();

      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);

      StringBuilder table_params = new StringBuilder();
      List<String> capabilities = new ArrayList<>();
      TableType type = TableType.EXTERNAL_TABLE;

      capabilities.add("HIVEFULLACIDWRITE");
      capabilities.add("HIVEFULLACIDREAD");
      capabilities.add("HIVECACHEINVALIDATE");
      capabilities.add("CONNECTORREAD");
      capabilities.add("CONNECTORWRITE");

      tProps.put("CAPABILITIES", capabilities);
      tProps.put("TABLECOUNT", count);
      tProps.put("TBLTYPE", type);

      setHMSClient("test_get_tables_ext", (String[])(capabilities.toArray(new String[0])));

      List<String> tables = createTables(tProps);
      int requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.PROCESSOR_CAPABILITIES)).bitValue();
      List<ExtendedTableInfo> extTables = client.getTablesExt(null, dbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size:extTables", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertNull("Return object should not have read capabilities", tableInfo.getRequiredReadCapabilities());
        assertNull("Return object should not have write capabilities", tableInfo.getRequiredWriteCapabilities());
        assertEquals("AccessType not expected to be set", 0, tableInfo.getAccessType());
      }

      requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.ACCESS_TYPE)).bitValue();
      extTables = client.getTablesExt(null, dbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertNull("Return object should not have read capabilities", tableInfo.getRequiredReadCapabilities());
        assertNull("Return object should not have write capabilities", tableInfo.getRequiredWriteCapabilities());
        assertTrue("AccessType expected to be set", tableInfo.getAccessType() > 0);
      }

      requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.ALL)).bitValue();
      extTables = client.getTablesExt(null, dbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertTrue("AccessType expected to be set", tableInfo.getAccessType() > 0);
      }

      extTables = client.getTablesExt(null, dbName, "*", requestedFields, (count - 3));
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", (count - 3), extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertTrue("AccessType expected to be set", tableInfo.getAccessType() > 0);
      }

      extTables = client.getTablesExt(null, dbName, "*", requestedFields, -1);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());

      count = 300;
      tProps.put("TBLNAME", "test_limit");
      tProps.put("TABLECOUNT", count);
      tables = createTables(tProps);
      assertEquals("Unexpected number of tables created", count, tables.size());

      extTables = client.getTablesExt(null, dbName, "test_limit*", requestedFields, count);
      assertEquals("Unexpected number of tables returned", count, extTables.size());

      extTables = client.getTablesExt(null, dbName, "test_limit*", requestedFields, (count/2));
      assertEquals("Unexpected number of tables returned", (count/2), extTables.size());

      extTables = client.getTablesExt(null, dbName, "test_limit*", requestedFields, 1);
      assertEquals("Unexpected number of tables returned", 1, extTables.size());

      LOG.info("Test execution complete:testTablesExt");
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testGetTablesExt() failed.");
      fail("testGetTablesExt failed with " + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testGetPartitionsByNames() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "db1";
      String tblName = "test_get_parts_by_names";
      int count = 1;
      int pCount = 10;
      int bucketCount = 100;
      Map<String, Object> tProps = new HashMap<>();

      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);

      List<String> capabilities = new ArrayList<>();
      TableType type = TableType.MANAGED_TABLE;

      capabilities.add("HIVEFULLACIDWRITE");
      capabilities.add("HIVEFULLACIDREAD");
      capabilities.add("HIVECACHEINVALIDATE");
      capabilities.add("CONNECTORREAD");
      capabilities.add("CONNECTORWRITE");

      // tProps.put("CAPABILITIES", capabilities);
      tProps.put("TBLTYPE", type);
      tProps.put("PARTITIONS", pCount);

      setHMSClient("TestGetPartitionByNames#1", (String[])(capabilities.toArray(new String[0])));

      Table table = createTableWithCapabilities(tProps);

      List<String> partValues = new ArrayList<>();
      for (int i = 1; i <= pCount; i++) {
        partValues.add("partcol=" + i);
      }
      GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(dbName, tblName, partValues);
      List<Partition> parts = client.getPartitionsByNames(req).getPartitions();
      assertEquals("Return list size does not match expected size", pCount, parts.size());

      tblName = "test_gp_ext_bucketed_wc";
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("HIVEBUCKET2,EXTREAD,EXTWRITE");
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("BUCKETS", bucketCount);
      tProps.put("PROPERTIES", properties.toString());
      table = createTableWithCapabilities(tProps);

      req = convertToGetPartitionsByNamesRequest(dbName, tblName, partValues);
      parts = client.getPartitionsByNames(req).getPartitions();
      LOG.debug("Return list size=" + parts.size());

      for (Partition part : parts) {
        assertEquals("Partition bucket count does not match", -1, part.getSd().getNumBuckets());
      }

      // processor has capabilities
      capabilities.clear();
      capabilities.add("HIVEBUCKET2");
      setHMSClient("TestGetPartitionByNames#2", (String[])(capabilities.toArray(new String[0])));
      req = convertToGetPartitionsByNamesRequest(dbName, tblName, partValues);
      parts = client.getPartitionsByNames(req).getPartitions();

      for (Partition part : parts) {
        assertEquals("Partition bucket count does not match", bucketCount, part.getSd().getNumBuckets());
      }

      // processor has ACCEPTS_UNMODIFIED_METADATA
      capabilities.clear();
      capabilities.add("ACCEPTS_UNMODIFIED_METADATA");
      setHMSClient("TestGetPartitionByNames#3", (String[])(capabilities.toArray(new String[0])));
      req = convertToGetPartitionsByNamesRequest(dbName, tblName, partValues);
      parts = client.getPartitionsByNames(req).getPartitions();

      for (Partition part : parts) {
        assertEquals("Partition bucket count does not match", bucketCount, part.getSd().getNumBuckets());
      }

      tblName = "test_parts_mgd_insert_wc";
      properties = new StringBuilder();
      properties.append("transactional=true");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("HIVEMANAGEDINSERTREAD,HIVEMANAGEDINSERTWRITE,HIVECACHEINVALIDATE,")
                .append("HIVEMANAGEDSTATS,CONNECTORREAD,CONNECTORWRITE");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      setHMSClient("createTable", new String[] {"HIVEMANAGEDINSERTWRITE,HIVEFULLACIDWRITE"});
      table = createTableWithCapabilities(tProps);
      resetHMSClient();

      capabilities.clear();
      capabilities.add("CONNECTORREAD");
      setHMSClient("TestGetPartitionByNames#3", (String[])(capabilities.toArray(new String[0])));
      req = convertToGetPartitionsByNamesRequest(dbName, tblName, partValues);
      parts = client.getPartitionsByNames(req).getPartitions();
      assertEquals("Partition count does not match", pCount, parts.size());

      LOG.info("Test execution complete:testGetPartitionsByNames");
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testGetPartitionsByNames() failed.");
      fail("testGetPartitions failed:" + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testCreateTable() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "dbcreate";
      String tblName = "test_create_table_ext";
      TableType type = TableType.EXTERNAL_TABLE;
      StringBuilder table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("EXTERNAL=TRUE");
      Map<String, Object> tProps = new HashMap<>();
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      tProps.put("PROPERTIES", table_params.toString());

      Table table = createTableWithCapabilities(tProps);

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be EXTERNAL", "EXTERNAL_TABLE", tbl2.getTableType());

      tblName = "test_create_table_mgd_wc";
      type = TableType.MANAGED_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional_properties=default");
      tProps.put("PROPERTIES", table_params.toString());

      List<String> capabilities = new ArrayList<>();
      capabilities.add("HIVEFULLACIDWRITE");
      setHMSClient("TestCreateTableMGD#1", (String[])(capabilities.toArray(new String[0])));

      table = createTableWithCapabilities(tProps);

      // retrieve the table
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be converted to EXTERNAL", "EXTERNAL_TABLE", tbl2.getTableType());
      assertNotNull("external.table.purge is expected to be non-null", tbl2.getParameters().get("external.table.purge"));
      assertTrue("external.table.purge is expected to be true",
          tbl2.getParameters().get("external.table.purge").equalsIgnoreCase("TRUE"));
      assertTrue("Table params expected to contain original properties", tbl2.getParameters().get("key1").equals("val1"));

      resetHMSClient();

      capabilities = new ArrayList<>();
      capabilities.add("HIVEMANAGEDINSERTWRITE");
      setHMSClient("TestCreateTableMGD#2", (String[])(capabilities.toArray(new String[0])));

      table = createTableWithCapabilities(tProps);

      // retrieve the table
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be converted to EXTERNAL", "EXTERNAL_TABLE", tbl2.getTableType());
      assertNotNull("external.table.purge is expected to be non-null", tbl2.getParameters().get("external.table.purge"));
      assertTrue("external.table.purge is expected to be true",
          tbl2.getParameters().get("external.table.purge").equalsIgnoreCase("TRUE"));
      assertTrue("Table params expected to contain original properties", tbl2.getParameters().get("key1").equals("val1"));

      resetHMSClient();

      // Test for FULL ACID tables
      tblName = "test_create_table_acid_mgd_woc";
      type = TableType.MANAGED_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional=true");
      tProps.put("PROPERTIES", table_params.toString());

      try {
        table = createTableWithCapabilities(tProps);
        fail("CreateTable expected to fail, but passed for " + tblName);
      } catch (MetaException me) {
        LOG.info("Create table expected to fail as ACID table cannot be created without possessing capabilities");
      }

      capabilities = new ArrayList<>();
      capabilities.add("CONNECTORWRITE");
      setHMSClient("TestCreateTableACID#1", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        fail("Create table expected to fail but has succeeded.");
      } catch (MetaException me) {
        LOG.info("CreateTable expected to fail and has failed for " + tblName);
      }
      resetHMSClient();

      capabilities = new ArrayList<>();
      capabilities.add("HIVEFULLACIDWRITE");
      setHMSClient("TestCreateTableACID#2", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        LOG.info("Create table expected to succeed and has succeeded.");

        // retrieve the table
        tbl2 = client.getTable(dbName, tblName);
        assertEquals("TableType expected to be MANAGED_TABLE", TableType.MANAGED_TABLE.name(), tbl2.getTableType());
        assertTrue("Table params expected to contain ACID properties",
            tbl2.getParameters().get("transactional").equals("true"));
        assertTrue("Table params not expected to contain INSERT ACID properties",
            ((tbl2.getParameters().get("transactional_properties") == null) ||
                !(tbl2.getParameters().get("transactional_properties").equalsIgnoreCase("insert_only"))));
      } catch (MetaException me) {
        fail("CreateTable expected to succeed, but failed for " + tblName);
      }
      resetHMSClient();

      tblName = "test_create_table_acid_mgd_wc";
      tProps.put("TBLNAME", tblName);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional=true");
      table_params.append(";");
      table_params.append(CAPABILITIES_KEY).append("=").append("HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,")
          .append("HIVEMANAGEDSTATS,CONNECTORREAD,CONNECTORWRITE");
      tProps.put("PROPERTIES", table_params.toString());

      try {
        table = createTableWithCapabilities(tProps);
        fail("CreateTable expected to fail, but passed for " + tblName);
      } catch (MetaException me) {
        LOG.info("Create table expected to fail as ACID table cannot be created without possessing capabilities");
      }

      tblName = "test_create_table_acid_mgd_wcw";
      tProps.put("TBLNAME", tblName);
      capabilities = new ArrayList<>();
      capabilities.add("CONNECTORWRITE");
      setHMSClient("TestCreateTableACID#3", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        fail("Create table expected to fail but has succeeded.");
      } catch (MetaException me) {
        LOG.info("CreateTable expected to fail and has failed for " + tblName);
      }
      resetHMSClient();

      tblName = "test_create_table_acid_mgd_whfaw";
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional=true");
      table_params.append(";");
      table_params.append(CAPABILITIES_KEY).append("=").append("HIVEFULLACIDREAD,HIVEFULLACIDWRITE,HIVECACHEINVALIDATE,")
          .append("HIVEMANAGEDSTATS,CONNECTORREAD,CONNECTORWRITE");
      tProps.put("TBLNAME", tblName);
      capabilities = new ArrayList<>();
      capabilities.add("HIVEFULLACIDWRITE");
      setHMSClient("TestCreateTableACID#4", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        LOG.info("Create table expected to succeed and has succeeded.");

        // retrieve the table
        tbl2 = client.getTable(dbName, tblName);
        assertEquals("TableType expected to be MANAGED_TABLE", TableType.MANAGED_TABLE.name(), tbl2.getTableType());
        assertTrue("Table params expected to contain ACID properties",
            tbl2.getParameters().get("transactional").equals("true"));
        assertTrue("Table params not expected to contain INSERT ACID properties",
            ((tbl2.getParameters().get("transactional_properties") == null) ||
                !(tbl2.getParameters().get("transactional_properties").equalsIgnoreCase("insert_only"))));
        assertEquals("Expected access of type READONLY", ACCESSTYPE_READWRITE, tbl2.getAccessType());
        assertNull("Expected null required write capabilities", tbl2.getRequiredWriteCapabilities());
      } catch (MetaException me) {
        fail("CreateTable expected to succeed, but failed for " + tblName);
      }
      resetHMSClient();

      tblName = "test_create_table_insert_mgd_woc";
      type = TableType.MANAGED_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional=true");
      table_params.append(";");
      table_params.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", table_params.toString());

      try {
        table = createTableWithCapabilities(tProps);
        fail("CreateTable expected to fail, but passed for " + tblName);
      } catch (MetaException me) {
        LOG.info("Create table expected to fail as ACID table cannot be created without possessing capabilities");
      }

      capabilities = new ArrayList<>();
      capabilities.add("CONNECTORWRITE");
      setHMSClient("TestCreateTableMGD#1", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        fail("Create table expected to fail but has succeeded.");
      } catch (MetaException me) {
        LOG.info("CreateTable expected to fail and has failed for " + tblName);
      }
      resetHMSClient();

      capabilities = new ArrayList<>();
      capabilities.add("HIVEMANAGEDINSERTWRITE");
      setHMSClient("TestCreateTableMGD#2", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        LOG.info("Create table expected to succeed and has succeeded.");

        // retrieve the table
        tbl2 = client.getTable(dbName, tblName);
        assertEquals("TableType expected to be MANAGED_TABLE", TableType.MANAGED_TABLE.name(), tbl2.getTableType());
        assertTrue("Table params expected to contain ACID properties",
            tbl2.getParameters().get("transactional").equals("true"));
        assertTrue("Table params expected to contain ACID properties",
            tbl2.getParameters().get("transactional_properties").equals("insert_only"));
      } catch (MetaException me) {
        fail("CreateTable expected to succeed, but failed for " + tblName);
      }
      resetHMSClient();

      // table has capabilities
      tblName = "test_view_wc";
      table_params = new StringBuilder();
      table_params.append(CAPABILITIES_KEY).append("=").append("HIVESQL");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", table_params.toString());

      try {
        table = createTableWithCapabilities(tProps);
        LOG.info("Create view is expected to succeed and has succeeded"); // no transformation for views
      } catch (Exception e) {
        LOG.info("Create view expected to succeed but has failed.");
        fail("Create view expected to succeed but has failed. <" + e.getMessage() +">");
      }
      resetHMSClient();
    } catch (Exception e) {
      System.err.println(org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.err.println("testCreateTable() failed.");
      fail("testCreateTable failed:" + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerAlterTable() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "dbalter";
      String tblName = "test_alter_mgd_table";
      TableType type = TableType.MANAGED_TABLE;
      StringBuilder table_params = new StringBuilder();
      table_params.append("key1=val1");
      Map<String, Object> tProps = new HashMap<>();
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      tProps.put("PROPERTIES", table_params.toString());

      Table table = createTableWithCapabilities(tProps); // should be converted to external table

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be EXTERNAL", "EXTERNAL_TABLE", tbl2.getTableType());
      String tableLocation = tbl2.getSd().getLocation();
      int idx = (tableLocation.indexOf(":") > 0) ? tableLocation.indexOf(":") : 0;
      tableLocation = tableLocation.substring(idx+1);
      String expectedPath = ext_wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(DATABASE_WAREHOUSE_SUFFIX)
          .concat(File.separator).concat(tblName);
      assertEquals("Table location", expectedPath, tableLocation);

      String newLocation = wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(File.separator)
          .concat(tblName);
      tbl2.getSd().setLocation(newLocation);
      try {
        client.alter_table(dbName, tblName, tbl2);
        fail("alter_table expected to fail due to location:" + newLocation);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.info("alter_table failed with exception, as expected");
      }

      // retrieve the table and check that the location was not altered
      tbl2 = client.getTable(dbName, tblName);
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to be in external warehouse", tableLocation, tbl2.getSd().getLocation().substring(idx+1));

      newLocation = tableLocation.concat("_new");
      table.getSd().setLocation((new File(newLocation)).getCanonicalPath());
      try {
        client.alter_table(dbName, tblName, table);
        LOG.info("alter_table with new location succeeded as expected");
      } catch (Exception e) {
        fail("alter_table expected to succeed with new location:" + newLocation);
      }

      // retrieve the table and check that the location was altered
      tbl2 = client.getTable(dbName, tblName);
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to be in external warehouse", newLocation, tbl2.getSd().getLocation().substring(idx+1));

      tblName = "test_create_insert_table";
      type = TableType.MANAGED_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", table_params.toString());

      List<String> capabilities = new ArrayList<>();
      capabilities.add("HIVEMANAGEDINSERTWRITE");
      setHMSClient("TestAlterTableMGD#1", (String[])(capabilities.toArray(new String[0])));

      table = createTableWithCapabilities(tProps);

      // retrieve the table
      tbl2 = client.getTable(dbName, tblName);
      tableLocation = tbl2.getSd().getLocation();
      idx = (tableLocation.indexOf(":") > 0) ? tableLocation.indexOf(":") : 0;
      tableLocation = tableLocation.substring(idx+1);

      assertEquals("Table type expected to be MANAGED", "MANAGED_TABLE", tbl2.getTableType());

      newLocation = ext_wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(File.separator)
          .concat(tblName);
      table.getSd().setLocation(newLocation);
      try {
        client.alter_table(dbName, tblName, table);
        fail("alter_table expected to fail but succeeded with new location:" + newLocation);
      } catch (Exception e) {
        LOG.info("alter_table failed with exception as expected");
      }

      // retrieve the table and ensure location has not been changed.
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be MANAGED", "MANAGED_TABLE", tbl2.getTableType());
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to remain unaltered", tableLocation, tbl2.getSd().getLocation().substring(idx+1));

      newLocation = tableLocation + "_new";
      table.getSd().setLocation(newLocation);
      try {
        client.alter_table(dbName, tblName, table);
        LOG.info("alter_table succeeded with new location as expected");
      } catch (Exception e) {
        fail("alter_table expected to succeed but failed with new location:" + newLocation);
      }
      // retrieve the table and ensure location has not been changed.
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be MANAGED", "MANAGED_TABLE", tbl2.getTableType());
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to be new location", newLocation, tbl2.getSd().getLocation().substring(idx+1));
      resetHMSClient();

      tblName = "test_create_acid_table";
      type = TableType.MANAGED_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("transactional=true");
      tProps.put("PROPERTIES", table_params.toString());

      capabilities = new ArrayList<>();
      capabilities.add("HIVEFULLACIDWRITE");
      setHMSClient("TestAlterTableMGD#1", (String[])(capabilities.toArray(new String[0])));

      table = createTableWithCapabilities(tProps);

      // retrieve the table
      tbl2 = client.getTable(dbName, tblName);
      tableLocation = tbl2.getSd().getLocation();
      idx = (tableLocation.indexOf(":") > 0) ? tableLocation.indexOf(":") : 0;
      tableLocation = tableLocation.substring(idx+1);

      assertEquals("Table type expected to be MANAGED", "MANAGED_TABLE", tbl2.getTableType());

      newLocation = ext_wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(File.separator)
          .concat(tblName);
      table.getSd().setLocation(newLocation);
      try {
        client.alter_table(dbName, tblName, table);
        fail("alter_table expected to fail but succeeded with new location:" + newLocation);
      } catch (Exception e) {
        LOG.info("alter_table failed with exception as expected");
      }

      // retrieve the table and ensure location has not been changed.
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be MANAGED", "MANAGED_TABLE", tbl2.getTableType());
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to remain unaltered", tableLocation, tbl2.getSd().getLocation().substring(idx+1));

      newLocation = tableLocation + "_new";
      table.getSd().setLocation(newLocation);
      try {
        client.alter_table(dbName, tblName, table);
        LOG.info("alter_table succeeded with new location as expected");
      } catch (Exception e) {
        e.printStackTrace();
        fail("alter_table expected to succeed but failed with new location:" + newLocation);
      }
      // retrieve the table and ensure location has not been changed.
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table type expected to be MANAGED", "MANAGED_TABLE", tbl2.getTableType());
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to be new location", newLocation, tbl2.getSd().getLocation().substring(idx+1));
    } catch (Exception e) {
      System.err.println(org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.err.println("testAlterTable() failed.");
      fail("testAlterTable failed:" + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerAlterTableWithoutLocationChangeDoesntValidateLocation() throws Exception {
    try {
      resetHMSClient();
      String dbName = "dbalter";
      String tblName = "test_alter_mgd_table";
      TableType type = TableType.MANAGED_TABLE;
      Map<String, Object> tProps = new HashMap<>();
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      tProps.put("LOCATION", wh.getAbsolutePath().concat(File.separator).concat(dbName).concat(File.separator).concat(tblName));
      createTableWithCapabilities(tProps);
      client.alter_table(dbName, tblName, client.getTable(dbName, tblName));
    } finally {
      resetHMSClient();
    }
  }


  @Test
  public void testTransformerDatabase() throws Exception {
    try {
      resetHMSClient();

      String dbName = "testdb";
      String dbWithLocation = "dbWithLocation";
      try {
        silentDropDatabase(dbName);
        silentDropDatabase(dbWithLocation);
      } catch (Exception e) {
        LOG.info("Drop database failed for " + dbName);
      }

      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);

      List<String> capabilities = new ArrayList<>();
      capabilities.add("EXTWRITE");
      setHMSClient("TestGetDatabaseEXTWRITE", (String[])(capabilities.toArray(new String[0])));
      Database db = client.getDatabase(dbName);
      assertTrue("Database location not as expected:actual=" + db.getLocationUri(),
          db.getLocationUri().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));

      capabilities = new ArrayList<>();
      capabilities.add("HIVEFULLACIDWRITE");
      setHMSClient("TestGetDatabaseACIDWRITE", (String[])(capabilities.toArray(new String[0])));

      db = client.getDatabase(dbName);
      assertTrue("Database location expected to be external warehouse:actual=" + db.getLocationUri(),
          db.getLocationUri().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));
      resetHMSClient();

      capabilities = new ArrayList<>();
      capabilities.add("HIVEMANAGEDINSERTWRITE");
      setHMSClient("TestGetDatabaseINSERTWRITE", (String[])(capabilities.toArray(new String[0])));

      db = client.getDatabase(dbName);
      assertTrue("Database location expected to be external warehouse:actual=" + db.getLocationUri(),
          db.getLocationUri().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));
      resetHMSClient();

      Warehouse wh = new Warehouse(conf);
      String mgdPath = wh.getDefaultDatabasePath(dbWithLocation, false).toString();
      new DatabaseBuilder()
          .setName(dbWithLocation)
          .setLocation(mgdPath)
          .create(client, conf);

      capabilities = new ArrayList<>();
      capabilities.add("EXTWRITE");
      setHMSClient("TestGetDatabaseWithLocation", (String[])(capabilities.toArray(new String[0])));

      db = client.getDatabase(dbWithLocation);
      assertTrue("Database location expected to be external warehouse:actual=" + db.getLocationUri(),
          db.getLocationUri().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));
      assertNull("Database managed location expected to be null", db.getManagedLocationUri());
      resetHMSClient();

      capabilities.add("HIVEMANAGEDINSERTWRITE");
      setHMSClient("TestGetDatabaseWithLocation#2", (String[])(capabilities.toArray(new String[0])));

      db = client.getDatabase(dbWithLocation);
      assertTrue("Database location expected to be external warehouse", db.getLocationUri().contains(conf.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname())));
      assertEquals("Database managedLocationUri expected to be set to locationUri", mgdPath, db.getManagedLocationUri());
      resetHMSClient();
    } catch (Exception e) {
      System.err.println(org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.err.println("testTransformerDatabase() failed.");
      fail("testTransformerDatabase failed:" + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerMultiTable() throws Exception {
    try {
      resetHMSClient();

      final String dbName = "testdb";
      final String ext_table = "ext_table";
      final String acidTable = "managed_table";
      final String part_ext = "part_ext";

      try {
        silentDropDatabase(dbName);
      } catch (Exception e) {
        LOG.info("Drop database failed for " + dbName);
      }

      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);

      List<String> capabilities = new ArrayList<>();
      capabilities.add("HIVEFULLACIDWRITE");
      setHMSClient("TestTransformerMultiTable", (String[])(capabilities.toArray(new String[0])));

      Map<String, Object> tProps = new HashMap<>();
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", ext_table);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("DROPDB", Boolean.FALSE);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      tProps.put("TBLNAME", acidTable);
      tProps.put("TBLTYPE", TableType.MANAGED_TABLE);
      properties = new StringBuilder();
      properties.append("transactional").append("=").append("true");
      tProps.put("PROPERTIES", properties.toString());
      tProps.put("DROPDB", Boolean.FALSE);
      tbl = createTableWithCapabilities(tProps);

      tProps = new HashMap<>();
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", part_ext);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("DROPDB", Boolean.FALSE);
      properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      tProps.put("PROPERTIES", properties.toString());
      tProps.put("PARTITIONS", 10);
      tbl = createTableWithCapabilities(tProps);

      resetHMSClient();
      capabilities = new ArrayList<>();
      capabilities.add("EXTWRITE");
      capabilities.add("EXTREAD");
      setHMSClient("TestTransformerMultiTable", (String[])(capabilities.toArray(new String[0])));
      tbl = client.getTable(dbName, acidTable);
      assertEquals("AccessType does not match", ACCESSTYPE_NONE, tbl.getAccessType());
      tbl = client.getTable(dbName, acidTable);
      assertEquals("AccessType does not match", ACCESSTYPE_NONE, tbl.getAccessType());

      tbl = client.getTable(dbName, ext_table);
      assertEquals("AccessType does not match", ACCESSTYPE_READWRITE, tbl.getAccessType());
      tbl = client.getTable(dbName, acidTable);
      assertEquals("AccessType does not match", ACCESSTYPE_NONE, tbl.getAccessType());
      resetHMSClient();

      capabilities = new ArrayList<>();
      capabilities.add("EXTWRITE");
      capabilities.add("EXTREAD");
      capabilities.add("HIVESQL");
      capabilities.add("SPARKSQL");
      capabilities.add("HIVEBUCKET2");
      setHMSClient("TestTransformerMultiTable", (String[])(capabilities.toArray(new String[0])));
      tbl = client.getTable(dbName, acidTable);
      assertEquals("AccessType does not match", ACCESSTYPE_NONE, tbl.getAccessType());
      tbl = client.getTable(dbName, acidTable);
      assertEquals("AccessType does not match", ACCESSTYPE_NONE, tbl.getAccessType());

      tbl = client.getTable(dbName, part_ext);
      assertEquals("AccessType does not match", ACCESSTYPE_READWRITE, tbl.getAccessType());
      tbl = client.getTable(dbName, acidTable);
      assertEquals("AccessType does not match", ACCESSTYPE_NONE, tbl.getAccessType());
      resetHMSClient();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.err.println("testTransformerDatabase() failed.");
      fail("testTransformerDatabase failed:" + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  @Test
  public void testTransformerWithNonHiveCatalogs() throws Exception {
    try {
      resetHMSClient();
      Table table, tbl2;
      String tblName = "non_hive_exttable";
      String sparkDbName = "sparkdb";
      String catalog = "sparkcat";
      Map<String, Object> tProps = new HashMap<>();
      TableType type = TableType.EXTERNAL_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      tProps.put("CATALOG", catalog);
      tProps.put("DBNAME", sparkDbName);
      StringBuilder table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      table_params.append("EXTERNAL").append("=").append("TRUE");
      tProps.put("PROPERTIES", table_params.toString());

      List<String> capabilities = new ArrayList<>();
      setHMSClient("TestCreateTableNonHive#1", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        LOG.info("Create non-hive table is expected to succeed and has succeeded"); // no transformation for views
      } catch (Exception e) {
        fail("Create non-hive table expected to succeed but has failed. <" + e.getMessage() +">");
      }

      tbl2 = client.getTable(catalog, sparkDbName, tblName);
      assertEquals("TableName expected to be " + tblName, tblName, tbl2.getTableName());
      assertEquals("TableType expected to be EXTERNAL", TableType.EXTERNAL_TABLE.name(), tbl2.getTableType());
      assertNull("Table's ReadCapabilities is expected to be null", tbl2.getRequiredReadCapabilities());
      assertNull("Table's WriteCapabilities is expected to be null", tbl2.getRequiredWriteCapabilities());

      String newLocation = wh.getAbsolutePath().concat(File.separator).concat(sparkDbName).concat(File.separator)
          .concat(tblName);
      tbl2.getSd().setLocation(newLocation);

      setHMSClient("TestAlterTableNonHive#1", (String[])(capabilities.toArray(new String[0])));
      try {
        client.alter_table(catalog, sparkDbName, tblName, tbl2);
        LOG.info("alter_table succeeded with new location in managed warehouse as expected");
      } catch (Exception e) {
        fail("alter_table expected to succeed but failed with new location:" + newLocation);
      }

      tbl2 = client.getTable(catalog, sparkDbName, tblName);
      assertEquals("TableType expected to be EXTERNAL", TableType.EXTERNAL_TABLE.name(), tbl2.getTableType());
      int idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to be in external warehouse", newLocation, tbl2.getSd().getLocation().substring(idx+1));

      tblName = "non_hive_mgdtable";
      tProps = new HashMap<>();
      type = TableType.MANAGED_TABLE;
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", type);
      tProps.put("CATALOG", catalog);
      tProps.put("DBNAME", sparkDbName);
      tProps.put("DROPDB", Boolean.FALSE);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      tProps.put("PROPERTIES", table_params.toString());

      capabilities.add("CONNECTORWRITE");
      setHMSClient("TestCreateTableNonHive#2", (String[])(capabilities.toArray(new String[0])));

      try {
        table = createTableWithCapabilities(tProps);
        LOG.info("Create non-hive MGD table is expected to succeed and has succeeded"); // no transformation for views
      } catch (Exception e) {
        fail("Create non-hive MGD table expected to succeed but has failed. <" + e.getMessage() +">");
      }

      tbl2 = client.getTable(catalog, sparkDbName, tblName);
      assertEquals("TableName expected to be " + tblName, tblName, tbl2.getTableName());
      assertEquals("TableType expected to be MANAGED", TableType.MANAGED_TABLE.name(), tbl2.getTableType());
      assertNull("Table's ReadCapabilities is expected to be null", tbl2.getRequiredReadCapabilities());
      assertNull("Table's WriteCapabilities is expected to be null", tbl2.getRequiredWriteCapabilities());


      // TESTS to ensure AlterTable does not go thru translation for non-hive catalog objects
      setHMSClient("TestAlterTableNonHive#2", (String[])(capabilities.toArray(new String[0])));
      tbl2 = client.getTable(catalog, sparkDbName, tblName);
      newLocation = ext_wh.getAbsolutePath().concat(File.separator).concat(sparkDbName).concat(File.separator)
          .concat(tblName);
      tbl2.getSd().setLocation(newLocation);

      try {
        client.alter_table(catalog, sparkDbName, tblName, tbl2);
        LOG.info("alter_table succeeded with new location in external warehouse as expected");
      } catch (Exception e) {
        fail("alter_table expected to succeed but failed with new location:" + newLocation);
      }

      tbl2 = client.getTable(catalog, sparkDbName, tblName);
      assertEquals("TableType expected to be MANAGED", TableType.MANAGED_TABLE.name(), tbl2.getTableType());
      idx = (tbl2.getSd().getLocation().indexOf(":") > 0) ? tbl2.getSd().getLocation().indexOf(":") : 0;
      assertEquals("Table location expected to be in managed warehouse", newLocation, tbl2.getSd().getLocation().substring(idx+1));

      // Test getTablesExt with many tables.
      sparkDbName = "sparkdbext";
      tblName = "test_get_tables_ext";
      int count = 10;

      tProps = new HashMap<>();
      capabilities = new ArrayList<>();
      capabilities.add("EXTREAD");
      tProps.put("CATALOG", catalog);
      tProps.put("DBNAME", sparkDbName);
      tProps.put("TBLNAME", tblName);
      type = TableType.MANAGED_TABLE;
      tProps.put("TABLECOUNT", count);
      tProps.put("TBLTYPE", type);
      table_params = new StringBuilder();
      table_params.append("key1=val1");
      table_params.append(";");
      tProps.put("PROPERTIES", table_params.toString());

      setHMSClient("test_get_tables_ext", (String[])(capabilities.toArray(new String[0])));

      List<String> tables = createTables(tProps);
      int requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.PROCESSOR_CAPABILITIES)).bitValue();
      List<ExtendedTableInfo> extTables = client.getTablesExt(catalog, sparkDbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size:extTables", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertNull("Return object should not have read capabilities", tableInfo.getRequiredReadCapabilities());
        assertNull("Return object should not have write capabilities", tableInfo.getRequiredWriteCapabilities());
        assertEquals("AccessType not expected to be set", 0, tableInfo.getAccessType());
      }

      requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.ACCESS_TYPE)).bitValue();
      extTables = client.getTablesExt(catalog, sparkDbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertNull("Return object should not have read capabilities", tableInfo.getRequiredReadCapabilities());
        assertNull("Return object should not have write capabilities", tableInfo.getRequiredWriteCapabilities());
        assertTrue("AccessType not expected to be set", tableInfo.getAccessType() <= 0);
      }

      requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.ALL)).bitValue();
      extTables = client.getTablesExt(catalog, sparkDbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertTrue("AccessType not expected to be set", tableInfo.getAccessType() <= 0);
      }

      extTables = client.getTablesExt(catalog, sparkDbName, "*", requestedFields, (count - 3));
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", (count - 3), extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertTrue("AccessType not expected to be set", tableInfo.getAccessType() <= 0);
      }

      extTables = client.getTablesExt(catalog, sparkDbName, "*", requestedFields, -1);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());

      count = 300;
      tProps.put("TBLNAME", "test_limit");
      tProps.put("TABLECOUNT", count);
      tables = createTables(tProps);
      assertEquals("Unexpected number of tables created", count, tables.size());

      extTables = client.getTablesExt(catalog, sparkDbName, "test_limit*", requestedFields, count);
      assertEquals("Unexpected number of tables returned", count, extTables.size());

      extTables = client.getTablesExt(catalog, sparkDbName, "test_limit*", requestedFields, (count/2));
      assertEquals("Unexpected number of tables returned", (count/2), extTables.size());

      extTables = client.getTablesExt(catalog, sparkDbName, "test_limit*", requestedFields, 1);
      assertEquals("Unexpected number of tables returned", 1, extTables.size());

    } catch (Exception e) {
      System.err.println(org.apache.hadoop.util.StringUtils.stringifyException(e));
      System.err.println("testCreateTable() failed.");
      fail("testCreateTable failed:" + e.getMessage());
    } finally {
      resetHMSClient();
    }
  }

  private List<String> createTables(Map<String, Object> props) throws Exception {
    int count = ((Integer)props.get("TABLECOUNT")).intValue();
    String tblName  = (String)props.get("TBLNAME");
    List<String> caps = (List<String>)props.get("CAPABILITIES");
    StringBuilder table_params = new StringBuilder();
    table_params.append((String)props.get("PROPERTIES"));
    if (caps != null)
      table_params.append(CAPABILITIES_KEY).append("=").append(String.join(",", caps));
    props.put("PROPERTIES", table_params.toString());
    List<String> ret = new ArrayList<>();
    String newtblName = null;

    try {
      for (int i = 0; i < count; i++) {
        newtblName = tblName + i;
        props.put("TBLNAME", newtblName);
        createTableWithCapabilities(props);
        props.put("DROPDB", Boolean.FALSE);
        ret.add(newtblName);
      }
    } catch (Exception e) {
      LOG.warn("Create table failed for " + newtblName);
    }
    return ret;
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
        LOG.info("Catalog does not exist, creating a new one");
        try {
          if (cat == null) {
            cat = new Catalog();
            cat.setName(catalog.toLowerCase());
            Warehouse wh = new Warehouse(conf);
            cat.setLocationUri(wh.getWhRootExternal().toString() + File.separator + catalog);
            cat.setDescription("Non-hive catalog");
            client.createCatalog(cat);
            LOG.info("Catalog " + catalog + " created");
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
    p.getSd().setNumBuckets(table.getSd().getNumBuckets());
    client.add_partition(p);
  }
}

