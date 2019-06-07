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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient.GetTablesRequestBuilder;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ExtendedTableInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTablesExtRequestFields;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_NONE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READONLY;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_READWRITE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.ACCESSTYPE_WRITEONLY;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHiveMetastoreTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetastoreTransformer.class);
  protected static HiveMetaStoreClient client;
  protected static Configuration conf;

  protected static boolean isThriftClient = false;
  private static final String CAPABILITIES_KEY = "OBJCAPABILITIES";

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();

    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, "org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer");
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
   *   1) Old table with no capabilities
   *   2a) New table with capabilities with no client requirements
   *   2b) New table with capabilities with no matching client requirements
   *   2c) New table with capabilities with partial match requirements
   *   2d) New table with capabilities with full match requirements
   */
  @Test
  public void testTransformerExternalTable() throws Exception {
    try {
      final String dbName = "db1";
      String basetblName = "oldstyleexttable";
      Map<String, Object> tProps = new HashMap<>();
      int buckets = 32;

      String tblName = basetblName;
      tProps.put("DBNAME", dbName);
      tProps.put("TBLNAME", tblName);
      tProps.put("TBLTYPE", TableType.EXTERNAL_TABLE);
      tProps.put("BUCKETS", buckets);
      StringBuilder properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      tProps.put("PROPERTIES", properties.toString());
      Table tbl = createTableWithCapabilities(tProps);

      // retrieve the table
      Table tbl2 = client.getTable(dbName, tblName);
      LOG.info("Table=" + tblName + ",Access=" + tbl2.getAccessType());
      assertEquals("Created and retrieved tables do not match:" + tbl2.getTableName() + ":" + tblName,
          tbl2.getTableName(), tblName);
      assertEquals("TableType mismatch", TableType.EXTERNAL_TABLE.name(), tbl2.getTableType());
      assertEquals(buckets, tbl2.getSd().getNumBuckets()); // no transformation
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // old client, AccessType not set

      setHMSClient("testTranformerExternalTable", (new String[] { "HIVEBUCKET2" }));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Expected buckets does not match:", buckets, tbl2.getSd().getNumBuckets()); // no transformation
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

      resetHMSClient();

      setHMSClient("testTransformerExternalTableRO", (new String[] { "EXTREAD", "EXTWRITE"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      assertEquals(-1, tbl2.getSd().getNumBuckets()); // client has no HIVEBUCKET2 capability, remove bucketing info

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

      resetHMSClient();

      tblName = "test_ext_sparkbucketed_wc";
      properties = new StringBuilder();
      properties.append("EXTERNAL").append("=").append("TRUE");
      properties.append(";");
      properties.append(CAPABILITIES_KEY).append("=").append("SPARKDECIMAL,SPARKBUCKET,EXTREAD");
      tProps.put("TBLNAME", tblName);
      tProps.put("PROPERTIES", properties.toString());
      tProps.put("BUCKETS", buckets);
      tbl = createTableWithCapabilities(tProps);

      setHMSClient("testTransformerExternalTableSpark", (new String[] { "HIVEFULLACIDREAD", "CONNECTORREAD"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType()); // requires EXTREAD for RO
      assertEquals(buckets, tbl2.getSd().getNumBuckets());

      setHMSClient("testTransformerExternalTableSpark", (new String[] { "EXTREAD", "CONNECTORREAD"}));
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match the expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType()); // requires EXTREAD for RO
      assertEquals(buckets, tbl2.getSd().getNumBuckets());

      setHMSClient("testTransformerExternalTableSpark", (new String[] { "SPARKBUCKET", "SPARKDECIMAL", "EXTREAD" }));
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
      assertEquals("TableType mismatch", TableType.MANAGED_TABLE.name(), tbl2.getTableType());
      assertEquals(-1, tbl2.getSd().getNumBuckets());
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // no translation to be done, so accessType not set

      // managed table with no capabilities
      tblName = "test_mgd_insert_woc";
      StringBuilder properties = new StringBuilder();
      tProps.put("TBLNAME", tblName);
      properties.append("transactional=true");
      properties.append(";");
      properties.append("transactional_properties=insert_only");
      tProps.put("PROPERTIES", properties.toString());
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType()); // no transformation

      setHMSClient("testMGDwithConnectorRead", new String[] {"CONNECTORREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithInsertRead", new String[] {"HIVEMANAGEDINSERTREAD"});
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READONLY, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwithInsertWrite", new String[] {"HIVEMANAGEDINSERTWRITE"});
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
      tbl = createTableWithCapabilities(tProps);

      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          0, tbl2.getAccessType());

      setHMSClient("testMGDwithAllWrites", new String[] { "HIVEMANAGEDINSERTWRITE", "HIVECACHEINVALIDATE",
          "HIVEMANAGEDSTATS" , "CONNECTORWRITE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_READWRITE, tbl2.getAccessType());
      resetHMSClient();

      setHMSClient("testMGDwith1MissingWrite", new String[] {"HIVEMANAGEDINSERTREAD", "HIVEMANAGEDINSERTWRITE",
          "HIVECACHEINVALIDATE" });
      tbl2 = client.getTable(dbName, tblName);
      assertEquals("Table access type does not match expected value:" + tblName,
          ACCESSTYPE_NONE, tbl2.getAccessType());
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
      tbl = createTableWithCapabilities(tProps);

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
      tbl = createTableWithCapabilities(tProps);

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
        assertEquals("Capability set size does not match", capabilities.size(), tableInfo.getProcessorCapabilities().size());
        assertEquals("AccessType not expected to be set", 0, tableInfo.getAccessType());
      }

      requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.ACCESS_TYPE)).bitValue();
      extTables = client.getTablesExt(null, dbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertNull("Return value should not contain capabilities", tableInfo.getProcessorCapabilities());
        assertTrue("AccessType expected to be set", tableInfo.getAccessType() > 0);
      }

      requestedFields = (new GetTablesRequestBuilder().with(GetTablesExtRequestFields.ALL)).bitValue();
      extTables = client.getTablesExt(null, dbName, "*", requestedFields, 2000);
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", count, extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertEquals("Capability set size does not match", capabilities.size(), tableInfo.getProcessorCapabilities().size());
        assertTrue("AccessType expected to be set", tableInfo.getAccessType() > 0);
      }

      extTables = client.getTablesExt(null, dbName, "*", requestedFields, (count - 3));
      LOG.debug("Return list size=" + extTables.size() + ",bitValue=" + requestedFields);
      assertEquals("Return list size does not match expected size", (count - 3), extTables.size());
      for (ExtendedTableInfo tableInfo : extTables) {
        assertEquals("Capability set size does not match", capabilities.size(), tableInfo.getProcessorCapabilities().size());
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

      List<String> partNames = new ArrayList<>();
      List<String> partValues = new ArrayList<>();
      for (int i = 1; i <= pCount; i++) {
        partValues.add("partcol=" + i);
      }
      List<Partition> parts = client.getPartitionsByNames(dbName, tblName, partValues, false);
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

      parts = client.getPartitionsByNames(dbName, tblName, partValues, false);
      LOG.debug("Return list size=" + parts.size());

      for (Partition part : parts) {
        assertEquals("Partition bucket count does not match", -1, part.getSd().getNumBuckets());
      }

      // processor has capabilities
      capabilities.clear();
      capabilities.add("HIVEBUCKET2");
      setHMSClient("TestGetPartitionByNames#2", (String[])(capabilities.toArray(new String[0])));
      parts = client.getPartitionsByNames(dbName, tblName, partValues, false);

      for (Partition part : parts) {
        assertEquals("Partition bucket count does not match", -1, part.getSd().getNumBuckets());
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
      table = createTableWithCapabilities(tProps);

      capabilities.clear();
      capabilities.add("CONNECTORREAD");
      setHMSClient("TestGetPartitionByNames#3", (String[])(capabilities.toArray(new String[0])));
      parts = client.getPartitionsByNames(dbName, tblName, partValues, false);
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

  private List<String> createTables(Map<String, Object> props) throws Exception {
    int count = ((Integer)props.get("TABLECOUNT")).intValue();
    String tblName  = (String)props.get("TBLNAME");
    List<String> caps = (List<String>)props.get("CAPABILITIES");
    StringBuilder table_params = new StringBuilder();
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
      String catalog = (String)props.getOrDefault("CATALOG", "testcat");
      String dbName = (String)props.getOrDefault("DBNAME", "simpdb");
      String tblName = (String)props.getOrDefault("TBLNAME", "test_table");
      TableType type = (TableType)props.getOrDefault("TBLTYPE", TableType.MANAGED_TABLE);
      int buckets = ((Integer)props.getOrDefault("BUCKETS", -1)).intValue();
      String properties = (String)props.getOrDefault("PROPERTIES", "");
      boolean dropDb = ((Boolean)props.getOrDefault("DROPDB", Boolean.TRUE)).booleanValue();
      int partitionCount = ((Integer)props.getOrDefault("PARTITIONS", 0)).intValue();

      final String typeName = "Person";

      Map<String,String> table_params = new HashMap();
      if (properties.length() > 0) {
        String[] propArray = properties.split(";");
        for (String prop : propArray) {
          String[] keyValue = prop.split("=");
          table_params.put(keyValue[0], keyValue[1]);
        }
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
          .setDbName(dbName)
          .setTableName(tblName)
          .setCols(typ1.getFields())
          .setType(type.name())
          .setNumBuckets(buckets)
          .setTableParams(table_params)
          .addBucketCol("name")
          .addStorageDescriptorParam("test_param_1", "Use this for comments etc");

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
      LOG.info("Table " + tblName + " created:type=" + type.name());

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
        tbl = client.getTable(dbName, tblName);
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

