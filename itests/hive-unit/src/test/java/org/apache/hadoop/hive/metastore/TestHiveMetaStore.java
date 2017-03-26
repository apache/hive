/**
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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public abstract class TestHiveMetaStore extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetaStore.class);
  protected static HiveMetaStoreClient client;
  protected static HiveConf hiveConf;
  protected static Warehouse warehouse;
  protected static boolean isThriftClient = false;

  private static final String TEST_DB1_NAME = "testdb1";
  private static final String TEST_DB2_NAME = "testdb2";

  private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;

  @Override
  protected void setUp() throws Exception {
    hiveConf = new HiveConf(this.getClass());
    warehouse = new Warehouse(hiveConf);

    // set some values to use for getting conf. vars
    hiveConf.set("hive.metastore.metrics.enabled","true");
    hiveConf.set("hive.key1", "value1");
    hiveConf.set("hive.key2", "http://www.example.com");
    hiveConf.set("hive.key3", "");
    hiveConf.set("hive.key4", "0");

    hiveConf.setIntVar(ConfVars.METASTORE_BATCH_RETRIEVE_MAX, 2);
    hiveConf.setIntVar(ConfVars.METASTORE_LIMIT_PARTITION_REQUEST, DEFAULT_LIMIT_PARTITION_REQUEST);
  }

  public void testNameMethods() {
    Map<String, String> spec = new LinkedHashMap<String, String>();
    spec.put("ds", "2008-07-01 14:13:12");
    spec.put("hr", "14");
    List<String> vals = new ArrayList<String>();
    for(String v : spec.values()) {
      vals.add(v);
    }
    String partName = "ds=2008-07-01 14%3A13%3A12/hr=14";

    try {
      List<String> testVals = client.partitionNameToVals(partName);
      assertTrue("Values from name are incorrect", vals.equals(testVals));

      Map<String, String> testSpec = client.partitionNameToSpec(partName);
      assertTrue("Spec from name is incorrect", spec.equals(testSpec));

      List<String> emptyVals = client.partitionNameToVals("");
      assertTrue("Values should be empty", emptyVals.size() == 0);

      Map<String, String> emptySpec =  client.partitionNameToSpec("");
      assertTrue("Spec should be empty", emptySpec.size() == 0);
    } catch (Exception e) {
      assert(false);
    }
  }

  /**
   * tests create table and partition and tries to drop the table without
   * droppping the partition
   *
   * @throws Exception
   */
  public void testPartition() throws Exception {
    partitionTester(client, hiveConf);
  }

  public static void partitionTester(HiveMetaStoreClient client, HiveConf hiveConf)
    throws Exception {
    try {
      String dbName = "compdb";
      String tblName = "comptbl";
      String typeName = "Person";
      List<String> vals = makeVals("2008-07-01 14:13:12", "14");
      List<String> vals2 = makeVals("2008-07-01 14:13:12", "15");
      List<String> vals3 = makeVals("2008-07-02 14:13:12", "15");
      List<String> vals4 = makeVals("2008-07-03 14:13:12", "151");

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      Database db = new Database();
      db.setName(dbName);
      client.createDatabase(db);
      db = client.getDatabase(dbName);
      Path dbPath = new Path(db.getLocationUri());
      FileSystem fs = FileSystem.get(dbPath.toUri(), hiveConf);
      boolean inheritPerms = hiveConf.getBoolVar(
          HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
      FsPermission dbPermission = fs.getFileStatus(dbPath).getPermission();
      if (inheritPerms) {
         //Set different perms for the database dir for further tests
         dbPermission = new FsPermission((short)488);
         fs.setPermission(dbPath, dbPermission);
      }

      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<FieldSchema>(2));
      typ1.getFields().add(
          new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));
      client.createType(typ1);

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(typ1.getFields());
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters()
          .put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.setSortCols(new ArrayList<Order>());
      sd.setStoredAsSubDirectories(false);
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());

      //skewed information
      SkewedInfo skewInfor = new SkewedInfo();
      skewInfor.setSkewedColNames(Arrays.asList("name"));
      List<String> skv = Arrays.asList("1");
      skewInfor.setSkewedColValues(Arrays.asList(skv));
      Map<List<String>, String> scvlm = new HashMap<List<String>, String>();
      scvlm.put(skv, "location1");
      skewInfor.setSkewedColValueLocationMaps(scvlm);
      sd.setSkewedInfo(skewInfor);

      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr", serdeConstants.STRING_TYPE_NAME, ""));

      client.createTable(tbl);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      assertEquals(dbPermission, fs.getFileStatus(new Path(tbl.getSd().getLocation()))
          .getPermission());

      Partition part = makePartitionObject(dbName, tblName, vals, tbl, "/part1");
      Partition part2 = makePartitionObject(dbName, tblName, vals2, tbl, "/part2");
      Partition part3 = makePartitionObject(dbName, tblName, vals3, tbl, "/part3");
      Partition part4 = makePartitionObject(dbName, tblName, vals4, tbl, "/part4");

      // check if the partition exists (it shouldn't)
      boolean exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tblName, vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("getPartition() should have thrown NoSuchObjectException", exceptionThrown);
      Partition retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);
      assertEquals(dbPermission, fs.getFileStatus(new Path(retp.getSd().getLocation()))
          .getPermission());
      Partition retp2 = client.add_partition(part2);
      assertNotNull("Unable to create partition " + part2, retp2);
      assertEquals(dbPermission, fs.getFileStatus(new Path(retp2.getSd().getLocation()))
          .getPermission());
      Partition retp3 = client.add_partition(part3);
      assertNotNull("Unable to create partition " + part3, retp3);
      assertEquals(dbPermission, fs.getFileStatus(new Path(retp3.getSd().getLocation()))
          .getPermission());
      Partition retp4 = client.add_partition(part4);
      assertNotNull("Unable to create partition " + part4, retp4);
      assertEquals(dbPermission, fs.getFileStatus(new Path(retp4.getSd().getLocation()))
          .getPermission());

      Partition part_get = client.getPartition(dbName, tblName, part.getValues());
      if(isThriftClient) {
        // since we are using thrift, 'part' will not have the create time and
        // last DDL time set since it does not get updated in the add_partition()
        // call - likewise part2 and part3 - set it correctly so that equals check
        // doesn't fail
        adjust(client, part, dbName, tblName);
        adjust(client, part2, dbName, tblName);
        adjust(client, part3, dbName, tblName);
      }
      assertTrue("Partitions are not same", part.equals(part_get));

      // check null cols schemas for a partition
      List<String> vals6 = makeVals("2016-02-22 00:00:00", "16");
      Partition part6 = makePartitionObject(dbName, tblName, vals6, tbl, "/part5");
      part6.getSd().setCols(null);
      LOG.info("Creating partition will null field schema");
      client.add_partition(part6);
      LOG.info("Listing all partitions for table " + dbName + "." + tblName);
      final List<Partition> partitions = client.listPartitions(dbName, tblName, (short) -1);
      boolean foundPart = false;
      for (Partition p : partitions) {
        if (p.getValues().equals(vals6)) {
          assertNull(p.getSd().getCols());
          LOG.info("Found partition " + p + " having null field schema");
          foundPart = true;
        }
      }
      assertTrue(foundPart);

      String partName = "ds=" + FileUtils.escapePathName("2008-07-01 14:13:12") + "/hr=14";
      String part2Name = "ds=" + FileUtils.escapePathName("2008-07-01 14:13:12") + "/hr=15";
      String part3Name = "ds=" + FileUtils.escapePathName("2008-07-02 14:13:12") + "/hr=15";
      String part4Name = "ds=" + FileUtils.escapePathName("2008-07-03 14:13:12") + "/hr=151";

      part_get = client.getPartition(dbName, tblName, partName);
      assertTrue("Partitions are not the same", part.equals(part_get));

      // Test partition listing with a partial spec - ds is specified but hr is not
      List<String> partialVals = new ArrayList<String>();
      partialVals.add(vals.get(0));
      Set<Partition> parts = new HashSet<Partition>();
      parts.add(part);
      parts.add(part2);

      List<Partition> partial = client.listPartitions(dbName, tblName, partialVals,
          (short) -1);
      assertTrue("Should have returned 2 partitions", partial.size() == 2);
      assertTrue("Not all parts returned", partial.containsAll(parts));

      Set<String> partNames = new HashSet<String>();
      partNames.add(partName);
      partNames.add(part2Name);
      List<String> partialNames = client.listPartitionNames(dbName, tblName, partialVals,
          (short) -1);
      assertTrue("Should have returned 2 partition names", partialNames.size() == 2);
      assertTrue("Not all part names returned", partialNames.containsAll(partNames));

      partNames.add(part3Name);
      partNames.add(part4Name);
      partialVals.clear();
      partialVals.add("");
      partialNames = client.listPartitionNames(dbName, tblName, partialVals, (short) -1);
      assertTrue("Should have returned 5 partition names", partialNames.size() == 5);
      assertTrue("Not all part names returned", partialNames.containsAll(partNames));

      // Test partition listing with a partial spec - hr is specified but ds is not
      parts.clear();
      parts.add(part2);
      parts.add(part3);

      partialVals.clear();
      partialVals.add("");
      partialVals.add(vals2.get(1));

      partial = client.listPartitions(dbName, tblName, partialVals, (short) -1);
      assertEquals("Should have returned 2 partitions", 2, partial.size());
      assertTrue("Not all parts returned", partial.containsAll(parts));

      partNames.clear();
      partNames.add(part2Name);
      partNames.add(part3Name);
      partialNames = client.listPartitionNames(dbName, tblName, partialVals,
          (short) -1);
      assertEquals("Should have returned 2 partition names", 2, partialNames.size());
      assertTrue("Not all part names returned", partialNames.containsAll(partNames));

      // Verify escaped partition names don't return partitions
      exceptionThrown = false;
      try {
        String badPartName = "ds=2008-07-01 14%3A13%3A12/hrs=14";
        client.getPartition(dbName, tblName, badPartName);
      } catch(NoSuchObjectException e) {
        exceptionThrown = true;
      }
      assertTrue("Bad partition spec should have thrown an exception", exceptionThrown);

      Path partPath = new Path(part.getSd().getLocation());


      assertTrue(fs.exists(partPath));
      client.dropPartition(dbName, tblName, part.getValues(), true);
      assertFalse(fs.exists(partPath));

      // Test append_partition_by_name
      client.appendPartition(dbName, tblName, partName);
      Partition part5 = client.getPartition(dbName, tblName, part.getValues());
      assertTrue("Append partition by name failed", part5.getValues().equals(vals));;
      Path part5Path = new Path(part5.getSd().getLocation());
      assertTrue(fs.exists(part5Path));

      // Test drop_partition_by_name
      assertTrue("Drop partition by name failed",
          client.dropPartition(dbName, tblName, partName, true));
      assertFalse(fs.exists(part5Path));

      // add the partition again so that drop table with a partition can be
      // tested
      retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);
      assertEquals(dbPermission, fs.getFileStatus(new Path(retp.getSd().getLocation()))
          .getPermission());

      // test add_partitions

      List<String> mvals1 = makeVals("2008-07-04 14:13:12", "14641");
      List<String> mvals2 = makeVals("2008-07-04 14:13:12", "14642");
      List<String> mvals3 = makeVals("2008-07-04 14:13:12", "14643");
      List<String> mvals4 = makeVals("2008-07-04 14:13:12", "14643"); // equal to 3
      List<String> mvals5 = makeVals("2008-07-04 14:13:12", "14645");

      Exception savedException;

      // add_partitions(empty list) : ok, normal operation
      client.add_partitions(new ArrayList<Partition>());

      // add_partitions(1,2,3) : ok, normal operation
      Partition mpart1 = makePartitionObject(dbName, tblName, mvals1, tbl, "/mpart1");
      Partition mpart2 = makePartitionObject(dbName, tblName, mvals2, tbl, "/mpart2");
      Partition mpart3 = makePartitionObject(dbName, tblName, mvals3, tbl, "/mpart3");
      client.add_partitions(Arrays.asList(mpart1,mpart2,mpart3));

      if(isThriftClient) {
        // do DDL time munging if thrift mode
        adjust(client, mpart1, dbName, tblName);
        adjust(client, mpart2, dbName, tblName);
        adjust(client, mpart3, dbName, tblName);
      }
      verifyPartitionsPublished(client, dbName, tblName,
          Arrays.asList(mvals1.get(0)),
          Arrays.asList(mpart1,mpart2,mpart3));

      Partition mpart4 = makePartitionObject(dbName, tblName, mvals4, tbl, "/mpart4");
      Partition mpart5 = makePartitionObject(dbName, tblName, mvals5, tbl, "/mpart5");

      // create dir for /mpart5
      Path mp5Path = new Path(mpart5.getSd().getLocation());
      warehouse.mkdirs(mp5Path, true);
      assertTrue(fs.exists(mp5Path));
      assertEquals(dbPermission, fs.getFileStatus(mp5Path).getPermission());

      // add_partitions(5,4) : err = duplicate keyvals on mpart4
      savedException = null;
      try {
        client.add_partitions(Arrays.asList(mpart5,mpart4));
      } catch (Exception e) {
        savedException = e;
      } finally {
        assertNotNull(savedException);
      }

      // check that /mpart4 does not exist, but /mpart5 still does.
      assertTrue(fs.exists(mp5Path));
      assertFalse(fs.exists(new Path(mpart4.getSd().getLocation())));

      // add_partitions(5) : ok
      client.add_partitions(Arrays.asList(mpart5));

      if(isThriftClient) {
        // do DDL time munging if thrift mode
        adjust(client, mpart5, dbName, tblName);
      }

      verifyPartitionsPublished(client, dbName, tblName,
          Arrays.asList(mvals1.get(0)),
          Arrays.asList(mpart1,mpart2,mpart3,mpart5));

      //// end add_partitions tests

      client.dropTable(dbName, tblName);

      client.dropType(typeName);

      // recreate table as external, drop partition and it should
      // still exist
      tbl.setParameters(new HashMap<String, String>());
      tbl.getParameters().put("EXTERNAL", "TRUE");
      client.createTable(tbl);
      retp = client.add_partition(part);
      assertTrue(fs.exists(partPath));
      client.dropPartition(dbName, tblName, part.getValues(), true);
      assertTrue(fs.exists(partPath));

      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }

      client.dropDatabase(dbName);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed.");
      throw e;
    }
  }

  private static void verifyPartitionsPublished(HiveMetaStoreClient client,
      String dbName, String tblName, List<String> partialSpec,
      List<Partition> expectedPartitions)
          throws NoSuchObjectException, MetaException, TException {
    // Test partition listing with a partial spec

    List<Partition> mpartial = client.listPartitions(dbName, tblName, partialSpec,
        (short) -1);
    assertEquals("Should have returned "+expectedPartitions.size()+
        " partitions, returned " + mpartial.size(),
        expectedPartitions.size(), mpartial.size());
    assertTrue("Not all parts returned", mpartial.containsAll(expectedPartitions));
  }

  private static List<String> makeVals(String ds, String id) {
    List <String> vals4 = new ArrayList<String>(2);
    vals4 = new ArrayList<String>(2);
    vals4.add(ds);
    vals4.add(id);
    return vals4;
  }

  private static Partition makePartitionObject(String dbName, String tblName,
      List<String> ptnVals, Table tbl, String ptnLocationSuffix) throws MetaException {
    Partition part4 = new Partition();
    part4.setDbName(dbName);
    part4.setTableName(tblName);
    part4.setValues(ptnVals);
    part4.setParameters(new HashMap<String, String>());
    part4.setSd(tbl.getSd().deepCopy());
    part4.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo().deepCopy());
    part4.getSd().setLocation(tbl.getSd().getLocation() + ptnLocationSuffix);
    MetaStoreUtils.updatePartitionStatsFast(part4, warehouse, null);
    return part4;
  }

  public void testListPartitions() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<List<String>>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<Partition> partitions = client.listPartitions(dbName, tblName, (short)-1);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions", values.size(), partitions.size());

    partitions = client.listPartitions(dbName, tblName, (short)(values.size()/2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() / 2 +
      " partitions",values.size() / 2, partitions.size());


    partitions = client.listPartitions(dbName, tblName, (short) (values.size() * 2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions",values.size(), partitions.size());

    cleanUp(dbName, tblName, typeName);

  }

  public void testListPartitionsWihtLimitEnabled() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    // Create too many partitions, just enough to validate over limit requests
    List<List<String>> values = new ArrayList<List<String>>();
    for (int i=0; i<DEFAULT_LIMIT_PARTITION_REQUEST + 1; i++) {
      values.add(makeVals("2008-07-01 14:13:12", Integer.toString(i)));
    }

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<Partition> partitions;
    short maxParts;

    // Requesting more partitions than allowed should throw an exception
    try {
      maxParts = -1;
      partitions = client.listPartitions(dbName, tblName, maxParts);
      fail("should have thrown MetaException about partition limit");
    } catch (MetaException e) {
      assertTrue(true);
    }

    // Requesting more partitions than allowed should throw an exception
    try {
      maxParts = DEFAULT_LIMIT_PARTITION_REQUEST + 1;
      partitions = client.listPartitions(dbName, tblName, maxParts);
      fail("should have thrown MetaException about partition limit");
    } catch (MetaException e) {
      assertTrue(true);
    }

    // Requesting less partitions than allowed should work
    maxParts = DEFAULT_LIMIT_PARTITION_REQUEST / 2;
    partitions = client.listPartitions(dbName, tblName, maxParts);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned 50 partitions", maxParts, partitions.size());
  }

  public void testListPartitionNames() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<List<String>>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));



    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<String> partitions = client.listPartitionNames(dbName, tblName, (short)-1);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions", values.size(), partitions.size());

    partitions = client.listPartitionNames(dbName, tblName, (short)(values.size()/2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() / 2 +
      " partitions",values.size() / 2, partitions.size());


    partitions = client.listPartitionNames(dbName, tblName, (short) (values.size() * 2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions",values.size(), partitions.size());

    cleanUp(dbName, tblName, typeName);

  }


  public void testDropTable() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<List<String>>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    client.dropTable(dbName, tblName);
    client.dropType(typeName);

    boolean exceptionThrown = false;
    try {
      client.getTable(dbName, tblName);
    } catch(Exception e) {
      assertEquals("table should not have existed",
          NoSuchObjectException.class, e.getClass());
      exceptionThrown = true;
    }
    assertTrue("Table " + tblName + " should have been dropped ", exceptionThrown);

  }

  public void testAlterViewParititon() throws Throwable {
    String dbName = "compdb";
    String tblName = "comptbl";
    String viewName = "compView";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    db.setDescription("Alter Partition Test database");
    client.createDatabase(db);

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    tbl.setSd(sd);
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    sd.setSortCols(new ArrayList<Order>());

    client.createTable(tbl);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tblName);
    }

    ArrayList<FieldSchema> viewCols = new ArrayList<FieldSchema>(1);
    viewCols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    ArrayList<FieldSchema> viewPartitionCols = new ArrayList<FieldSchema>(1);
    viewPartitionCols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));

    Table view = new Table();
    view.setDbName(dbName);
    view.setTableName(viewName);
    view.setTableType(TableType.VIRTUAL_VIEW.name());
    view.setPartitionKeys(viewPartitionCols);
    view.setViewOriginalText("SELECT income, name FROM " + tblName);
    view.setViewExpandedText("SELECT `" + tblName + "`.`income`, `" + tblName +
        "`.`name` FROM `" + dbName + "`.`" + tblName + "`");
    view.setRewriteEnabled(false);
    StorageDescriptor viewSd = new StorageDescriptor();
    view.setSd(viewSd);
    viewSd.setCols(viewCols);
    viewSd.setCompressed(false);
    viewSd.setParameters(new HashMap<String, String>());
    viewSd.setSerdeInfo(new SerDeInfo());
    viewSd.getSerdeInfo().setParameters(new HashMap<String, String>());

    client.createTable(view);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      view = client.getTable(dbName, viewName);
    }

    List<String> vals = new ArrayList<String>(1);
    vals.add("abc");

    Partition part = new Partition();
    part.setDbName(dbName);
    part.setTableName(viewName);
    part.setValues(vals);
    part.setParameters(new HashMap<String, String>());

    client.add_partition(part);

    Partition part2 = client.getPartition(dbName, viewName, part.getValues());

    part2.getParameters().put("a", "b");

    client.alter_partition(dbName, viewName, part2, null);

    Partition part3 = client.getPartition(dbName, viewName, part.getValues());
    assertEquals("couldn't view alter partition", part3.getParameters().get(
        "a"), "b");

    client.dropTable(dbName, viewName);

    client.dropTable(dbName, tblName);

    client.dropDatabase(dbName);
  }

  public void testAlterPartition() throws Throwable {

    try {
      String dbName = "compdb";
      String tblName = "comptbl";
      List<String> vals = new ArrayList<String>(2);
      vals.add("2008-07-01");
      vals.add("14");

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      Database db = new Database();
      db.setName(dbName);
      db.setDescription("Alter Partition Test database");
      client.createDatabase(db);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(cols);
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters()
          .put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());
      sd.setSortCols(new ArrayList<Order>());

      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr", serdeConstants.INT_TYPE_NAME, ""));

      client.createTable(tbl);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Partition part = new Partition();
      part.setDbName(dbName);
      part.setTableName(tblName);
      part.setValues(vals);
      part.setParameters(new HashMap<String, String>());
      part.setSd(tbl.getSd());
      part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
      part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");

      client.add_partition(part);

      Partition part2 = client.getPartition(dbName, tblName, part.getValues());

      part2.getParameters().put("retention", "10");
      part2.getSd().setNumBuckets(12);
      part2.getSd().getSerdeInfo().getParameters().put("abc", "1");
      client.alter_partition(dbName, tblName, part2, null);

      Partition part3 = client.getPartition(dbName, tblName, part.getValues());
      assertEquals("couldn't alter partition", part3.getParameters().get(
          "retention"), "10");
      assertEquals("couldn't alter partition", part3.getSd().getSerdeInfo()
          .getParameters().get("abc"), "1");
      assertEquals("couldn't alter partition", part3.getSd().getNumBuckets(),
          12);

      client.dropTable(dbName, tblName);

      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed.");
      throw e;
    }
  }

  public void testRenamePartition() throws Throwable {

    try {
      String dbName = "compdb1";
      String tblName = "comptbl1";
      List<String> vals = new ArrayList<String>(2);
      vals.add("2011-07-11");
      vals.add("8");
      String part_path = "/ds=2011-07-11/hr=8";
      List<String> tmp_vals = new ArrayList<String>(2);
      tmp_vals.add("tmp_2011-07-11");
      tmp_vals.add("-8");
      String part2_path = "/ds=tmp_2011-07-11/hr=-8";

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      Database db = new Database();
      db.setName(dbName);
      db.setDescription("Rename Partition Test database");
      client.createDatabase(db);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(cols);
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters()
          .put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());
      sd.setSortCols(new ArrayList<Order>());

      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds", serdeConstants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr", serdeConstants.INT_TYPE_NAME, ""));

      client.createTable(tbl);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Partition part = new Partition();
      part.setDbName(dbName);
      part.setTableName(tblName);
      part.setValues(vals);
      part.setParameters(new HashMap<String, String>());
      part.setSd(tbl.getSd().deepCopy());
      part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
      part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");
      part.getParameters().put("retention", "10");
      part.getSd().setNumBuckets(12);
      part.getSd().getSerdeInfo().getParameters().put("abc", "1");

      client.add_partition(part);

      part.setValues(tmp_vals);
      client.renamePartition(dbName, tblName, vals, part);

      boolean exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tblName, vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);

      Partition part3 = client.getPartition(dbName, tblName, tmp_vals);
      assertEquals("couldn't rename partition", part3.getParameters().get(
          "retention"), "10");
      assertEquals("couldn't rename partition", part3.getSd().getSerdeInfo()
          .getParameters().get("abc"), "1");
      assertEquals("couldn't rename partition", part3.getSd().getNumBuckets(),
          12);
      assertEquals("new partition sd matches", part3.getSd().getLocation(),
          tbl.getSd().getLocation() + part2_path);

      part.setValues(vals);
      client.renamePartition(dbName, tblName, tmp_vals, part);

      exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tblName, tmp_vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);

      part3 = client.getPartition(dbName, tblName, vals);
      assertEquals("couldn't rename partition", part3.getParameters().get(
          "retention"), "10");
      assertEquals("couldn't rename partition", part3.getSd().getSerdeInfo()
          .getParameters().get("abc"), "1");
      assertEquals("couldn't rename partition", part3.getSd().getNumBuckets(),
          12);
      assertEquals("new partition sd matches", part3.getSd().getLocation(),
          tbl.getSd().getLocation() + part_path);

      client.dropTable(dbName, tblName);

      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testRenamePartition() failed.");
      throw e;
    }
  }

  public void testDatabase() throws Throwable {
    try {
      // clear up any existing databases
      silentDropDatabase(TEST_DB1_NAME);
      silentDropDatabase(TEST_DB2_NAME);

      Database db = new Database();
      db.setName(TEST_DB1_NAME);
      db.setOwnerName(SessionState.getUserFromAuthenticator());
      db.setOwnerType(PrincipalType.USER);
      client.createDatabase(db);

      db = client.getDatabase(TEST_DB1_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB1_NAME, db.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDatabasePath(db).toString(), db.getLocationUri());
      assertEquals(db.getOwnerName(), SessionState.getUserFromAuthenticator());
      assertEquals(db.getOwnerType(), PrincipalType.USER);
      Database db2 = new Database();
      db2.setName(TEST_DB2_NAME);
      client.createDatabase(db2);

      db2 = client.getDatabase(TEST_DB2_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB2_NAME, db2.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDatabasePath(db2).toString(), db2.getLocationUri());

      List<String> dbs = client.getDatabases(".*");

      assertTrue("first database is not " + TEST_DB1_NAME, dbs.contains(TEST_DB1_NAME));
      assertTrue("second database is not " + TEST_DB2_NAME, dbs.contains(TEST_DB2_NAME));

      client.dropDatabase(TEST_DB1_NAME);
      client.dropDatabase(TEST_DB2_NAME);
      silentDropDatabase(TEST_DB1_NAME);
      silentDropDatabase(TEST_DB2_NAME);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDatabase() failed.");
      throw e;
    }
  }

  public void testDatabaseLocationWithPermissionProblems() throws Exception {

    // Note: The following test will fail if you are running this test as root. Setting
    // permission to '0' on the database folder will not preclude root from being able
    // to create the necessary files.

    if (System.getProperty("user.name").equals("root")) {
      System.err.println("Skipping test because you are running as root!");
      return;
    }

    silentDropDatabase(TEST_DB1_NAME);

    Database db = new Database();
    db.setName(TEST_DB1_NAME);
    String dbLocation =
      HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/test/_testDB_create_";
    FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), hiveConf);
    fs.mkdirs(
              new Path(HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/test"),
              new FsPermission((short) 0));
    db.setLocationUri(dbLocation);


    boolean createFailed = false;
    try {
      client.createDatabase(db);
    } catch (MetaException cantCreateDB) {
      createFailed = true;
    } finally {
      // Cleanup
      if (!createFailed) {
        try {
          client.dropDatabase(TEST_DB1_NAME);
        } catch(Exception e) {
          System.err.println("Failed to remove database in cleanup: " + e.getMessage());
        }
      }

      fs.setPermission(new Path(HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/test"),
                       new FsPermission((short) 755));
      fs.delete(new Path(HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/test"), true);
    }

    assertTrue("Database creation succeeded even with permission problem", createFailed);
  }

  public void testDatabaseLocation() throws Throwable {
    try {
      // clear up any existing databases
      silentDropDatabase(TEST_DB1_NAME);

      Database db = new Database();
      db.setName(TEST_DB1_NAME);
      String dbLocation =
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/_testDB_create_";
      db.setLocationUri(dbLocation);
      client.createDatabase(db);

      db = client.getDatabase(TEST_DB1_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB1_NAME, db.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDnsPath(new Path(dbLocation)).toString(), db.getLocationUri());

      client.dropDatabase(TEST_DB1_NAME);
      silentDropDatabase(TEST_DB1_NAME);

      boolean objectNotExist = false;
      try {
        client.getDatabase(TEST_DB1_NAME);
      } catch (NoSuchObjectException e) {
        objectNotExist = true;
      }
      assertTrue("Database " + TEST_DB1_NAME + " exists ", objectNotExist);

      db = new Database();
      db.setName(TEST_DB1_NAME);
      dbLocation =
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/_testDB_file_";
      FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), hiveConf);
      fs.createNewFile(new Path(dbLocation));
      fs.deleteOnExit(new Path(dbLocation));
      db.setLocationUri(dbLocation);

      boolean createFailed = false;
      try {
        client.createDatabase(db);
      } catch (MetaException cantCreateDB) {
        System.err.println(cantCreateDB.getMessage());
        createFailed = true;
      }
      assertTrue("Database creation succeeded even location exists and is a file", createFailed);

      objectNotExist = false;
      try {
        client.getDatabase(TEST_DB1_NAME);
      } catch (NoSuchObjectException e) {
        objectNotExist = true;
      }
      assertTrue("Database " + TEST_DB1_NAME + " exists when location is specified and is a file",
          objectNotExist);

    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDatabaseLocation() failed.");
      throw e;
    }
  }


  public void testSimpleTypeApi() throws Exception {
    try {
      client.dropType(serdeConstants.INT_TYPE_NAME);

      Type typ1 = new Type();
      typ1.setName(serdeConstants.INT_TYPE_NAME);
      boolean ret = client.createType(typ1);
      assertTrue("Unable to create type", ret);

      Type typ1_2 = client.getType(serdeConstants.INT_TYPE_NAME);
      assertNotNull(typ1_2);
      assertEquals(typ1.getName(), typ1_2.getName());

      ret = client.dropType(serdeConstants.INT_TYPE_NAME);
      assertTrue("unable to drop type integer", ret);

      boolean exceptionThrown = false;
      try {
        client.getType(serdeConstants.INT_TYPE_NAME);
      } catch (NoSuchObjectException e) {
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTypeApi() failed.");
      throw e;
    }
  }

  // TODO:pc need to enhance this with complex fields and getType_all function
  public void testComplexTypeApi() throws Exception {
    try {
      client.dropType("Person");

      Type typ1 = new Type();
      typ1.setName("Person");
      typ1.setFields(new ArrayList<FieldSchema>(2));
      typ1.getFields().add(
          new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));
      boolean ret = client.createType(typ1);
      assertTrue("Unable to create type", ret);

      Type typ1_2 = client.getType("Person");
      assertNotNull("type Person not found", typ1_2);
      assertEquals(typ1.getName(), typ1_2.getName());
      assertEquals(typ1.getFields().size(), typ1_2.getFields().size());
      assertEquals(typ1.getFields().get(0), typ1_2.getFields().get(0));
      assertEquals(typ1.getFields().get(1), typ1_2.getFields().get(1));

      client.dropType("Family");

      Type fam = new Type();
      fam.setName("Family");
      fam.setFields(new ArrayList<FieldSchema>(2));
      fam.getFields().add(
          new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      fam.getFields().add(
          new FieldSchema("members",
              MetaStoreUtils.getListType(typ1.getName()), ""));

      ret = client.createType(fam);
      assertTrue("Unable to create type " + fam.getName(), ret);

      Type fam2 = client.getType("Family");
      assertNotNull("type Person not found", fam2);
      assertEquals(fam.getName(), fam2.getName());
      assertEquals(fam.getFields().size(), fam2.getFields().size());
      assertEquals(fam.getFields().get(0), fam2.getFields().get(0));
      assertEquals(fam.getFields().get(1), fam2.getFields().get(1));

      ret = client.dropType("Family");
      assertTrue("unable to drop type Family", ret);

      ret = client.dropType("Person");
      assertTrue("unable to drop type Person", ret);

      boolean exceptionThrown = false;
      try {
        client.getType("Person");
      } catch (NoSuchObjectException e) {
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testComplexTypeApi() failed.");
      throw e;
    }
  }

  public void testSimpleTable() throws Exception {
    try {
      String dbName = "simpdb";
      String tblName = "simptbl";
      String tblName2 = "simptbl2";
      String typeName = "Person";

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);

      Database db = new Database();
      db.setName(dbName);
      client.createDatabase(db);

      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<FieldSchema>(2));
      typ1.getFields().add(
          new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));
      client.createType(typ1);

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(typ1.getFields());
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setInputFormat(HiveOutputFormat.class.getName());

      tbl.setPartitionKeys(new ArrayList<FieldSchema>());

      client.createTable(tbl);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Table tbl2 = client.getTable(dbName, tblName);
      assertNotNull(tbl2);
      assertEquals(tbl2.getDbName(), dbName);
      assertEquals(tbl2.getTableName(), tblName);
      assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
      assertEquals(tbl2.getSd().isCompressed(), false);
      assertEquals(tbl2.getSd().getNumBuckets(), 1);
      assertEquals(tbl2.getSd().getLocation(), tbl.getSd().getLocation());
      assertNotNull(tbl2.getSd().getSerdeInfo());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");

      tbl2.setTableName(tblName2);
      tbl2.setParameters(new HashMap<String, String>());
      tbl2.getParameters().put("EXTERNAL", "TRUE");
      tbl2.getSd().setLocation(tbl.getSd().getLocation() + "-2");

      List<FieldSchema> fieldSchemas = client.getFields(dbName, tblName);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue(fieldSchemas.contains(fs));
      }

      List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size()
          + tbl.getPartitionKeys().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }
      for (FieldSchema fs : tbl.getPartitionKeys()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }

      client.createTable(tbl2);
      if (isThriftClient) {
        tbl2 = client.getTable(tbl2.getDbName(), tbl2.getTableName());
      }

      Table tbl3 = client.getTable(dbName, tblName2);
      assertNotNull(tbl3);
      assertEquals(tbl3.getDbName(), dbName);
      assertEquals(tbl3.getTableName(), tblName2);
      assertEquals(tbl3.getSd().getCols().size(), typ1.getFields().size());
      assertEquals(tbl3.getSd().isCompressed(), false);
      assertEquals(tbl3.getSd().getNumBuckets(), 1);
      assertEquals(tbl3.getSd().getLocation(), tbl2.getSd().getLocation());
      assertEquals(tbl3.getParameters(), tbl2.getParameters());

      fieldSchemas = client.getFields(dbName, tblName2);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl2.getSd().getCols().size());
      for (FieldSchema fs : tbl2.getSd().getCols()) {
        assertTrue(fieldSchemas.contains(fs));
      }

      fieldSchemasFull = client.getSchema(dbName, tblName2);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl2.getSd().getCols().size()
          + tbl2.getPartitionKeys().size());
      for (FieldSchema fs : tbl2.getSd().getCols()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }
      for (FieldSchema fs : tbl2.getPartitionKeys()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }

      assertEquals("Use this for comments etc", tbl2.getSd().getParameters()
          .get("test_param_1"));
      assertEquals("name", tbl2.getSd().getBucketCols().get(0));
      assertTrue("Partition key list is not empty",
          (tbl2.getPartitionKeys() == null)
              || (tbl2.getPartitionKeys().size() == 0));

      //test get_table_objects_by_name functionality
      ArrayList<String> tableNames = new ArrayList<String>();
      tableNames.add(tblName2);
      tableNames.add(tblName);
      tableNames.add(tblName2);
      List<Table> foundTables = client.getTableObjectsByName(dbName, tableNames);

      assertEquals(2, foundTables.size());
      for (Table t: foundTables) {
        if (t.getTableName().equals(tblName2)) {
          assertEquals(t.getSd().getLocation(), tbl2.getSd().getLocation());
        } else {
          assertEquals(t.getTableName(), tblName);
          assertEquals(t.getSd().getLocation(), tbl.getSd().getLocation());
        }
        assertEquals(t.getSd().getCols().size(), typ1.getFields().size());
        assertEquals(t.getSd().isCompressed(), false);
        assertEquals(foundTables.get(0).getSd().getNumBuckets(), 1);
        assertNotNull(t.getSd().getSerdeInfo());
        assertEquals(t.getDbName(), dbName);
      }

      tableNames.add(1, "table_that_doesnt_exist");
      foundTables = client.getTableObjectsByName(dbName, tableNames);
      assertEquals(foundTables.size(), 2);

      InvalidOperationException ioe = null;
      try {
        foundTables = client.getTableObjectsByName(dbName, null);
      } catch (InvalidOperationException e) {
        ioe = e;
      }
      assertNotNull(ioe);
      assertTrue("Table not found", ioe.getMessage().contains("null tables"));

      UnknownDBException udbe = null;
      try {
        foundTables = client.getTableObjectsByName("db_that_doesnt_exist", tableNames);
      } catch (UnknownDBException e) {
        udbe = e;
      }
      assertNotNull(udbe);
      assertTrue("DB not found", udbe.getMessage().contains("not find database db_that_doesnt_exist"));

      udbe = null;
      try {
        foundTables = client.getTableObjectsByName("", tableNames);
      } catch (UnknownDBException e) {
        udbe = e;
      }
      assertNotNull(udbe);
      assertTrue("DB not found", udbe.getMessage().contains("is null or empty"));

      FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), hiveConf);
      client.dropTable(dbName, tblName);
      assertFalse(fs.exists(new Path(tbl.getSd().getLocation())));

      client.dropTable(dbName, tblName2);
      assertTrue(fs.exists(new Path(tbl2.getSd().getLocation())));

      client.dropType(typeName);
      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTable() failed.");
      throw e;
    }
  }

  // Tests that in the absence of stats for partitions, and/or absence of columns
  // to get stats for, the metastore does not break. See HIVE-12083 for motivation.
  public void testStatsFastTrivial() throws Throwable {
    String dbName = "tstatsfast";
    String tblName = "t1";
    String tblOwner = "statstester";
    String typeName = "Person";
    int lastAccessed = 12083;

    cleanUp(dbName,tblName,typeName);

    List<List<String>> values = new ArrayList<List<String>>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<String> emptyColNames = new ArrayList<String>();
    List<String> emptyPartNames = new ArrayList<String>();

    List<String> colNames = new ArrayList<String>();
    colNames.add("name");
    colNames.add("income");
    List<String> partNames = client.listPartitionNames(dbName,tblName,(short)-1);

    assertEquals(0,emptyColNames.size());
    assertEquals(0,emptyPartNames.size());
    assertEquals(2,colNames.size());
    assertEquals(4,partNames.size());

    // Test for both colNames and partNames being empty:
    AggrStats aggrStatsEmpty = client.getAggrColStatsFor(dbName,tblName,emptyColNames,emptyPartNames);
    assertNotNull(aggrStatsEmpty); // short-circuited on client-side, verifying that it's an empty object, not null
    assertEquals(0,aggrStatsEmpty.getPartsFound());
    assertNotNull(aggrStatsEmpty.getColStats());
    assert(aggrStatsEmpty.getColStats().isEmpty());

    // Test for only colNames being empty
    AggrStats aggrStatsOnlyParts = client.getAggrColStatsFor(dbName,tblName,emptyColNames,partNames);
    assertNotNull(aggrStatsOnlyParts); // short-circuited on client-side, verifying that it's an empty object, not null
    assertEquals(0,aggrStatsOnlyParts.getPartsFound());
    assertNotNull(aggrStatsOnlyParts.getColStats());
    assert(aggrStatsOnlyParts.getColStats().isEmpty());

    // Test for only partNames being empty
    AggrStats aggrStatsOnlyCols = client.getAggrColStatsFor(dbName,tblName,colNames,emptyPartNames);
    assertNotNull(aggrStatsOnlyCols); // short-circuited on client-side, verifying that it's an empty object, not null
    assertEquals(0,aggrStatsOnlyCols.getPartsFound());
    assertNotNull(aggrStatsOnlyCols.getColStats());
    assert(aggrStatsOnlyCols.getColStats().isEmpty());

    // Test for valid values for both.
    AggrStats aggrStatsFull = client.getAggrColStatsFor(dbName,tblName,colNames,partNames);
    assertNotNull(aggrStatsFull);
    assertEquals(0,aggrStatsFull.getPartsFound()); // would still be empty, because no stats are actually populated.
    assertNotNull(aggrStatsFull.getColStats());
    assert(aggrStatsFull.getColStats().isEmpty());

  }

  public void testColumnStatistics() throws Throwable {

    String dbName = "columnstatstestdb";
    String tblName = "tbl";
    String typeName = "Person";
    String tblOwner = "testowner";
    int lastAccessed = 6796;

    try {
      cleanUp(dbName, tblName, typeName);
      Database db = new Database();
      db.setName(dbName);
      client.createDatabase(db);
      createTableForTestFilter(dbName,tblName, tblOwner, lastAccessed, true);

      // Create a ColumnStatistics Obj
      String[] colName = new String[]{"income", "name"};
      double lowValue = 50000.21;
      double highValue = 1200000.4525;
      long numNulls = 3;
      long numDVs = 22;
      double avgColLen = 50.30;
      long maxColLen = 102;
      String[] colType = new String[] {"double", "string"};
      boolean isTblLevel = true;
      String partName = null;
      List<ColumnStatisticsObj> statsObjs = new ArrayList<ColumnStatisticsObj>();

      ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
      statsDesc.setDbName(dbName);
      statsDesc.setTableName(tblName);
      statsDesc.setIsTblLevel(isTblLevel);
      statsDesc.setPartName(partName);

      ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
      statsObj.setColName(colName[0]);
      statsObj.setColType(colType[0]);

      ColumnStatisticsData statsData = new ColumnStatisticsData();
      DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
      statsData.setDoubleStats(numericStats);

      statsData.getDoubleStats().setHighValue(highValue);
      statsData.getDoubleStats().setLowValue(lowValue);
      statsData.getDoubleStats().setNumDVs(numDVs);
      statsData.getDoubleStats().setNumNulls(numNulls);

      statsObj.setStatsData(statsData);
      statsObjs.add(statsObj);

      statsObj = new ColumnStatisticsObj();
      statsObj.setColName(colName[1]);
      statsObj.setColType(colType[1]);

      statsData = new ColumnStatisticsData();
      StringColumnStatsData stringStats = new StringColumnStatsData();
      statsData.setStringStats(stringStats);
      statsData.getStringStats().setAvgColLen(avgColLen);
      statsData.getStringStats().setMaxColLen(maxColLen);
      statsData.getStringStats().setNumDVs(numDVs);
      statsData.getStringStats().setNumNulls(numNulls);

      statsObj.setStatsData(statsData);
      statsObjs.add(statsObj);

      ColumnStatistics colStats = new ColumnStatistics();
      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);

      // write stats objs persistently
      client.updateTableColumnStatistics(colStats);

      // retrieve the stats obj that was just written
      ColumnStatisticsObj colStats2 = client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName[0])).get(0);

     // compare stats obj to ensure what we get is what we wrote
      assertNotNull(colStats2);
      assertEquals(colStats2.getColName(), colName[0]);
      assertEquals(colStats2.getStatsData().getDoubleStats().getLowValue(), lowValue);
      assertEquals(colStats2.getStatsData().getDoubleStats().getHighValue(), highValue);
      assertEquals(colStats2.getStatsData().getDoubleStats().getNumNulls(), numNulls);
      assertEquals(colStats2.getStatsData().getDoubleStats().getNumDVs(), numDVs);

      // test delete column stats; if no col name is passed all column stats associated with the
      // table is deleted
      boolean status = client.deleteTableColumnStatistics(dbName, tblName, null);
      assertTrue(status);
      // try to query stats for a column for which stats doesn't exist
      assertTrue(client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName[1])).isEmpty());

      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);

      // update table level column stats
      client.updateTableColumnStatistics(colStats);

      // query column stats for column whose stats were updated in the previous call
      colStats2 = client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName[0])).get(0);

      // partition level column statistics test
      // create a table with multiple partitions
      cleanUp(dbName, tblName, typeName);

      List<List<String>> values = new ArrayList<List<String>>();
      values.add(makeVals("2008-07-01 14:13:12", "14"));
      values.add(makeVals("2008-07-01 14:13:12", "15"));
      values.add(makeVals("2008-07-02 14:13:12", "15"));
      values.add(makeVals("2008-07-03 14:13:12", "151"));

      createMultiPartitionTableSchema(dbName, tblName, typeName, values);

      List<String> partitions = client.listPartitionNames(dbName, tblName, (short)-1);

      partName = partitions.get(0);
      isTblLevel = false;

      // create a new columnstatistics desc to represent partition level column stats
      statsDesc = new ColumnStatisticsDesc();
      statsDesc.setDbName(dbName);
      statsDesc.setTableName(tblName);
      statsDesc.setPartName(partName);
      statsDesc.setIsTblLevel(isTblLevel);

      colStats = new ColumnStatistics();
      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);

     client.updatePartitionColumnStatistics(colStats);

     colStats2 = client.getPartitionColumnStatistics(dbName, tblName,
         Lists.newArrayList(partName), Lists.newArrayList(colName[1])).get(partName).get(0);

     // compare stats obj to ensure what we get is what we wrote
     assertNotNull(colStats2);
     assertEquals(colStats.getStatsDesc().getPartName(), partName);
     assertEquals(colStats2.getColName(), colName[1]);
     assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
     assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen);
     assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
     assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

     // test stats deletion at partition level
     client.deletePartitionColumnStatistics(dbName, tblName, partName, colName[1]);

     colStats2 = client.getPartitionColumnStatistics(dbName, tblName,
         Lists.newArrayList(partName), Lists.newArrayList(colName[0])).get(partName).get(0);

     // test get stats on a column for which stats doesn't exist
     assertTrue(client.getPartitionColumnStatistics(dbName, tblName,
           Lists.newArrayList(partName), Lists.newArrayList(colName[1])).isEmpty());
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testColumnStatistics() failed.");
      throw e;
    } finally {
      cleanUp(dbName, tblName, typeName);
    }
  }

  public void testAlterTable() throws Exception {
    String dbName = "alterdb";
    String invTblName = "alter-tbl";
    String tblName = "altertbl";

    try {
      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);

      Database db = new Database();
      db.setName(dbName);
      client.createDatabase(db);

      ArrayList<FieldSchema> invCols = new ArrayList<FieldSchema>(2);
      invCols.add(new FieldSchema("n-ame", serdeConstants.STRING_TYPE_NAME, ""));
      invCols.add(new FieldSchema("in.come", serdeConstants.INT_TYPE_NAME, ""));

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(invTblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(invCols);
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());
      
      boolean failed = false;
      try {
        client.createTable(tbl);
      } catch (InvalidObjectException ex) {
        failed = true;
      }
      if (!failed) {
        assertTrue("Able to create table with invalid name: " + invTblName,
            false);
      }

      // create an invalid table which has wrong column type
      ArrayList<FieldSchema> invColsInvType = new ArrayList<FieldSchema>(2);
      invColsInvType.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      invColsInvType.add(new FieldSchema("income", "xyz", ""));
      tbl.setTableName(tblName);
      tbl.getSd().setCols(invColsInvType);
      boolean failChecker = false;
      try {
        client.createTable(tbl);
      } catch (InvalidObjectException ex) {
        failChecker = true;
      }
      if (!failChecker) {
        assertTrue("Able to create table with invalid column type: " + invTblName,
            false);
      }

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

      // create a valid table
      tbl.setTableName(tblName);
      tbl.getSd().setCols(cols);
      client.createTable(tbl);

      if (isThriftClient) {
        tbl = client.getTable(tbl.getDbName(), tbl.getTableName());
      }

      // now try to invalid alter table
      Table tbl2 = client.getTable(dbName, tblName);
      failed = false;
      try {
        tbl2.setTableName(invTblName);
        tbl2.getSd().setCols(invCols);
        client.alter_table(dbName, tblName, tbl2);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      if (!failed) {
        assertTrue("Able to rename table with invalid name: " + invTblName,
            false);
      }

      //try an invalid alter table with partition key name
      Table tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
      List<FieldSchema> partitionKeys = tbl_pk.getPartitionKeys();
      for (FieldSchema fs : partitionKeys) {
        fs.setName("invalid_to_change_name");
        fs.setComment("can_change_comment");
      }
      tbl_pk.setPartitionKeys(partitionKeys);
      try {
        client.alter_table(dbName, tblName, tbl_pk);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      assertTrue("Should not have succeeded in altering partition key name", failed);

      //try a valid alter table partition key comment
      failed = false;
      tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
      partitionKeys = tbl_pk.getPartitionKeys();
      for (FieldSchema fs : partitionKeys) {
        fs.setComment("can_change_comment");
      }
      tbl_pk.setPartitionKeys(partitionKeys);
      try {
        client.alter_table(dbName, tblName, tbl_pk);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      assertFalse("Should not have failed alter table partition comment", failed);
      Table newT = client.getTable(tbl.getDbName(), tbl.getTableName());
      assertEquals(partitionKeys, newT.getPartitionKeys());

      // try a valid alter table
      tbl2.setTableName(tblName + "_renamed");
      tbl2.getSd().setCols(cols);
      tbl2.getSd().setNumBuckets(32);
      client.alter_table(dbName, tblName, tbl2);
      Table tbl3 = client.getTable(dbName, tbl2.getTableName());
      assertEquals("Alter table didn't succeed. Num buckets is different ",
          tbl2.getSd().getNumBuckets(), tbl3.getSd().getNumBuckets());
      // check that data has moved
      FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), hiveConf);
      assertFalse("old table location still exists", fs.exists(new Path(tbl
          .getSd().getLocation())));
      assertTrue("data did not move to new location", fs.exists(new Path(tbl3
          .getSd().getLocation())));

      if (!isThriftClient) {
        assertEquals("alter table didn't move data correct location", tbl3
            .getSd().getLocation(), tbl2.getSd().getLocation());
      }

      // alter table with invalid column type
      tbl_pk.getSd().setCols(invColsInvType);
      failed = false;
      try {
        client.alter_table(dbName, tbl2.getTableName(), tbl_pk);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      assertTrue("Should not have succeeded in altering column", failed);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTable() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  public void testComplexTable() throws Exception {

    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    try {
      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      Database db = new Database();
      db.setName(dbName);
      client.createDatabase(db);

      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<FieldSchema>(2));
      typ1.getFields().add(
          new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));
      client.createType(typ1);

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(typ1.getFields());
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.getParameters().put("test_param_1", "Use this for comments etc");
      sd.setBucketCols(new ArrayList<String>(2));
      sd.getBucketCols().add("name");
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "9");
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());
      
      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds",
              org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr",
              org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME, ""));

      client.createTable(tbl);

      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals(tbl2.getDbName(), dbName);
      assertEquals(tbl2.getTableName(), tblName);
      assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
      assertFalse(tbl2.getSd().isCompressed());
      assertFalse(tbl2.getSd().isStoredAsSubDirectories());
      assertEquals(tbl2.getSd().getNumBuckets(), 1);

      assertEquals("Use this for comments etc", tbl2.getSd().getParameters()
          .get("test_param_1"));
      assertEquals("name", tbl2.getSd().getBucketCols().get(0));

      assertNotNull(tbl2.getPartitionKeys());
      assertEquals(2, tbl2.getPartitionKeys().size());
      assertEquals(serdeConstants.DATE_TYPE_NAME, tbl2.getPartitionKeys().get(0)
          .getType());
      assertEquals(serdeConstants.INT_TYPE_NAME, tbl2.getPartitionKeys().get(1)
          .getType());
      assertEquals("ds", tbl2.getPartitionKeys().get(0).getName());
      assertEquals("hr", tbl2.getPartitionKeys().get(1).getName());

      List<FieldSchema> fieldSchemas = client.getFields(dbName, tblName);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue(fieldSchemas.contains(fs));
      }

      List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size()
          + tbl.getPartitionKeys().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }
      for (FieldSchema fs : tbl.getPartitionKeys()) {
        assertTrue(fieldSchemasFull.contains(fs));
      }
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testComplexTable() failed.");
      throw e;
    } finally {
      client.dropTable(dbName, tblName);
      boolean ret = client.dropType(typeName);
      assertTrue("Unable to drop type " + typeName, ret);
      client.dropDatabase(dbName);
    }
  }

  public void testTableDatabase() throws Exception {
    String dbName = "testDb";
    String tblName_1 = "testTbl_1";
    String tblName_2 = "testTbl_2";

    try {
      silentDropDatabase(dbName);

      Database db = new Database();
      db.setName(dbName);
      String dbLocation =
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "_testDB_table_create_";
      db.setLocationUri(dbLocation);
      client.createDatabase(db);
      db = client.getDatabase(dbName);

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName_1);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(
          org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT, "9");
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());

      tbl.setSd(sd);
      tbl.getSd().setCols(cols);
      client.createTable(tbl);
      tbl = client.getTable(dbName, tblName_1);

      Path path = new Path(tbl.getSd().getLocation());
      System.err.println("Table's location " + path + ", Database's location " + db.getLocationUri());
      assertEquals("Table location is not a subset of the database location",
          path.getParent().toString(), db.getLocationUri());

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTableDatabase() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }


  public void testGetConfigValue() {

    String val = "value";

    if (!isThriftClient) {
      try {
        assertEquals(client.getConfigValue("hive.key1", val), "value1");
        assertEquals(client.getConfigValue("hive.key2", val), "http://www.example.com");
        assertEquals(client.getConfigValue("hive.key3", val), "");
        assertEquals(client.getConfigValue("hive.key4", val), "0");
        assertEquals(client.getConfigValue("hive.key5", val), val);
        assertEquals(client.getConfigValue(null, val), val);
      } catch (ConfigValSecurityException e) {
        e.printStackTrace();
        assert (false);
      } catch (TException e) {
        e.printStackTrace();
        assert (false);
      }
    }

    boolean threwException = false;
    try {
      // Attempting to get the password should throw an exception
      client.getConfigValue("javax.jdo.option.ConnectionPassword", "password");
    } catch (ConfigValSecurityException e) {
      threwException = true;
    } catch (TException e) {
      e.printStackTrace();
      assert (false);
    }
    assert (threwException);
  }

  private static void adjust(HiveMetaStoreClient client, Partition part,
      String dbName, String tblName)
  throws NoSuchObjectException, MetaException, TException {
    Partition part_get = client.getPartition(dbName, tblName, part.getValues());
    part.setCreateTime(part_get.getCreateTime());
    part.putToParameters(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DDL_TIME, Long.toString(part_get.getCreateTime()));
  }

  private static void silentDropDatabase(String dbName) throws MetaException, TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException e) {
    } catch (InvalidOperationException e) {
    }
  }

  /**
   * Tests for list partition by filter functionality.
   * @throws Exception
   */

  public void testPartitionFilter() throws Exception {
    String dbName = "filterdb";
    String tblName = "filtertbl";

    silentDropDatabase(dbName);

    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(3);
    partCols.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema("p2", serdeConstants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema("p3", serdeConstants.INT_TYPE_NAME, ""));

    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    StorageDescriptor sd = new StorageDescriptor();
    tbl.setSd(sd);
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(new HashMap<String, String>());
    sd.setBucketCols(new ArrayList<String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    sd.setSortCols(new ArrayList<Order>());

    tbl.setPartitionKeys(partCols);
    client.createTable(tbl);

    tbl = client.getTable(dbName, tblName);

    add_partition(client, tbl, Lists.newArrayList("p11", "p21", "31"), "part1");
    add_partition(client, tbl, Lists.newArrayList("p11", "p22", "32"), "part2");
    add_partition(client, tbl, Lists.newArrayList("p12", "p21", "31"), "part3");
    add_partition(client, tbl, Lists.newArrayList("p12", "p23", "32"), "part4");
    add_partition(client, tbl, Lists.newArrayList("p13", "p24", "31"), "part5");
    add_partition(client, tbl, Lists.newArrayList("p13", "p25", "-33"), "part6");

    // Test equals operator for strings and integers.
    checkFilter(client, dbName, tblName, "p1 = \"p11\"", 2);
    checkFilter(client, dbName, tblName, "p1 = \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p2 = \"p21\"", 2);
    checkFilter(client, dbName, tblName, "p2 = \"p23\"", 1);
    checkFilter(client, dbName, tblName, "p3 = 31", 3);
    checkFilter(client, dbName, tblName, "p3 = 33", 0);
    checkFilter(client, dbName, tblName, "p3 = -33", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" and p2=\"p22\"", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p2=\"p23\"", 3);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" and p3 = 31", 1);
    checkFilter(client, dbName, tblName, "p3 = -33 or p1 = \"p12\"", 3);

    // Test not-equals operator for strings and integers.
    checkFilter(client, dbName, tblName, "p1 != \"p11\"", 4);
    checkFilter(client, dbName, tblName, "p2 != \"p23\"", 5);
    checkFilter(client, dbName, tblName, "p2 != \"p33\"", 6);
    checkFilter(client, dbName, tblName, "p3 != 32", 4);
    checkFilter(client, dbName, tblName, "p3 != 8589934592", 6);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p1 != \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p2 != \"p22\"", 4);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" or p2 != \"p22\"", 5);
    checkFilter(client, dbName, tblName, "p1 != \"p12\" and p2 != \"p25\"", 3);
    checkFilter(client, dbName, tblName, "p1 != \"p12\" or p2 != \"p25\"", 6);
    checkFilter(client, dbName, tblName, "p3 != -33 or p1 != \"p13\"", 5);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p3 = 31", 2);
    checkFilter(client, dbName, tblName, "p3 != 31 and p1 = \"p12\"", 1);

    // Test reverse order.
    checkFilter(client, dbName, tblName, "31 != p3 and p1 = \"p12\"", 1);
    checkFilter(client, dbName, tblName, "\"p23\" = p2", 1);

    // Test and/or more...
    checkFilter(client, dbName, tblName,
        "p1 = \"p11\" or (p1=\"p12\" and p2=\"p21\")", 3);
    checkFilter(client, dbName, tblName,
       "p1 = \"p11\" or (p1=\"p12\" and p2=\"p21\") Or " +
       "(p1=\"p13\" aNd p2=\"p24\")", 4);
    //test for and or precedence
    checkFilter(client, dbName, tblName,
       "p1=\"p12\" and (p2=\"p27\" Or p2=\"p21\")", 1);
    checkFilter(client, dbName, tblName,
       "p1=\"p12\" and p2=\"p27\" Or p2=\"p21\"", 2);

    // Test gt/lt/lte/gte/like for strings.
    checkFilter(client, dbName, tblName, "p1 > \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 >= \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 < \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 <= \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 like \"p1.*\"", 6);
    checkFilter(client, dbName, tblName, "p2 like \"p.*3\"", 1);

    // Test gt/lt/lte/gte for numbers.
    checkFilter(client, dbName, tblName, "p3 < 0", 1);
    checkFilter(client, dbName, tblName, "p3 >= -33", 6);
    checkFilter(client, dbName, tblName, "p3 > -33", 5);
    checkFilter(client, dbName, tblName, "p3 > 31 and p3 < 32", 0);
    checkFilter(client, dbName, tblName, "p3 > 31 or p3 < 31", 3);
    checkFilter(client, dbName, tblName, "p3 > 30 or p3 < 30", 6);
    checkFilter(client, dbName, tblName, "p3 >= 31 or p3 < -32", 6);
    checkFilter(client, dbName, tblName, "p3 >= 32", 2);
    checkFilter(client, dbName, tblName, "p3 > 32", 0);

    // Test between
    checkFilter(client, dbName, tblName, "p1 between \"p11\" and \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 not between \"p11\" and \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p3 not between 0 and 2", 6);
    checkFilter(client, dbName, tblName, "p3 between 31 and 32", 5);
    checkFilter(client, dbName, tblName, "p3 between 32 and 31", 0);
    checkFilter(client, dbName, tblName, "p3 between -32 and 34 and p3 not between 31 and 32", 0);
    checkFilter(client, dbName, tblName, "p3 between 1 and 3 or p3 not between 1 and 3", 6);
    checkFilter(client, dbName, tblName,
        "p3 between 31 and 32 and p1 between \"p12\" and \"p14\"", 3);

    //Test for setting the maximum partition count
    List<Partition> partitions = client.listPartitionsByFilter(dbName,
        tblName, "p1 >= \"p12\"", (short) 2);
    assertEquals("User specified row limit for partitions",
        2, partitions.size());

    //Negative tests
    Exception me = null;
    try {
      client.listPartitionsByFilter(dbName,
          tblName, "p3 >= \"p12\"", (short) -1);
    } catch(MetaException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("Filter on int partition key", me.getMessage().contains(
          "Filtering is supported only on partition keys of type string"));

    me = null;
    try {
      client.listPartitionsByFilter(dbName,
          tblName, "c1 >= \"p12\"", (short) -1);
    } catch(MetaException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("Filter on invalid key", me.getMessage().contains(
          "<c1> is not a partitioning key for the table"));

    me = null;
    try {
      client.listPartitionsByFilter(dbName,
          tblName, "c1 >= ", (short) -1);
    } catch(MetaException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("Invalid filter string", me.getMessage().contains(
          "Error parsing partition filter"));

    me = null;
    try {
      client.listPartitionsByFilter("invDBName",
          "invTableName", "p1 = \"p11\"", (short) -1);
    } catch(NoSuchObjectException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("NoSuchObject exception", me.getMessage().contains(
          "invDBName.invTableName table not found"));

    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
  }


  /**
   * Test filtering on table with single partition
   * @throws Exception
   */
  public void testFilterSinglePartition() throws Exception {
      String dbName = "filterdb";
      String tblName = "filtertbl";

      List<String> vals = new ArrayList<String>(1);
      vals.add("p11");
      List <String> vals2 = new ArrayList<String>(1);
      vals2.add("p12");
      List <String> vals3 = new ArrayList<String>(1);
      vals3.add("p13");

      silentDropDatabase(dbName);

      Database db = new Database();
      db.setName(dbName);
      client.createDatabase(db);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

      ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
      partCols.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(cols);
      sd.setCompressed(false);
      sd.setNumBuckets(1);
      sd.setParameters(new HashMap<String, String>());
      sd.setBucketCols(new ArrayList<String>());
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters()
          .put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());
      sd.setSortCols(new ArrayList<Order>());

      tbl.setPartitionKeys(partCols);
      client.createTable(tbl);

      tbl = client.getTable(dbName, tblName);

      add_partition(client, tbl, vals, "part1");
      add_partition(client, tbl, vals2, "part2");
      add_partition(client, tbl, vals3, "part3");

      checkFilter(client, dbName, tblName, "p1 = \"p12\"", 1);
      checkFilter(client, dbName, tblName, "p1 < \"p12\"", 1);
      checkFilter(client, dbName, tblName, "p1 > \"p12\"", 1);
      checkFilter(client, dbName, tblName, "p1 >= \"p12\"", 2);
      checkFilter(client, dbName, tblName, "p1 <= \"p12\"", 2);
      checkFilter(client, dbName, tblName, "p1 <> \"p12\"", 2);
      checkFilter(client, dbName, tblName, "p1 like \"p1.*\"", 3);
      checkFilter(client, dbName, tblName, "p1 like \"p.*2\"", 1);

      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);
  }

  /**
   * Test filtering based on the value of the last partition
   * @throws Exception
   */
  public void testFilterLastPartition() throws Exception {
      String dbName = "filterdb";
      String tblName = "filtertbl";

      List<String> vals = new ArrayList<String>(2);
      vals.add("p11");
      vals.add("p21");
      List <String> vals2 = new ArrayList<String>(2);
      vals2.add("p11");
      vals2.add("p22");
      List <String> vals3 = new ArrayList<String>(2);
      vals3.add("p12");
      vals3.add("p21");

      cleanUp(dbName, tblName, null);

      createDb(dbName);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

      ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(2);
      partCols.add(new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, ""));
      partCols.add(new FieldSchema("p2", serdeConstants.STRING_TYPE_NAME, ""));

      Map<String, String> serdParams = new HashMap<String, String>();
      serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");
      StorageDescriptor sd = createStorageDescriptor(tblName, partCols, null, serdParams);

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      tbl.setSd(sd);
      tbl.setPartitionKeys(partCols);
      client.createTable(tbl);
      tbl = client.getTable(dbName, tblName);

      add_partition(client, tbl, vals, "part1");
      add_partition(client, tbl, vals2, "part2");
      add_partition(client, tbl, vals3, "part3");

      checkFilter(client, dbName, tblName, "p2 = \"p21\"", 2);
      checkFilter(client, dbName, tblName, "p2 < \"p23\"", 3);
      checkFilter(client, dbName, tblName, "p2 > \"p21\"", 1);
      checkFilter(client, dbName, tblName, "p2 >= \"p21\"", 3);
      checkFilter(client, dbName, tblName, "p2 <= \"p21\"", 2);
      checkFilter(client, dbName, tblName, "p2 <> \"p12\"", 3);
      checkFilter(client, dbName, tblName, "p2 != \"p12\"", 3);
      checkFilter(client, dbName, tblName, "p2 like \"p2.*\"", 3);
      checkFilter(client, dbName, tblName, "p2 like \"p.*2\"", 1);

      try {
        checkFilter(client, dbName, tblName, "p2 !< 'dd'", 0);
        fail("Invalid operator not detected");
      } catch (MetaException e) {
        // expected exception due to lexer error
      }

      cleanUp(dbName, tblName, null);
  }

  private void checkFilter(HiveMetaStoreClient client, String dbName,
        String tblName, String filter, int expectedCount)
        throws MetaException, NoSuchObjectException, TException {
    LOG.debug("Testing filter: " + filter);
    List<Partition> partitions = client.listPartitionsByFilter(dbName,
            tblName, filter, (short) -1);

    assertEquals("Partition count expected for filter " + filter,
            expectedCount, partitions.size());
  }

  private void add_partition(HiveMetaStoreClient client, Table table,
      List<String> vals, String location) throws InvalidObjectException,
        AlreadyExistsException, MetaException, TException {

    Partition part = new Partition();
    part.setDbName(table.getDbName());
    part.setTableName(table.getTableName());
    part.setValues(vals);
    part.setParameters(new HashMap<String, String>());
    part.setSd(table.getSd().deepCopy());
    part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
    part.getSd().setLocation(table.getSd().getLocation() + location);

    client.add_partition(part);
  }

  /**
   * Tests {@link HiveMetaStoreClient#newSynchronizedClient}.  Does not
   * actually test multithreading, but does verify that the proxy
   * at least works correctly.
   */
  public void testSynchronized() throws Exception {
    int currentNumberOfDbs = client.getAllDatabases().size();

    IMetaStoreClient synchronizedClient =
      HiveMetaStoreClient.newSynchronizedClient(client);
    List<String> databases = synchronizedClient.getAllDatabases();
    assertEquals(currentNumberOfDbs, databases.size());
  }

  public void testTableFilter() throws Exception {
    try {
      String dbName = "testTableFilter";
      String owner1 = "testOwner1";
      String owner2 = "testOwner2";
      int lastAccessTime1 = 90;
      int lastAccessTime2 = 30;
      String tableName1 = "table1";
      String tableName2 = "table2";
      String tableName3 = "table3";

      client.dropTable(dbName, tableName1);
      client.dropTable(dbName, tableName2);
      client.dropTable(dbName, tableName3);
      silentDropDatabase(dbName);
      Database db = new Database();
      db.setName(dbName);
      db.setDescription("Alter Partition Test database");
      client.createDatabase(db);

      Table table1 = createTableForTestFilter(dbName,tableName1, owner1, lastAccessTime1, true);
      Table table2 = createTableForTestFilter(dbName,tableName2, owner2, lastAccessTime2, true);
      Table table3 = createTableForTestFilter(dbName,tableName3, owner1, lastAccessTime2, false);

      List<String> tableNames;
      String filter;
      //test owner
      //owner like ".*Owner.*" and owner like "test.*"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
          " like \".*Owner.*\" and " +
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
          " like  \"test.*\"";
      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(tableNames.size(), 3);
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table2.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //owner = "testOwner1"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
          " = \"testOwner1\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //lastAccessTime < 90
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
          " < 90";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table2.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //lastAccessTime > 90
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
      " > 90";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //test params
      //test_param_2 = "50"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_2 LIKE \"50\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table2.getTableName()));

      //test_param_2 = "75"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_2 LIKE \"75\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //key_dne = "50"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "key_dne LIKE \"50\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //test_param_1 != "yellow"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_1 NOT LIKE \"yellow\"";

      // Commenting as part of HIVE-12274 != and <> are not supported for CLOBs
      // tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
      // assertEquals(2, tableNames.size());

      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_1 NOT LIKE \"yellow\"";

      // tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
      // assertEquals(2, tableNames.size());

      //owner = "testOwner1" and (lastAccessTime = 30 or test_param_1 = "hi")
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
        " = \"testOwner1\" and (" +
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
        " = 30 or " +
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
        "test_param_1 LIKE \"hi\")";
      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);

      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //Negative tests
      Exception me = null;
      try {
        filter = "badKey = \"testOwner1\"";
        tableNames = client.listTableNamesByFilter(dbName, filter, (short) -1);
      } catch(MetaException e) {
        me = e;
      }
      assertNotNull(me);
      assertTrue("Bad filter key test", me.getMessage().contains(
            "Invalid key name in filter"));

      client.dropTable(dbName, tableName1);
      client.dropTable(dbName, tableName2);
      client.dropTable(dbName, tableName3);
      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTableFilter() failed.");
      throw e;
    }
  }

  private Table createTableForTestFilter(String dbName, String tableName, String owner,
    int lastAccessTime, boolean hasSecondParam) throws Exception {

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    Map<String, String> params = new HashMap<String, String>();
    params.put("sd_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd = createStorageDescriptor(tableName, cols, params, serdParams);

    Map<String, String> partitionKeys = new HashMap<String, String>();
    partitionKeys.put("ds", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("hr", serdeConstants.INT_TYPE_NAME);

    Map<String, String> tableParams =  new HashMap<String, String>();
    tableParams.put("test_param_1", "hi");
    if(hasSecondParam) {
      tableParams.put("test_param_2", "50");
    }

    Table tbl = createTable(dbName, tableName, owner, tableParams,
        partitionKeys, sd, lastAccessTime);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tableName);
    }
    return tbl;
  }
  /**
   * Verify that if another  client, either a metastore Thrift server or  a Hive CLI instance
   * renames a table recently created by this instance, and hence potentially in its cache, the
   * current instance still sees the change.
   * @throws Exception
   */
  public void testConcurrentMetastores() throws Exception {
    String dbName = "concurrentdb";
    String tblName = "concurrenttbl";
    String renameTblName = "rename_concurrenttbl";

    try {
      cleanUp(dbName, tblName, null);

      createDb(dbName);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, ""));

      Map<String, String> params = new HashMap<String, String>();
      params.put("test_param_1", "Use this for comments etc");

      Map<String, String> serdParams = new HashMap<String, String>();
      serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

      StorageDescriptor sd =  createStorageDescriptor(tblName, cols, params, serdParams);

      createTable(dbName, tblName, null, null, null, sd, 0);

      // get the table from the client, verify the name is correct
      Table tbl2 = client.getTable(dbName, tblName);

      assertEquals("Client returned table with different name.", tbl2.getTableName(), tblName);

      // Simulate renaming via another metastore Thrift server or another Hive CLI instance
      updateTableNameInDB(tblName, renameTblName);

      // get the table from the client again, verify the name has been updated
      Table tbl3 = client.getTable(dbName, renameTblName);

      assertEquals("Client returned table with different name after rename.",
          tbl3.getTableName(), renameTblName);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testConcurrentMetastores() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  public void testSimpleFunction() throws Exception {
    String dbName = "test_db";
    String funcName = "test_func";
    String className = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
    String owner = "test_owner";
    final int N_FUNCTIONS = 5;
    PrincipalType ownerType = PrincipalType.USER;
    int createTime = (int) (System.currentTimeMillis() / 1000);
    FunctionType funcType = FunctionType.JAVA;

    try {
      cleanUp(dbName, null, null);
      for (Function f : client.getAllFunctions().getFunctions()) {
        client.dropFunction(f.getDbName(), f.getFunctionName());
      }

      createDb(dbName);

      for (int i = 0; i < N_FUNCTIONS; i++) {
        createFunction(dbName, funcName + "_" + i, className, owner, ownerType, createTime, funcType, null);
      }

      // Try the different getters

      // getFunction()
      Function func = client.getFunction(dbName, funcName + "_0");
      assertEquals("function db name", dbName, func.getDbName());
      assertEquals("function name", funcName + "_0", func.getFunctionName());
      assertEquals("function class name", className, func.getClassName());
      assertEquals("function owner name", owner, func.getOwnerName());
      assertEquals("function owner type", PrincipalType.USER, func.getOwnerType());
      assertEquals("function type", funcType, func.getFunctionType());
      List<ResourceUri> resources = func.getResourceUris();
      assertTrue("function resources", resources == null || resources.size() == 0);

      boolean gotException = false;
      try {
        func = client.getFunction(dbName, "nonexistent_func");
      } catch (NoSuchObjectException e) {
        // expected failure
        gotException = true;
      }
      assertEquals(true, gotException);

      // getAllFunctions()
      GetAllFunctionsResponse response = client.getAllFunctions();
      List<Function> allFunctions = response.getFunctions();
      assertEquals(N_FUNCTIONS, allFunctions.size());
      assertEquals(funcName + "_3", allFunctions.get(3).getFunctionName());

      // getFunctions()
      List<String> funcs = client.getFunctions(dbName, "*_func_*");
      assertEquals(N_FUNCTIONS, funcs.size());
      assertEquals(funcName + "_0", funcs.get(0));

      funcs = client.getFunctions(dbName, "nonexistent_func");
      assertEquals(0, funcs.size());

      // dropFunction()
      for (int i = 0; i < N_FUNCTIONS; i++) {
        client.dropFunction(dbName, funcName + "_" + i);
      }

      // Confirm that the function is now gone
      funcs = client.getFunctions(dbName, funcName);
      assertEquals(0, funcs.size());
      response = client.getAllFunctions();
      allFunctions = response.getFunctions();
      assertEquals(0, allFunctions.size());
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testConcurrentMetastores() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  public void testFunctionWithResources() throws Exception {
    String dbName = "test_db2";
    String funcName = "test_func";
    String className = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
    String owner = "test_owner";
    PrincipalType ownerType = PrincipalType.USER;
    int createTime = (int) (System.currentTimeMillis() / 1000);
    FunctionType funcType = FunctionType.JAVA;
    List<ResourceUri> resList = new ArrayList<ResourceUri>();
    resList.add(new ResourceUri(ResourceType.JAR, "hdfs:///tmp/jar1.jar"));
    resList.add(new ResourceUri(ResourceType.FILE, "hdfs:///tmp/file1.txt"));
    resList.add(new ResourceUri(ResourceType.ARCHIVE, "hdfs:///tmp/archive1.tgz"));

    try {
      cleanUp(dbName, null, null);

      createDb(dbName);

      createFunction(dbName, funcName, className, owner, ownerType, createTime, funcType, resList);

      // Try the different getters

      // getFunction()
      Function func = client.getFunction(dbName, funcName);
      assertEquals("function db name", dbName, func.getDbName());
      assertEquals("function name", funcName, func.getFunctionName());
      assertEquals("function class name", className, func.getClassName());
      assertEquals("function owner name", owner, func.getOwnerName());
      assertEquals("function owner type", PrincipalType.USER, func.getOwnerType());
      assertEquals("function type", funcType, func.getFunctionType());
      List<ResourceUri> resources = func.getResourceUris();
      assertEquals("Resource list size", resList.size(), resources.size());
      for (ResourceUri res : resources) {
        assertTrue("Matching resource " + res.getResourceType() + " " + res.getUri(),
            resList.indexOf(res) >= 0);
      }

      client.dropFunction(dbName, funcName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testConcurrentMetastores() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  /**
   * This method simulates another Hive metastore renaming a table, by accessing the db and
   * updating the name.
   *
   * Unfortunately, derby cannot be run in two different JVMs simultaneously, but the only way
   * to rename without having it put in this client's cache is to run a metastore in a separate JVM,
   * so this simulation is required.
   * @param oldTableName
   * @param newTableName
   * @throws SQLException
   */
  private void updateTableNameInDB(String oldTableName, String newTableName) throws SQLException {
    String connectionStr = HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
    int interval= 1;
    int attempts = 1;


    Utilities.SQLCommand<Void> execUpdate = new Utilities.SQLCommand<Void>() {
      @Override
      public Void run(PreparedStatement stmt) throws SQLException {
        stmt.executeUpdate();
        return null;
      }
    };

    Connection conn = Utilities.connectWithRetry(connectionStr, interval, attempts);

    PreparedStatement updateStmt = Utilities.prepareWithRetry(conn,
        "UPDATE TBLS SET tbl_name = '" + newTableName + "' WHERE tbl_name = '" + oldTableName + "'",
        interval, attempts);

    Utilities.executeWithRetry(execUpdate, updateStmt, interval, attempts);
  }

  private void cleanUp(String dbName, String tableName, String typeName) throws Exception {
    if(dbName != null && tableName != null) {
      client.dropTable(dbName, tableName);
    }
    if(dbName != null) {
      silentDropDatabase(dbName);
    }
    if(typeName != null) {
      client.dropType(typeName);
    }
  }

  private Database createDb(String dbName) throws Exception {
    if(null == dbName) { return null; }
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    return db;
  }

  private Type createType(String typeName, Map<String, String> fields) throws Throwable {
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<FieldSchema>(fields.size()));
    for(String fieldName : fields.keySet()) {
      typ1.getFields().add(
          new FieldSchema(fieldName, fields.get(fieldName), ""));
    }
    client.createType(typ1);
    return typ1;
  }

  /**
   * Creates a simple table under specified database
   * @param dbName    the database name that the table will be created under
   * @param tableName the table name to be created
   * @throws Exception
   */
  private void createTable(String dbName, String tableName)
      throws Exception {
    List<FieldSchema> columns = new ArrayList<FieldSchema>();
    columns.add(new FieldSchema("foo", "string", ""));
    columns.add(new FieldSchema("bar", "string", ""));

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd =  createStorageDescriptor(tableName, columns, null, serdParams);

    createTable(dbName, tableName, null, null, null, sd, 0);
  }

  @Test
  public void testTransactionalValidation() throws Throwable {
    String dbName = "acidDb";
    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    String tblName = "acidTable";
    String owner = "acid";
    Map<String, String> fields = new HashMap<String, String>();
    fields.put("name", serdeConstants.STRING_TYPE_NAME);
    fields.put("income", serdeConstants.INT_TYPE_NAME);

    Type type = createType("Person", fields);

    Map<String, String> params = new HashMap<String, String>();
    params.put("transactional", "");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");
    StorageDescriptor sd =  createStorageDescriptor(tblName, type.getFields(), params, serdParams);
    sd.setNumBuckets(0);
    sd.unsetBucketCols();

    /// CREATE TABLE scenarios

    // Fail - No "transactional" property is specified
    try {
      Table t = createTable(dbName, tblName, owner, params, null, sd, 0);
      Assert.assertTrue("Expected exception", false);
    } catch (MetaException e) {
      Assert.assertEquals("'transactional' property of TBLPROPERTIES may only have value 'true'", e.getMessage());
    }

    // Fail - "transactional" property is set to an invalid value
    try {
      params.clear();
      params.put("transactional", "foobar");
      Table t = createTable(dbName, tblName, owner, params, null, sd, 0);
      Assert.assertTrue("Expected exception", false);
    } catch (MetaException e) {
      Assert.assertEquals("'transactional' property of TBLPROPERTIES may only have value 'true'", e.getMessage());
    }

    // Fail - "transactional" is set to true, but the table is not bucketed
    try {
      params.clear();
      params.put("transactional", "true");
      Table t = createTable(dbName, tblName, owner, params, null, sd, 0);
      Assert.assertTrue("Expected exception", false);
    } catch (MetaException e) {
      Assert.assertEquals("The table must be bucketed and stored using an ACID compliant format (such as ORC)", e.getMessage());
    }

    // Fail - "transactional" is set to true, and the table is bucketed, but doesn't use ORC
    try {
      params.clear();
      params.put("transactional", "true");
      List<String> bucketCols = new ArrayList<String>();
      bucketCols.add("income");
      sd.setBucketCols(bucketCols);
      Table t = createTable(dbName, tblName, owner, params, null, sd, 0);
      Assert.assertTrue("Expected exception", false);
    } catch (MetaException e) {
      Assert.assertEquals("The table must be bucketed and stored using an ACID compliant format (such as ORC)", e.getMessage());
    }

    // Succeed - "transactional" is set to true, and the table is bucketed, and uses ORC
    params.clear();
    params.put("transactional", "true");
    List<String> bucketCols = new ArrayList<String>();
    bucketCols.add("income");
    sd.setBucketCols(bucketCols);
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
    Table t = createTable(dbName, tblName, owner, params, null, sd, 0);
    Assert.assertTrue("CREATE TABLE should succeed", "true".equals(t.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)));

    /// ALTER TABLE scenarios

    // Fail - trying to set "transactional" to "false" is not allowed
    try {
      params.clear();
      params.put("transactional", "false");
      t = new Table();
      t.setParameters(params);
      client.alter_table(dbName, tblName, t);
      Assert.assertTrue("Expected exception", false);
    } catch (MetaException e) {
      Assert.assertEquals("TBLPROPERTIES with 'transactional'='true' cannot be unset", e.getMessage());
    }

    // Fail - trying to set "transactional" to "true" but doesn't satisfy bucketing and Input/OutputFormat requirement
    try {
      tblName += "1";
      params.clear();
      sd.unsetBucketCols();
      t = createTable(dbName, tblName, owner, params, null, sd, 0);
      params.put("transactional", "true");
      t.setParameters(params);
      client.alter_table(dbName, tblName, t);
      Assert.assertTrue("Expected exception", false);
    } catch (MetaException e) {
      Assert.assertEquals("The table must be bucketed and stored using an ACID compliant format (such as ORC)", e.getMessage());
    }

    // Succeed - trying to set "transactional" to "true", and satisfies bucketing and Input/OutputFormat requirement
    tblName += "2";
    params.clear();
    sd.setNumBuckets(1);
    sd.setBucketCols(bucketCols);
    t = createTable(dbName, tblName, owner, params, null, sd, 0);
    params.put("transactional", "true");
    t.setParameters(params);
    t.setPartitionKeys(Collections.EMPTY_LIST);
    client.alter_table(dbName, tblName, t);
    Assert.assertTrue("ALTER TABLE should succeed", "true".equals(t.getParameters().get(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL)));
  }

  private Table createTable(String dbName, String tblName, String owner,
      Map<String,String> tableParams, Map<String, String> partitionKeys,
      StorageDescriptor sd, int lastAccessTime) throws Exception {
    Table tbl = new Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    if(tableParams != null) {
      tbl.setParameters(tableParams);
    }

    if(owner != null) {
      tbl.setOwner(owner);
    }

    if(partitionKeys != null) {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>(partitionKeys.size()));
      for(String key : partitionKeys.keySet()) {
        tbl.getPartitionKeys().add(
            new FieldSchema(key, partitionKeys.get(key), ""));
      }
    }

    tbl.setSd(sd);
    tbl.setLastAccessTime(lastAccessTime);
    tbl.setTableType(TableType.MANAGED_TABLE.toString());

    client.createTable(tbl);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and ALTER TABLE relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tblName);
    }

    return tbl;
  }

  private StorageDescriptor createStorageDescriptor(String tableName,
    List<FieldSchema> cols, Map<String, String> params, Map<String, String> serdParams)  {
    StorageDescriptor sd = new StorageDescriptor();

    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(params);
    sd.setBucketCols(new ArrayList<String>(2));
    sd.getBucketCols().add("name");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tableName);
    sd.getSerdeInfo().setParameters(serdParams);
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
    sd.setInputFormat(HiveInputFormat.class.getName());
    sd.setOutputFormat(HiveOutputFormat.class.getName());
    
    return sd;
  }

  private List<Partition> createPartitions(String dbName, Table tbl,
      List<List<String>> values)  throws Throwable {
    int i = 1;
    List<Partition> partitions = new ArrayList<Partition>();
    for(List<String> vals : values) {
      Partition part = makePartitionObject(dbName, tbl.getTableName(), vals, tbl, "/part"+i);
      i++;
      // check if the partition exists (it shouldn't)
      boolean exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tbl.getTableName(), vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("getPartition() should have thrown NoSuchObjectException", exceptionThrown);
      Partition retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);
      partitions.add(retp);
    }
    return partitions;
  }

  private void createMultiPartitionTableSchema(String dbName, String tblName,
      String typeName, List<List<String>> values)
      throws Throwable, MetaException, TException, NoSuchObjectException {
    createDb(dbName);

    Map<String, String> fields = new HashMap<String, String>();
    fields.put("name", serdeConstants.STRING_TYPE_NAME);
    fields.put("income", serdeConstants.INT_TYPE_NAME);

    Type typ1 = createType(typeName, fields);

    Map<String , String> partitionKeys = new HashMap<String, String>();
    partitionKeys.put("ds", serdeConstants.STRING_TYPE_NAME);
    partitionKeys.put("hr", serdeConstants.STRING_TYPE_NAME);

    Map<String, String> params = new HashMap<String, String>();
    params.put("test_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(serdeConstants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd =  createStorageDescriptor(tblName, typ1.getFields(), params, serdParams);

    Table tbl = createTable(dbName, tblName, null, null, partitionKeys, sd, 0);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tblName);
    }

    createPartitions(dbName, tbl, values);
  }

  @Test
  public void testDBOwner() throws NoSuchObjectException, MetaException, TException {
    Database db = client.getDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    assertEquals(db.getOwnerName(), HiveMetaStore.PUBLIC);
    assertEquals(db.getOwnerType(), PrincipalType.ROLE);
  }

  /**
   * Test changing owner and owner type of a database
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  @Test
  public void testDBOwnerChange() throws NoSuchObjectException, MetaException, TException {
    final String dbName = "alterDbOwner";
    final String user1 = "user1";
    final String user2 = "user2";
    final String role1 = "role1";

    silentDropDatabase(dbName);
    Database db = new Database();
    db.setName(dbName);
    db.setOwnerName(user1);
    db.setOwnerType(PrincipalType.USER);

    client.createDatabase(db);
    checkDbOwnerType(dbName, user1, PrincipalType.USER);

    db.setOwnerName(user2);
    client.alterDatabase(dbName, db);
    checkDbOwnerType(dbName, user2, PrincipalType.USER);

    db.setOwnerName(role1);
    db.setOwnerType(PrincipalType.ROLE);
    client.alterDatabase(dbName, db);
    checkDbOwnerType(dbName, role1, PrincipalType.ROLE);

  }

  /**
   * Test table objects can be retrieved in batches
   * @throws Exception
   */
  @Test
  public void testGetTableObjects() throws Exception {
    String dbName = "db";
    List<String> tableNames = Arrays.asList("table1", "table2", "table3", "table4", "table5");

    // Setup
    silentDropDatabase(dbName);

    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    for (String tableName : tableNames) {
      createTable(dbName, tableName);
    }

    // Test
    List<Table> tableObjs = client.getTableObjectsByName(dbName, tableNames);

    // Verify
    assertEquals(tableNames.size(), tableObjs.size());
    for(Table table : tableObjs) {
      assertTrue(tableNames.contains(table.getTableName().toLowerCase()));
    }

    // Cleanup
    client.dropDatabase(dbName, true, true, true);
  }

  private void checkDbOwnerType(String dbName, String ownerName, PrincipalType ownerType)
      throws NoSuchObjectException, MetaException, TException {
    Database db = client.getDatabase(dbName);
    assertEquals("Owner name", ownerName, db.getOwnerName());
    assertEquals("Owner type", ownerType, db.getOwnerType());
  }

  private void createFunction(String dbName, String funcName, String className,
      String ownerName, PrincipalType ownerType, int createTime,
      org.apache.hadoop.hive.metastore.api.FunctionType functionType, List<ResourceUri> resources)
          throws Exception {
    Function func = new Function(funcName, dbName, className,
        ownerName, ownerType, createTime, functionType, resources);
    client.createFunction(func);
  }

  public void testRetriableClientWithConnLifetime() throws Exception {

    HiveConf conf = new HiveConf(hiveConf);
    conf.setLong(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME.name(), 60);
    long timeout = 65 * 1000; // Lets use a timeout more than the socket lifetime to simulate a reconnect

    // Test a normal retriable client
    IMetaStoreClient client = RetryingMetaStoreClient.getProxy(conf, getHookLoader(), HiveMetaStoreClient.class.getName());
    client.getAllDatabases();
    client.close();

    // Connect after the lifetime, there should not be any failures
    client = RetryingMetaStoreClient.getProxy(conf, getHookLoader(), HiveMetaStoreClient.class.getName());
    Thread.sleep(timeout);
    client.getAllDatabases();
    client.close();
  }

  public void testJDOPersistanceManagerCleanup() throws Exception {
    if (isThriftClient == false) {
      return;
    }

    int numObjectsBeforeClose =  getJDOPersistanceManagerCacheSize();
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(hiveConf);
    closingClient.getAllDatabases();
    closingClient.close();
    Thread.sleep(5 * 1000); // give HMS time to handle close request
    int numObjectsAfterClose =  getJDOPersistanceManagerCacheSize();
    Assert.assertTrue(numObjectsBeforeClose == numObjectsAfterClose);

    HiveMetaStoreClient nonClosingClient = new HiveMetaStoreClient(hiveConf);
    nonClosingClient.getAllDatabases();
    // Drop connection without calling close. HMS thread deleteContext
    // will trigger cleanup
    nonClosingClient.getTTransport().close();
    Thread.sleep(5 * 1000);
    int numObjectsAfterDroppedConnection =  getJDOPersistanceManagerCacheSize();
    Assert.assertTrue(numObjectsAfterClose == numObjectsAfterDroppedConnection);
  }

  private static int getJDOPersistanceManagerCacheSize() {
    JDOPersistenceManagerFactory jdoPmf;
    Set<JDOPersistenceManager> pmCacheObj;
    Field pmCache;
    Field pmf;
    try {
      pmf = ObjectStore.class.getDeclaredField("pmf");
      if (pmf != null) {
        pmf.setAccessible(true);
        jdoPmf = (JDOPersistenceManagerFactory) pmf.get(null);
        pmCache = JDOPersistenceManagerFactory.class.getDeclaredField("pmCache");
        if (pmCache != null) {
          pmCache.setAccessible(true);
          pmCacheObj = (Set<JDOPersistenceManager>) pmCache.get(jdoPmf);
          if (pmCacheObj != null) {
            return pmCacheObj.size();
          }
        }
      }
    } catch (Exception ex) {
      System.out.println(ex);
    }
    return -1;
  }

  private HiveMetaHookLoader getHookLoader() {
    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(
          org.apache.hadoop.hive.metastore.api.Table tbl)
          throws MetaException {
        return null;
      }
    };
    return hookLoader;
  }

  public void testValidateTableCols() throws Throwable {

    try {
      String dbName = "compdb";
      String tblName = "comptbl";

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      Database db = new Database();
      db.setName(dbName);
      db.setDescription("Validate Table Columns test");
      client.createDatabase(db);

      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

      Table tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(tblName);
      StorageDescriptor sd = new StorageDescriptor();
      tbl.setSd(sd);
      sd.setCols(cols);
      sd.setCompressed(false);
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters()
          .put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(LazySimpleSerDe.class.getName());
      sd.setInputFormat(HiveInputFormat.class.getName());
      sd.setOutputFormat(HiveOutputFormat.class.getName());
      sd.setSortCols(new ArrayList<Order>());

      client.createTable(tbl);
      if (isThriftClient) {
        tbl = client.getTable(dbName, tblName);
      }

      List<String> expectedCols = Lists.newArrayList();
      expectedCols.add("name");
      ObjectStore objStore = new ObjectStore();
      try {
        objStore.validateTableCols(tbl, expectedCols);
      } catch (MetaException ex) {
        throw new RuntimeException(ex);
      }

      expectedCols.add("doesntExist");
      boolean exceptionFound = false;
      try {
        objStore.validateTableCols(tbl, expectedCols);
      } catch (MetaException ex) {
        assertEquals(ex.getMessage(),
            "Column doesntExist doesn't exist in table comptbl in database compdb");
        exceptionFound = true;
      }
      assertTrue(exceptionFound);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testValidateTableCols() failed.");
      throw e;
    }
  }
}
