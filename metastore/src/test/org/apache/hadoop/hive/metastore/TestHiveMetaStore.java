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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

public abstract class TestHiveMetaStore extends TestCase {
  protected static HiveMetaStoreClient client;
  protected static HiveConf hiveConf;
  protected static Warehouse warehouse;
  protected static boolean isThriftClient = false;

  private static final String TEST_DB1_NAME = "testdb1";
  private static final String TEST_DB2_NAME = "testdb2";

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
          new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
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
          .put(Constants.SERIALIZATION_FORMAT, "1");
      sd.setSortCols(new ArrayList<Order>());

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
          new FieldSchema("ds", Constants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr", Constants.STRING_TYPE_NAME, ""));

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

      String partName = "ds=2008-07-01 14%3A13%3A12/hr=14";
      String part2Name = "ds=2008-07-01 14%3A13%3A12/hr=15";
      String part3Name ="ds=2008-07-02 14%3A13%3A12/hr=15";
      String part4Name ="ds=2008-07-03 14%3A13%3A12/hr=151";

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
      assertTrue("Should have returned 4 partition names", partialNames.size() == 4);
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
      warehouse.mkdirs(mp5Path);
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
      List<String> ptnVals, Table tbl, String ptnLocationSuffix) {
    Partition part4 = new Partition();
    part4.setDbName(dbName);
    part4.setTableName(tblName);
    part4.setValues(ptnVals);
    part4.setParameters(new HashMap<String, String>());
    part4.setSd(tbl.getSd().deepCopy());
    part4.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo().deepCopy());
    part4.getSd().setLocation(tbl.getSd().getLocation() + ptnLocationSuffix);
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
      cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));

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
          .put(Constants.SERIALIZATION_FORMAT, "1");
      sd.setSortCols(new ArrayList<Order>());

      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds", Constants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr", Constants.INT_TYPE_NAME, ""));

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
      client.alter_partition(dbName, tblName, part2);

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
      cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));

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
          .put(Constants.SERIALIZATION_FORMAT, "1");
      sd.setSortCols(new ArrayList<Order>());

      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds", Constants.STRING_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr", Constants.INT_TYPE_NAME, ""));

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
      client.createDatabase(db);

      db = client.getDatabase(TEST_DB1_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB1_NAME, db.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDatabasePath(db).toString(), db.getLocationUri());

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

      db = new Database();
      db.setName(TEST_DB1_NAME);
      dbLocation =
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
      }
      assertTrue("Database creation succeeded even with permission problem", createFailed);

      boolean objectNotExist = false;
      try {
        client.getDatabase(TEST_DB1_NAME);
      } catch (NoSuchObjectException e) {
        objectNotExist = true;
      }
      assertTrue("Database " + TEST_DB1_NAME + " exists ", objectNotExist);

      // Cleanup
      fs.setPermission(
          new Path(HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/test"),
          new FsPermission((short) 755));
      fs.delete(new Path(HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/test"), true);


      db = new Database();
      db.setName(TEST_DB1_NAME);
      dbLocation =
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.METASTOREWAREHOUSE) + "/_testDB_file_";
      fs = FileSystem.get(new Path(dbLocation).toUri(), hiveConf);
      fs.createNewFile(new Path(dbLocation));
      fs.deleteOnExit(new Path(dbLocation));
      db.setLocationUri(dbLocation);

      createFailed = false;
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
      client.dropType(Constants.INT_TYPE_NAME);

      Type typ1 = new Type();
      typ1.setName(Constants.INT_TYPE_NAME);
      boolean ret = client.createType(typ1);
      assertTrue("Unable to create type", ret);

      Type typ1_2 = client.getType(Constants.INT_TYPE_NAME);
      assertNotNull(typ1_2);
      assertEquals(typ1.getName(), typ1_2.getName());

      ret = client.dropType(Constants.INT_TYPE_NAME);
      assertTrue("unable to drop type integer", ret);

      boolean exceptionThrown = false;
      try {
        client.getType(Constants.INT_TYPE_NAME);
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
          new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
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
          new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
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
          new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
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
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
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
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");

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

      assertEquals(foundTables.size(), 2);
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
      invCols.add(new FieldSchema("n-ame", Constants.STRING_TYPE_NAME, ""));
      invCols.add(new FieldSchema("in.come", Constants.INT_TYPE_NAME, ""));

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
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
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
      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
      cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));

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
          new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", Constants.INT_TYPE_NAME, ""));
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
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "9");
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());

      tbl.setPartitionKeys(new ArrayList<FieldSchema>(2));
      tbl.getPartitionKeys().add(
          new FieldSchema("ds",
              org.apache.hadoop.hive.serde.Constants.DATE_TYPE_NAME, ""));
      tbl.getPartitionKeys().add(
          new FieldSchema("hr",
              org.apache.hadoop.hive.serde.Constants.INT_TYPE_NAME, ""));

      client.createTable(tbl);

      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals(tbl2.getDbName(), dbName);
      assertEquals(tbl2.getTableName(), tblName);
      assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
      assertFalse(tbl2.getSd().isCompressed());
      assertEquals(tbl2.getSd().getNumBuckets(), 1);

      assertEquals("Use this for comments etc", tbl2.getSd().getParameters()
          .get("test_param_1"));
      assertEquals("name", tbl2.getSd().getBucketCols().get(0));

      assertNotNull(tbl2.getPartitionKeys());
      assertEquals(2, tbl2.getPartitionKeys().size());
      assertEquals(Constants.DATE_TYPE_NAME, tbl2.getPartitionKeys().get(0)
          .getType());
      assertEquals(Constants.INT_TYPE_NAME, tbl2.getPartitionKeys().get(1)
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
      cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));

      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(tbl.getTableName());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      sd.setParameters(new HashMap<String, String>());
      sd.getSerdeInfo().getParameters().put(
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "9");
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());

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
      } catch (TException e) {
        e.printStackTrace();
          assert (false);
      } catch (ConfigValSecurityException e) {
        e.printStackTrace();
        assert (false);
      }
    }

    boolean threwException = false;
    try {
      // Attempting to get the password should throw an exception
      client.getConfigValue("javax.jdo.option.ConnectionPassword", "password");
    } catch (TException e) {
      e.printStackTrace();
      assert (false);
    } catch (ConfigValSecurityException e) {
      threwException = true;
    }
    assert (threwException);
  }

  private static void adjust(HiveMetaStoreClient client, Partition part,
      String dbName, String tblName)
  throws NoSuchObjectException, MetaException, TException {
    Partition part_get = client.getPartition(dbName, tblName, part.getValues());
    part.setCreateTime(part_get.getCreateTime());
    part.putToParameters(org.apache.hadoop.hive.metastore.api.Constants.DDL_TIME, Long.toString(part_get.getCreateTime()));
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

    List<String> vals = new ArrayList<String>(3);
    vals.add("p11");
    vals.add("p21");
    vals.add("p31");
    List <String> vals2 = new ArrayList<String>(3);
    vals2.add("p11");
    vals2.add("p22");
    vals2.add("p31");
    List <String> vals3 = new ArrayList<String>(3);
    vals3.add("p12");
    vals3.add("p21");
    vals3.add("p31");
    List <String> vals4 = new ArrayList<String>(3);
    vals4.add("p12");
    vals4.add("p23");
    vals4.add("p31");
    List <String> vals5 = new ArrayList<String>(3);
    vals5.add("p13");
    vals5.add("p24");
    vals5.add("p31");
    List <String> vals6 = new ArrayList<String>(3);
    vals6.add("p13");
    vals6.add("p25");
    vals6.add("p31");

    silentDropDatabase(dbName);

    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("c1", Constants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("c2", Constants.INT_TYPE_NAME, ""));

    ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(3);
    partCols.add(new FieldSchema("p1", Constants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema("p2", Constants.STRING_TYPE_NAME, ""));
    partCols.add(new FieldSchema("p3", Constants.INT_TYPE_NAME, ""));

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
        .put(Constants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());

    tbl.setPartitionKeys(partCols);
    client.createTable(tbl);

    tbl = client.getTable(dbName, tblName);

    add_partition(client, tbl, vals, "part1");
    add_partition(client, tbl, vals2, "part2");
    add_partition(client, tbl, vals3, "part3");
    add_partition(client, tbl, vals4, "part4");
    add_partition(client, tbl, vals5, "part5");
    add_partition(client, tbl, vals6, "part6");

    checkFilter(client, dbName, tblName, "p1 = \"p11\"", 2);
    checkFilter(client, dbName, tblName, "p1 = \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p2 = \"p21\"", 2);
    checkFilter(client, dbName, tblName, "p2 = \"p23\"", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" and p2=\"p22\"", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p2=\"p23\"", 3);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);

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

    checkFilter(client, dbName, tblName, "p1 > \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 >= \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 < \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 <= \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 <> \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 like \"p1.*\"", 6);
    checkFilter(client, dbName, tblName, "p2 like \"p.*3\"", 1);

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
          "database/table does not exist"));

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
      cols.add(new FieldSchema("c1", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("c2", Constants.INT_TYPE_NAME, ""));

      ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(1);
      partCols.add(new FieldSchema("p1", Constants.STRING_TYPE_NAME, ""));

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
          .put(Constants.SERIALIZATION_FORMAT, "1");
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
      cols.add(new FieldSchema("c1", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("c2", Constants.INT_TYPE_NAME, ""));

      ArrayList<FieldSchema> partCols = new ArrayList<FieldSchema>(2);
      partCols.add(new FieldSchema("p1", Constants.STRING_TYPE_NAME, ""));
      partCols.add(new FieldSchema("p2", Constants.STRING_TYPE_NAME, ""));

      Map<String, String> serdParams = new HashMap<String, String>();
      serdParams.put(Constants.SERIALIZATION_FORMAT, "1");
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
    part.setSd(table.getSd());
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
    IMetaStoreClient synchronizedClient =
      HiveMetaStoreClient.newSynchronizedClient(client);
    List<String> databases = synchronizedClient.getAllDatabases();
    assertEquals(1, databases.size());
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
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_OWNER +
          " like \".*Owner.*\" and " +
          org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_OWNER +
          " like  \"test.*\"";
      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(tableNames.size(), 3);
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table2.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //owner = "testOwner1"
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_OWNER +
          " = \"testOwner1\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //lastAccessTime < 90
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_LAST_ACCESS +
          " < 90";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table2.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //lastAccessTime > 90
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_LAST_ACCESS +
      " > 90";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //test params
      //test_param_2 = "50"
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_2 = \"50\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table2.getTableName()));

      //test_param_2 = "75"
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_2 = \"75\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //key_dne = "50"
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_PARAMS +
          "key_dne = \"50\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //test_param_1 != "yellow"
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_1 <> \"yellow\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
      assertEquals(2, tableNames.size());

      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_1 != \"yellow\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
      assertEquals(2, tableNames.size());

      //owner = "testOwner1" and (lastAccessTime = 30 or test_param_1 = "hi")
      filter = org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_OWNER +
        " = \"testOwner1\" and (" +
        org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_LAST_ACCESS +
        " = 30 or " +
        org.apache.hadoop.hive.metastore.api.Constants.HIVE_FILTER_FIELD_PARAMS +
        "test_param_1 = \"hi\")";
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
    cols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    cols.add(new FieldSchema("income", Constants.INT_TYPE_NAME, ""));

    Map<String, String> params = new HashMap<String, String>();
    params.put("sd_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(Constants.SERIALIZATION_FORMAT, "1");

    StorageDescriptor sd = createStorageDescriptor(tableName, cols, params, serdParams);

    Map<String, String> partitionKeys = new HashMap<String, String>();
    partitionKeys.put("ds", Constants.STRING_TYPE_NAME);
    partitionKeys.put("hr", Constants.INT_TYPE_NAME);

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
      cols.add(new FieldSchema("c1", Constants.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("c2", Constants.INT_TYPE_NAME, ""));

      Map<String, String> params = new HashMap<String, String>();
      params.put("test_param_1", "Use this for comments etc");

      Map<String, String> serdParams = new HashMap<String, String>();
      serdParams.put(Constants.SERIALIZATION_FORMAT, "1");

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
    int interval= HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.METASTOREINTERVAL);
    int attempts = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.METASTOREATTEMPTS);


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

    client.createTable(tbl);
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
        .put(Constants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());

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
    fields.put("name", Constants.STRING_TYPE_NAME);
    fields.put("income", Constants.INT_TYPE_NAME);

    Type typ1 = createType(typeName, fields);

    Map<String , String> partitionKeys = new HashMap<String, String>();
    partitionKeys.put("ds", Constants.STRING_TYPE_NAME);
    partitionKeys.put("hr", Constants.STRING_TYPE_NAME);

    Map<String, String> params = new HashMap<String, String>();
    params.put("test_param_1", "Use this for comments etc");

    Map<String, String> serdParams = new HashMap<String, String>();
    serdParams.put(Constants.SERIALIZATION_FORMAT, "1");

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
}
