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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
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
      List<String> vals = new ArrayList<String>(2);
      vals.add("2008-07-01 14:13:12");
      vals.add("14");
      List <String> vals2 = new ArrayList<String>(2);
      vals2.add("2008-07-01 14:13:12");
      vals2.add("15");
      List <String> vals3 = new ArrayList<String>(2);
      vals3 = new ArrayList<String>(2);
      vals3.add("2008-07-02 14:13:12");
      vals3.add("15");

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

      Partition part2 = new Partition();
      part2.setDbName(dbName);
      part2.setTableName(tblName);
      part2.setValues(vals2);
      part2.setParameters(new HashMap<String, String>());
      part2.setSd(tbl.getSd());
      part2.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
      part2.getSd().setLocation(tbl.getSd().getLocation() + "/part2");

      Partition part3 = new Partition();
      part3.setDbName(dbName);
      part3.setTableName(tblName);
      part3.setValues(vals3);
      part3.setParameters(new HashMap<String, String>());
      part3.setSd(tbl.getSd());
      part3.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
      part3.getSd().setLocation(tbl.getSd().getLocation() + "/part2");

      // check if the partition exists (it shouldn;t)
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
      Partition retp2 = client.add_partition(part2);
      assertNotNull("Unable to create partition " + part2, retp2);
      Partition retp3 = client.add_partition(part3);
      assertNotNull("Unable to create partition " + part3, retp3);

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

      // Test partition listing with a partial spec - hr is specified but ds is not
      parts.clear();
      parts.add(part2);
      parts.add(part3);

      partialVals.clear();
      partialVals.add("");
      partialVals.add(vals2.get(1));

      partial = client.listPartitions(dbName, tblName, partialVals, (short) -1);
      assertTrue("Should have returned 2 partitions", partial.size() == 2);
      assertTrue("Not all parts returned", partial.containsAll(parts));

      partNames.clear();
      partNames.add(part2Name);
      partNames.add(part3Name);
      partialNames = client.listPartitionNames(dbName, tblName, partialVals,
          (short) -1);
      assertTrue("Should have returned 2 partition names", partialNames.size() == 2);
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

      Path partPath = new Path(part2.getSd().getLocation());
      FileSystem fs = FileSystem.get(partPath.toUri(), hiveConf);


      assertTrue(fs.exists(partPath));
      client.dropPartition(dbName, tblName, part.getValues(), true);
      assertFalse(fs.exists(partPath));

      // Test append_partition_by_name
      client.appendPartition(dbName, tblName, partName);
      Partition part4 = client.getPartition(dbName, tblName, part.getValues());
      assertTrue("Append partition by name failed", part4.getValues().equals(vals));;
      Path part4Path = new Path(part4.getSd().getLocation());
      assertTrue(fs.exists(part4Path));

      // Test drop_partition_by_name
      assertTrue("Drop partition by name failed",
          client.dropPartition(dbName, tblName, partName, true));
      assertFalse(fs.exists(part4Path));

      // add the partition again so that drop table with a partition can be
      // tested
      retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);

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
          warehouse.getDefaultDatabasePath(TEST_DB1_NAME).toString(), db.getLocationUri());

      Database db2 = new Database();
      db2.setName(TEST_DB2_NAME);
      client.createDatabase(db2);

      db2 = client.getDatabase(TEST_DB2_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB2_NAME, db2.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDefaultDatabasePath(TEST_DB2_NAME).toString(), db2.getLocationUri());

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
}
