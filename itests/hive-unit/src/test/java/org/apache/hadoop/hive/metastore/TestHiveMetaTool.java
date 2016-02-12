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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.tools.HiveMetaTool;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.util.StringUtils;

public class TestHiveMetaTool extends TestCase {

  private HiveMetaStoreClient client;

  private PrintStream originalOut;
  private OutputStream os;
  private PrintStream ps;
  private String locationUri;
  private final String dbName = "TestHiveMetaToolDB";
  private final String typeName = "Person";
  private final String tblName = "simpleTbl";
  private final String badTblName = "badSimpleTbl";


  private void dropDatabase(String dbName) throws Exception {
    try {
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException e) {
    } catch (InvalidOperationException e) {
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    try {
      HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
      client = new HiveMetaStoreClient(hiveConf);

      // Setup output stream to redirect output to
      os = new ByteArrayOutputStream();
      ps = new PrintStream(os);

      // create a dummy database and a couple of dummy tables
      Database db = new Database();
      db.setName(dbName);
      client.dropTable(dbName, tblName);
      client.dropTable(dbName, badTblName);
      dropDatabase(dbName);
      client.createDatabase(db);
      locationUri = db.getLocationUri();
      String avroUri = "hdfs://nn.example.com/warehouse/hive/ab.avsc";
      String badAvroUri = new String("hdfs:/hive");

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
      Map<String, String> parameters = new HashMap<>();
      parameters.put(AvroSerdeUtils.SCHEMA_URL, avroUri);
      tbl.setParameters(parameters);
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
      sd.getParameters().put(AvroSerdeUtils.SCHEMA_URL, avroUri);
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.avro.AvroSerDe.class.getName());
      sd.setInputFormat(AvroContainerInputFormat.class.getName());
      sd.setOutputFormat(AvroContainerOutputFormat.class.getName());
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
      client.createTable(tbl);

      //create a table with bad avro uri
      tbl = new Table();
      tbl.setDbName(dbName);
      tbl.setTableName(badTblName);
      sd = new StorageDescriptor();
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
      sd.getParameters().put(AvroSerdeUtils.SCHEMA_URL, badAvroUri);
      sd.getSerdeInfo().setSerializationLib(
          org.apache.hadoop.hive.serde2.avro.AvroSerDe.class.getName());
      sd.setInputFormat(AvroContainerInputFormat.class.getName());
      sd.setOutputFormat(AvroContainerOutputFormat.class.getName());
      
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
      client.createTable(tbl);
      client.close();
    } catch (Exception e) {
      System.err.println("Unable to setup the hive metatool test");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private void redirectOutputStream() {

    originalOut = System.out;
    System.setOut(ps);

  }

  private void restoreOutputStream() {

    System.setOut(originalOut);
  }

  public void testListFSRoot() throws Exception {

    redirectOutputStream();
    String[] args = new String[1];
    args[0] = new String("-listFSRoot");

    try {
      HiveMetaTool.main(args);
      String out = os.toString();
      boolean b = out.contains(locationUri);
      assertTrue(b);
    } finally {
      restoreOutputStream();
      System.out.println("Completed testListFSRoot");
    }
  }

  public void testExecuteJDOQL() throws Exception {

    redirectOutputStream();
    String[] args = new String[2];
    args[0] = new String("-executeJDOQL");
    args[1] = new String("select locationUri from org.apache.hadoop.hive.metastore.model.MDatabase");

    try {
      HiveMetaTool.main(args);
      String out = os.toString();
      boolean b = out.contains(locationUri);
      assertTrue(b);
    } finally {
      restoreOutputStream();
      System.out.println("Completed testExecuteJDOQL");
    }
  }

  public void testUpdateFSRootLocation() throws Exception {
    redirectOutputStream();
    String oldLocationUri = "hdfs://nn.example.com/";
    String newLocationUri = "hdfs://nn-ha-uri/";
    String oldSchemaUri = "hdfs://nn.example.com/warehouse/hive/ab.avsc";
    String newSchemaUri = "hdfs://nn-ha-uri/warehouse/hive/ab.avsc";

    String[] args = new String[5];
    args[0] = new String("-updateLocation");
    args[1] = new String(newLocationUri);
    args[2] = new String(oldLocationUri);
    args[3] = new String("-tablePropKey");
    args[4] = new String("avro.schema.url");

    try {
      checkAvroSchemaURLProps(client.getTable(dbName, tblName), oldSchemaUri);

      // perform HA upgrade
      HiveMetaTool.main(args);
      String out = os.toString();
      boolean b = out.contains(newLocationUri);
      restoreOutputStream();
      assertTrue(b);
      checkAvroSchemaURLProps(client.getTable(dbName,tblName), newSchemaUri);

      //restore the original HDFS root
      args[1] = new String(oldLocationUri);
      args[2] = new String(newLocationUri);
      redirectOutputStream();
      HiveMetaTool.main(args);
      checkAvroSchemaURLProps(client.getTable(dbName,tblName), oldSchemaUri);
      restoreOutputStream();
    } finally {
      restoreOutputStream();
      System.out.println("Completed testUpdateFSRootLocation..");
    }
  }

  private void checkAvroSchemaURLProps(Table table, String expectedURL) {
    assertEquals(expectedURL, table.getParameters().get(AvroSerdeUtils.SCHEMA_URL));
    assertEquals(expectedURL, table.getSd().getParameters().get(AvroSerdeUtils.SCHEMA_URL));
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      client.dropTable(dbName, tblName);
      client.dropTable(dbName, badTblName);
      dropDatabase(dbName);
      super.tearDown();
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }
}
