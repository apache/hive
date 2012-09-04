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
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;

public class TestHiveMetaTool extends TestCase {

  private HiveMetaStoreClient client;

  private PrintStream originalOut;
  private OutputStream os;
  private PrintStream ps;
  private String locationUri;


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
      client = new HiveMetaStoreClient(hiveConf, null);

      // Setup output stream to redirect output to
      os = new ByteArrayOutputStream();
      ps = new PrintStream(os);

      // create a dummy database and a couple of dummy tables
      String dbName = "testDB";
      String typeName = "Person";
      String tblName = "simpleTbl";

      Database db = new Database();
      db.setName(dbName);
      client.dropTable(dbName, tblName);
      dropDatabase(dbName);
      client.createDatabase(db);
      locationUri = db.getLocationUri();

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
    String newLocationUri = "hdfs://nn-ha-uri/user/hive/warehouse";
    String[] args = new String[3];
    args[0] = new String("-updateLocation");
    args[1] = new String(newLocationUri);
    args[2] = new String(locationUri);

    String[] args2 = new String[1];
    args2[0] = new String("-listFSRoot");

    try {

      // perform HA upgrade
      HiveMetaTool.main(args);

      // obtain new HDFS root
      HiveMetaTool.main(args2);

      String out = os.toString();
      boolean b = out.contains(newLocationUri);

      if (b) {
        System.out.println("updateFSRootLocation successful");
      } else {
        System.out.println("updateFSRootLocation failed");
      }
      // restore the original HDFS root if needed
      if (b) {
        args[1] = new String(locationUri);
        args[2] = new String(newLocationUri);
        HiveMetaTool.main(args);
      }
    } finally {
      restoreOutputStream();
      System.out.println("Completed testUpdateFSRootLocation..");
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      super.tearDown();

    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }
}
