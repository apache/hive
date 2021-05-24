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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

/** Integration tests for the HiveMetaTool program. */
public class TestHiveMetaTool extends TestCase {
  private static final String DB_NAME = "TestHiveMetaToolDB";
  private static final String TABLE_NAME = "simpleTbl";
  private static final String LOCATION = "hdfs://nn.example.com/";
  private static final String NEW_LOCATION = "hdfs://nn-ha-uri/";
  private static final String PATH = "warehouse/hive/ab.avsc";
  private static final String AVRO_URI = LOCATION + PATH;
  private static final String NEW_AVRO_URI = NEW_LOCATION + PATH;

  private HiveMetaStoreClient client;
  private OutputStream os;

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    try {
      os = new ByteArrayOutputStream();
      System.setOut(new PrintStream(os));

      HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
      client = new HiveMetaStoreClient(hiveConf);

      createDatabase();
      createTable();

      client.close();
    } catch (Exception e) {
      System.err.println("Unable to setup the hive metatool test");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private void createDatabase() throws Exception {
    if (client.getAllDatabases().contains(DB_NAME)) {
      client.dropDatabase(DB_NAME);
    }

    Database db = new Database();
    db.setName(DB_NAME);
    client.createDatabase(db);
  }

  private void createTable() throws Exception {
    Table tbl = new Table();
    tbl.setDbName(DB_NAME);
    tbl.setTableName(TABLE_NAME);

    Map<String, String> parameters = new HashMap<>();
    parameters.put(AvroTableProperties.SCHEMA_URL.getPropName(), AVRO_URI);
    tbl.setParameters(parameters);

    List<FieldSchema> fields = new ArrayList<FieldSchema>(2);
    fields.add(new FieldSchema("name", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("income", serdeConstants.INT_TYPE_NAME, ""));

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(fields);
    sd.setParameters(new HashMap<String, String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getParameters().put(AvroTableProperties.SCHEMA_URL.getPropName(), AVRO_URI);
    tbl.setSd(sd);

    client.createTable(tbl);
  }

  public void testListFSRoot() throws Exception {
    HiveMetaTool.main(new String[] {"-listFSRoot"});
    String out = os.toString();
    assertTrue(out + " doesn't contain " + client.getDatabase(DB_NAME).getLocationUri(),
        out.contains(client.getDatabase(DB_NAME).getLocationUri()));
  }

  public void testExecuteJDOQL() throws Exception {
    HiveMetaTool.main(
        new String[] {"-executeJDOQL", "select locationUri from org.apache.hadoop.hive.metastore.model.MDatabase"});
    String out = os.toString();
    assertTrue(out + " doesn't contain " + client.getDatabase(DB_NAME).getLocationUri(),
        out.contains(client.getDatabase(DB_NAME).getLocationUri()));
  }

  public void testUpdateFSRootLocation() throws Exception {
    checkAvroSchemaURLProps(AVRO_URI);

    HiveMetaTool.main(new String[] {"-updateLocation", NEW_LOCATION, LOCATION, "-tablePropKey", "avro.schema.url"});
    checkAvroSchemaURLProps(NEW_AVRO_URI);

    HiveMetaTool.main(new String[] {"-updateLocation", LOCATION, NEW_LOCATION, "-tablePropKey", "avro.schema.url"});
    checkAvroSchemaURLProps(AVRO_URI);
  }

  private void checkAvroSchemaURLProps(String expectedUri) throws TException {
    Table table = client.getTable(DB_NAME, TABLE_NAME);
    assertEquals(expectedUri, table.getParameters().get(AvroTableProperties.SCHEMA_URL.getPropName()));
    assertEquals(expectedUri, table.getSd().getParameters().get(AvroTableProperties.SCHEMA_URL.getPropName()));
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      client.dropTable(DB_NAME, TABLE_NAME);
      client.dropDatabase(DB_NAME);
      super.tearDown();
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }
}
