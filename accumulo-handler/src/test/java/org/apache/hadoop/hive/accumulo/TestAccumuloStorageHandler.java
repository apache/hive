/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.serde.AccumuloSerDeParameters;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 *
 */
public class TestAccumuloStorageHandler {

  private AccumuloStorageHandler storageHandler;
  private Configuration conf;

  @Rule
  public TestName test = new TestName();

  @Before
  public void setup() {
    conf = new Configuration();
    storageHandler = new AccumuloStorageHandler();
    storageHandler.setConf(conf);
  }

  @Test
  public void testTablePropertiesPassedToOutputJobProperties() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string:int:string");
    props.setProperty(serdeConstants.LIST_COLUMNS, "name,age,email");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY, "foo");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureOutputJobProperties(tableDesc, jobProperties);

    Assert.assertEquals(3, jobProperties.size());
    Assert.assertTrue("Job properties did not contain column mappings",
        jobProperties.containsKey(AccumuloSerDeParameters.COLUMN_MAPPINGS));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        jobProperties.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    Assert.assertTrue("Job properties did not contain accumulo table name",
        jobProperties.containsKey(AccumuloSerDeParameters.TABLE_NAME));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.TABLE_NAME),
        jobProperties.get(AccumuloSerDeParameters.TABLE_NAME));

    Assert.assertTrue("Job properties did not contain visibility label",
        jobProperties.containsKey(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY),
        jobProperties.get(AccumuloSerDeParameters.VISIBILITY_LABEL_KEY));
  }

  @Test
  public void testTablePropertiesPassedToInputJobProperties() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "true");
    props
        .setProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE, ColumnEncoding.BINARY.getName());
    props.setProperty(AccumuloSerDeParameters.AUTHORIZATIONS_KEY, "foo,bar");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureInputJobProperties(tableDesc, jobProperties);

    Assert.assertEquals(5, jobProperties.size());

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.COLUMN_MAPPINGS));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS),
        jobProperties.get(AccumuloSerDeParameters.COLUMN_MAPPINGS));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.TABLE_NAME));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.TABLE_NAME),
        jobProperties.get(AccumuloSerDeParameters.TABLE_NAME));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY),
        jobProperties.get(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE),
        jobProperties.get(AccumuloSerDeParameters.DEFAULT_STORAGE_TYPE));

    Assert.assertTrue(jobProperties.containsKey(AccumuloSerDeParameters.AUTHORIZATIONS_KEY));
    Assert.assertEquals(props.getProperty(AccumuloSerDeParameters.AUTHORIZATIONS_KEY),
        jobProperties.get(AccumuloSerDeParameters.AUTHORIZATIONS_KEY));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonBooleanIteratorPushdownValue() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "foo");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureInputJobProperties(tableDesc, jobProperties);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyIteratorPushdownValue() {
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Properties props = new Properties();
    Map<String,String> jobProperties = new HashMap<String,String>();

    props.setProperty(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq1,cf:cq2,cf:cq3");
    props.setProperty(AccumuloSerDeParameters.TABLE_NAME, "table");
    props.setProperty(AccumuloSerDeParameters.ITERATOR_PUSHDOWN_KEY, "");

    Mockito.when(tableDesc.getProperties()).thenReturn(props);

    storageHandler.configureInputJobProperties(tableDesc, jobProperties);
  }

  @Test
  public void testTableJobPropertiesCallsInputAndOutputMethods() {
    AccumuloStorageHandler mockStorageHandler = Mockito.mock(AccumuloStorageHandler.class);
    TableDesc tableDesc = Mockito.mock(TableDesc.class);
    Map<String,String> jobProperties = new HashMap<String,String>();

    Mockito.doCallRealMethod().when(mockStorageHandler)
        .configureTableJobProperties(tableDesc, jobProperties);

    // configureTableJobProperties shouldn't be getting called by Hive, but, if it somehow does,
    // we should just set all of the configurations for input and output.
    mockStorageHandler.configureTableJobProperties(tableDesc, jobProperties);

    Mockito.verify(mockStorageHandler).configureInputJobProperties(tableDesc, jobProperties);
    Mockito.verify(mockStorageHandler).configureOutputJobProperties(tableDesc, jobProperties);
  }

  @Test
  public void testPreCreateTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Define the SerDe Parameters
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);

    Assert.assertTrue("Table does not exist when we expect it to",
        conn.tableOperations().exists(tableName));
  }

  @Test(expected = MetaException.class)
  public void testMissingColumnMappingFails() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Empty parameters are sent, no COLUMN_MAPPING
    Map<String,String> params = new HashMap<String,String>();

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test(expected = MetaException.class)
  public void testNonNullLocation() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Empty parameters are sent, no COLUMN_MAPPING
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn("foobar");

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test
  public void testExternalNonExistentTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Define the SerDe Parameters
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is not marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(false);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test
  public void testExternalExistentTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    String tableName = "table";

    // Create the table
    conn.tableOperations().create(tableName);

    // Define the SerDe Parameters
    Map<String,String> params = new HashMap<String,String>();
    params.put(AccumuloSerDeParameters.COLUMN_MAPPINGS, "cf:cq");

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    StorageDescriptor sd = Mockito.mock(StorageDescriptor.class);
    Table table = Mockito.mock(Table.class);
    SerDeInfo serDeInfo = Mockito.mock(SerDeInfo.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).preCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the mocked StorageDescriptor
    Mockito.when(table.getSd()).thenReturn(sd);

    // No location expected with AccumuloStorageHandler
    Mockito.when(sd.getLocation()).thenReturn(null);

    // Return mocked SerDeInfo
    Mockito.when(sd.getSerdeInfo()).thenReturn(serDeInfo);

    // Custom parameters
    Mockito.when(serDeInfo.getParameters()).thenReturn(params);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.preCreateTable(table);
  }

  @Test()
  public void testRollbackCreateTableOnNonExistentTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    String tableName = "table";

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    Table table = Mockito.mock(Table.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).rollbackCreateTable(table);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.rollbackCreateTable(table);
  }

  @Test()
  public void testRollbackCreateTableDeletesExistentTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    String tableName = "table";

    // Create the table
    conn.tableOperations().create(tableName);

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    Table table = Mockito.mock(Table.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).rollbackCreateTable(table);
    Mockito.doCallRealMethod().when(storageHandler).commitDropTable(table, true);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.rollbackCreateTable(table);

    Assert.assertFalse(conn.tableOperations().exists(tableName));
  }

  @Test()
  public void testRollbackCreateTableDoesntDeleteExternalExistentTable() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    String tableName = "table";

    // Create the table
    conn.tableOperations().create(tableName);

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    Table table = Mockito.mock(Table.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).rollbackCreateTable(table);
    Mockito.doCallRealMethod().when(storageHandler).commitDropTable(table, true);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is not marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(false);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.rollbackCreateTable(table);

    Assert.assertTrue(conn.tableOperations().exists(tableName));
  }

  @Test
  public void testDropTableWithoutDeleteLeavesTableIntact() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));
    AccumuloStorageHandler storageHandler = Mockito.mock(AccumuloStorageHandler.class);
    String tableName = "table";

    // Create the table
    conn.tableOperations().create(tableName);

    AccumuloConnectionParameters connectionParams = Mockito
        .mock(AccumuloConnectionParameters.class);
    Table table = Mockito.mock(Table.class);

    // Call the real preCreateTable method
    Mockito.doCallRealMethod().when(storageHandler).commitDropTable(table, false);

    // Return our known table name
    Mockito.when(storageHandler.getTableName(table)).thenReturn(tableName);

    // Is marked for purge
    Mockito.when(storageHandler.isPurge(table)).thenReturn(true);

    // Return the MockInstance's Connector
    Mockito.when(connectionParams.getConnector()).thenReturn(conn);

    storageHandler.connectionParams = connectionParams;

    storageHandler.rollbackCreateTable(table);

    Assert.assertTrue(conn.tableOperations().exists(tableName));
  }
}
