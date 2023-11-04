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

package org.apache.hadoop.hive.metastore.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.client.builder.CatalogBuilder;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * API tests for HMS client's listPartitions methods.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestListPartitions extends MetaStoreClientTest {
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  protected static final String DB_NAME = "testpartdb";
  protected static final String TABLE_NAME = "testparttable";
  protected static final String USER_NAME = "user0";
  protected static final String GROUP = "group0";

  public static class ReturnTable {
    public Table table;
    public List<List<String>> testValues;

    public ReturnTable(Table table, List<List<String>> testValues) {
      this.table = table;
      this.testValues = testValues;
    }
  }

  public TestListPartitions(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = metaStore.getClient();

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    createDB(DB_NAME);

  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
      }
    } finally {
      client = null;
    }
  }

  protected AbstractMetaStoreService getMetaStore() {
    return metaStore;
  }

  protected void setMetaStore(AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  protected IMetaStoreClient getClient() {
    return client;
  }

  protected void setClient(IMetaStoreClient client) {
    this.client = client;
  }

  private void createDB(String dbName) throws TException {
    new DatabaseBuilder().
            setName(dbName).
            create(client, metaStore.getConf());
  }

  private Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols) throws Exception {

    return createTestTable(client, dbName, tableName, partCols, false);
  }


  protected Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols, boolean setPartitionLevelPrivilages)
          throws TException {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build(metaStore.getConf());

    if (setPartitionLevelPrivilages) {
      table.putToParameters("PARTITION_LEVEL_PRIVILEGE", "true");
    }

    client.createTable(table);
    return table;
  }

  protected void addPartition(IMetaStoreClient client, Table table, List<String> values)
          throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    client.add_partition(partitionBuilder.build(metaStore.getConf()));
  }

  protected void addPartitions(IMetaStoreClient client, List<Partition> partitions)
      throws TException{
    client.add_partitions(partitions);
  }

  private Table createTable3PartCols1PartGeneric(IMetaStoreClient client, boolean authOn)
          throws TException {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm",
            "dd"), authOn);
    addPartition(client, t, Lists.newArrayList("1997", "05", "16"));

    return t;
  }

  protected Table createTable3PartCols1Part(IMetaStoreClient client) throws TException {
    return createTable3PartCols1PartGeneric(client, false);
  }

  private ReturnTable createTable4PartColsPartsGeneric(IMetaStoreClient client,
                                                                     boolean authOn) throws
          Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"),
            authOn);
    List<List<String>> testValues = Lists.newArrayList(
            Lists.newArrayList("1999", "01", "02"),
            Lists.newArrayList("2009", "02", "10"),
            Lists.newArrayList("2017", "10", "26"),
            Lists.newArrayList("2017", "11", "27"));

    for(List<String> vals : testValues) {
      addPartition(client, t, vals);
    }

    return new ReturnTable(t, testValues);
  }

  protected ReturnTable createTable4PartColsParts(IMetaStoreClient client) throws
          Exception {
    return createTable4PartColsPartsGeneric(client, false);
  }

  protected ReturnTable createTable4PartColsPartsAuthOn(IMetaStoreClient client) throws
          Exception {
    return createTable4PartColsPartsGeneric(client, true);
  }

  protected void assertAuthInfoReturned(String user, String group, Partition partition) {
    assertNotNull(partition.getPrivileges());
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getUserPrivileges().get(user));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getGroupPrivileges().get(group));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getRolePrivileges().get("public"));
  }

  private void assertPartitionsHaveCorrectValues(List<Partition> partitions,
                                               List<List<String>> testValues) throws Exception {
    assertEquals(testValues.size(), partitions.size());
    partitions.forEach(p -> {});

    for (int i = 0; i < partitions.size(); ++i) {
      assertEquals(testValues.get(i), partitions.get(i).getValues());
    }
  }

  protected void assertCorrectPartitionNames(List<String> names,
                                                  List<List<String>> testValues,
                                                  List<String>partCols) throws Exception {
    assertEquals(testValues.size(), names.size());
    for (int i = 0; i < names.size(); ++i) {
      List<String> expectedKVPairs = new ArrayList<>();
      for (int j = 0; j < partCols.size(); ++j) {
        expectedKVPairs.add(partCols.get(j) + "=" + testValues.get(i).get(j));
      }
      assertEquals(expectedKVPairs.stream().collect(joining("/")), names.get(i));
    }
  }

  protected void assertPartitionsSpecProxy(PartitionSpecProxy partSpecProxy,
                                                List<List<String>> testValues) throws Exception {
    assertEquals(testValues.size(), partSpecProxy.size());
    List<PartitionSpec> partitionSpecs = partSpecProxy.toPartitionSpec();
    List<Partition> partitions = partitionSpecs.get(0).getPartitionList().getPartitions();
    assertEquals(testValues.size(), partitions.size());

    for (int i = 0; i < partitions.size(); ++i) {
      assertEquals(testValues.get(i), partitions.get(i).getValues());
    }
  }

  private void assertCorrectPartitionValuesResponse(List<List<String>> testValues,
                                         PartitionValuesResponse resp) throws Exception {
    assertEquals(testValues.size(), resp.getPartitionValuesSize());
    List<PartitionValuesRow> rowList = resp.getPartitionValues();
    for (int i = 0; i < rowList.size(); ++i) {
      PartitionValuesRow pvr = rowList.get(i);
      List<String> values = pvr.getRow();
      for (int j = 0; j < values.size(); ++j) {
        assertEquals(testValues.get(i).get(j), values.get(j));
      }
    }
  }

  protected void assertPartitionsHaveCorrectParams(List<Partition> partitions) {

  }

  /**
   * Testing listPartitions(String,String,short) ->
   *         get_partitions(String,String,short).
   */
  @Test
  public void testListPartitionsAll() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    assertPartitionsHaveCorrectValues(partitions, testValues);
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(0, 1));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short) 0);
    assertTrue(partitions.isEmpty());
  }

  /**
   * Testing getPartitionsRequest(PartitionsRequest) ->
   *         get_partitions_req(PartitionsRequest).
   */
  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testGetPartitionsRequest() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    PartitionsRequest req = new PartitionsRequest();
    req.setCatName(MetaStoreUtils.getDefaultCatalog(metaStore.getConf()));
    req.setDbName(DB_NAME);
    req.setTblName(TABLE_NAME);
    req.setMaxParts((short)-1);
    PartitionsResponse res = client.getPartitionsRequest(req);
    assertPartitionsHaveCorrectValues(res.getPartitions(), testValues);
    assertPartitionsHaveCorrectParams(res.getPartitions());

    req.setMaxParts((short)1);
    res = client.getPartitionsRequest(req);
    assertPartitionsHaveCorrectValues(res.getPartitions(), testValues.subList(0, 1));
    assertPartitionsHaveCorrectParams(res.getPartitions());

    req.setMaxParts((short)0);
    res = client.getPartitionsRequest(req);
    assertTrue(res.getPartitions().isEmpty());
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionsAllHighMaxParts() throws Exception {
    createTable3PartCols1Part(client);
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)101);
    assertTrue(partitions.isEmpty());
  }

  @Test
  public void testListPartitionsAllNoParts() throws Exception {
    createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"));
    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
    assertTrue(partitions.isEmpty());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoTable() throws Exception {
    client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitions(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions("", TABLE_NAME, (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsAllNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, "", (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsAllNullTblName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.listPartitions(DB_NAME, (String)null, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsAllNullDbName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.listPartitions(null, TABLE_NAME, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }



  /**
   * Testing listPartitions(String,String,List(String),short) ->
   *         get_partitions(String,String,List(String),short).
   */
  @Test
  public void testListPartitionsByValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;

    List<Partition> partitions = client.listPartitions(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)-1);
    assertEquals(2, partitions.size());
    assertEquals(testValues.get(2), partitions.get(0).getValues());
    assertEquals(testValues.get(3), partitions.get(1).getValues());
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitions(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017", "11"), (short)-1);
    assertEquals(1, partitions.size());
    assertEquals(testValues.get(3), partitions.get(0).getValues());
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitions(DB_NAME, TABLE_NAME,
            Lists.newArrayList("20177", "11"), (short)-1);
    assertEquals(0, partitions.size());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNoVals() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList(), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesTooManyVals() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList("0", "1", "2", "3"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions("", TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, "", Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoTable() throws Exception {
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitions(DB_NAME, TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNullDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(null, TABLE_NAME, Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNullTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, null, Lists.newArrayList("1999"), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByValuesNullValues() throws Exception {
    createTable3PartCols1Part(client);
    client.listPartitions(DB_NAME, TABLE_NAME, (List<String>)null, (short)-1);
  }



  /**
   * Testing listPartitionSpecs(String,String,int) ->
   *         get_partitions_pspec(String,String,int).
   */
  @Test
  public void testListPartitionSpecs() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;

    PartitionSpecProxy partSpecProxy = client.listPartitionSpecs(DB_NAME, TABLE_NAME, -1);
    assertPartitionsSpecProxy(partSpecProxy, testValues);

    partSpecProxy = client.listPartitionSpecs(DB_NAME, TABLE_NAME, 2);
    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(0, 2));

    partSpecProxy = client.listPartitionSpecs(DB_NAME, TABLE_NAME, 0);
    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(0, 0));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoTable() throws Exception {
    client.listPartitionSpecs(DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionSpecs(DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionSpecsHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecs(DB_NAME, TABLE_NAME, 101);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecs("", TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecs(DB_NAME, "", -1);
  }

  @Test
  public void testListPartitionSpecsNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionSpecs(null, TABLE_NAME,  -1);
      fail("Should have thrown exception");
    } catch (MetaException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionSpecsNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionSpecs(DB_NAME, null, -1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException | TApplicationException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }



  /**
   * Testing listPartitionsWithAuthInfo(String,String,short,String,List(String)) ->
   *         get_partitions_with_auth(String,String,short,String,List(String)).
   */
  @Test
  public void testListPartitionsWithAuth() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client).testValues;
    List<String> groups = Lists.newArrayList(GROUP);
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1,
            USER_NAME, groups);

    assertEquals(4, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues);
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)2, USER_NAME, groups);
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(0, 2));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionsWithAuthHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)101, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthLowMaxParts() throws Exception {
    createTable4PartColsPartsAuthOn(getClient());
    List<Partition> partitions =
        getClient().listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short) 2, USER_NAME, Lists.newArrayList(GROUP));
    assertTrue(partitions.size() == 2);
    partitions.forEach(p -> assertAuthInfoReturned(USER_NAME, GROUP, p));
    assertPartitionsHaveCorrectParams(partitions);
    partitions = getClient().listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short) -1, USER_NAME,
        Lists.newArrayList(GROUP));
    assertTrue(partitions.size() == 4);
    partitions.forEach(p -> assertAuthInfoReturned(USER_NAME, GROUP, p));
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test
  public void testListPartitionsWithAuthNoPrivilegesSet() throws Exception {
    List<List<String>> partValues = createTable4PartColsParts(client).testValues;
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1,
            "", Lists.newArrayList());

    assertEquals(4, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues);
    partitions.forEach(partition -> assertNull(partition.getPrivileges()));
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo("", TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, "", (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionsWithAuthNoTable() throws Exception {
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }


  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthNullDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(null, TABLE_NAME, (short)-1, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsWithAuthInfo(DB_NAME, (String)null, (short)-1, "",
          Lists.newArrayList());
      fail("Should have thrown exception");
    } catch (MetaException| TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionsWithAuthNullUser() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    assertPartitionsHaveCorrectParams(client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1,
        null, Lists.newArrayList()));
  }

  @Test
  public void testListPartitionsWithAuthNullGroup() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    assertPartitionsHaveCorrectParams(client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (short)-1,
        "user0", null));
  }

  /**
   * Testing listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest) ->
   *         get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest).
   */
  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionsWithAuthRequestByValues() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client).testValues;
    List<String> groups = Lists.newArrayList(GROUP);

    GetPartitionsPsWithAuthRequest req = new GetPartitionsPsWithAuthRequest();
    req.setCatName(MetaStoreUtils.getDefaultCatalog(metaStore.getConf()));
    req.setDbName(DB_NAME);
    req.setTblName(TABLE_NAME);
    req.setPartVals(Lists
        .newArrayList("2017", "11", "27"));
    req.setMaxParts((short)-1);
    req.setUserName(USER_NAME);
    req.setGroupNames(groups);
    GetPartitionsPsWithAuthResponse res = client.listPartitionsWithAuthInfoRequest(req);
    List<Partition> partitions = res.getPartitions();
    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    req.setPartVals(Lists
        .newArrayList("2017"));
    res = client.listPartitionsWithAuthInfoRequest(req);
    partitions = res.getPartitions();
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    req.setMaxParts((short)1);
    res = client.listPartitionsWithAuthInfoRequest(req);
    partitions = res.getPartitions();
    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 3));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    req.setMaxParts((short)-1);
    req.setPartVals(Lists
        .newArrayList("2013"));
    res = client.listPartitionsWithAuthInfoRequest(req);
    partitions = res.getPartitions();
    assertTrue(partitions.isEmpty());
  }

  /**
   * Testing listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest) ->
   *         get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest).
   */
  @Test
  public void testListPartitionsWithAuthByValues() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client).testValues;
    List<String> groups = Lists.newArrayList(GROUP);

    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
        .newArrayList("2017", "11", "27"), (short)-1, USER_NAME, groups);
    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
        .newArrayList("2017"), (short)-1, USER_NAME, groups);
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
        .newArrayList("2017"), (short)1, USER_NAME, groups);
    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(2, 3));
    partitions.forEach(partition -> assertAuthInfoReturned(USER_NAME, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
        .newArrayList("2013"), (short)-1, USER_NAME, groups);
    assertTrue(partitions.isEmpty());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesNoVals() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList(), (short)-1, "", Lists.newArrayList());
  }


  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesTooManyVals() throws Exception {
    createTable4PartColsPartsAuthOn(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("0", "1", "2", "3"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionsWithAuthByValuesHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017"), (short) 101, "", Lists.newArrayList());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesTooManyValsHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("0", "1", "2", "3"), (short)101, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthByValuesNoPrivilegesSet() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client).testValues;
    String user = "user0";
    List<String> groups = Lists.newArrayList("group0");
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, user, groups);

    assertEquals(1, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    partitions.forEach(partition -> assertAuthInfoReturned(user, groups.get(0), partition));
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo("", TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, "", Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoTable() throws Exception {
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsWithAuthByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsWithAuthInfo(null, TABLE_NAME, Lists
              .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsWithAuthInfo(DB_NAME, null, Lists
              .newArrayList("2017", "11", "27"), (short)-1, "", Lists.newArrayList());
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsWithAuthByValuesNullValues() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, (List<String>)null,
            (short)-1, "", Lists.newArrayList());
  }

  @Test
  public void testListPartitionsWithAuthByValuesNullUser() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client).testValues;
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, null, Lists.newArrayList());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test
  public void testListPartitionsWithAuthByValuesNullGroup() throws Exception {
    List<List<String>> partValues = createTable4PartColsPartsAuthOn(client).testValues;
    List<Partition> partitions = client.listPartitionsWithAuthInfo(DB_NAME, TABLE_NAME, Lists
            .newArrayList("2017", "11", "27"), (short)-1, "", null);
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(3, 4));
    assertPartitionsHaveCorrectParams(partitions);
  }



  /**
   * Testing listPartitionsByFilter(String,String,String,short) ->
   *         get_partitions_by_filter(String,String,String,short).
   */

  @Test
  public void testListPartitionsByFilter() throws Exception {
    List<List<String>> partValues = createTable4PartColsParts(client).testValues;
    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", (short)-1);
    assertEquals(3, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(1, 4));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", (short)2);
    assertEquals(2, partitions.size());
    assertPartitionsHaveCorrectValues(partitions, partValues.subList(1, 3));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", (short)0);
    assertTrue(partitions.isEmpty());
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" AND mm=\"99\"", (short)-1);
    assertTrue(partitions.isEmpty());
  }

  @Test
  public void testListPartitionsByFilterCaseInsensitive() throws Exception {
    String tableName = TABLE_NAME + "_caseinsensitive";
    Table t = createTestTable(client, DB_NAME, tableName,
        Lists.newArrayList("yyyy", "month", "day"), false);
    List<List<String>> testValues = Lists.newArrayList(
        Lists.newArrayList("2017", "march", "11"),
        Lists.newArrayList("2017", "march", "15"),
        Lists.newArrayList("2017", "may", "15"),
        Lists.newArrayList("2018", "march", "11"),
        Lists.newArrayList("2018", "september", "7"));

    for(List<String> vals : testValues) {
      addPartition(client, t, vals);
    }

    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "yYyY=\"2017\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(0, 3));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "yYyY=\"2017\" AND mOnTh=\"may\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(2, 3));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "yYyY!=\"2017\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(3, 5));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "mOnTh=\"september\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(4, 5));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "mOnTh like \"m%\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(0, 4));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "yYyY=\"2018\" AND mOnTh like \"m%\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(3, 4));
    assertPartitionsHaveCorrectParams(partitions);
    client.dropTable(DB_NAME, tableName);
  }

  @Test
  public void testListPartitionsByFilterCaseSensitive() throws Exception {
    String tableName = TABLE_NAME + "_casesensitive";
    Table t = createTestTable(client, DB_NAME, tableName,
        Lists.newArrayList("yyyy", "month", "day"), false);
    List<List<String>> testValues = Lists.newArrayList(
        Lists.newArrayList("2017", "march", "11"),
        Lists.newArrayList("2017", "march", "15"),
        Lists.newArrayList("2017", "may", "15"),
        Lists.newArrayList("2018", "march", "11"),
        Lists.newArrayList("2018", "april", "7"));

    for(List<String> vals : testValues) {
      addPartition(client, t, vals);
    }

    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "month=\"mArCh\"", (short) -1);
    Assert.assertTrue(partitions.isEmpty());
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "yyyy=\"2017\" AND month=\"May\"", (short) -1);
    Assert.assertTrue(partitions.isEmpty());

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "yyyy=\"2017\" AND month!=\"mArCh\"", (short) -1);
    assertPartitionsHaveCorrectValues(partitions, testValues.subList(0, 3));
    assertPartitionsHaveCorrectParams(partitions);

    partitions = client.listPartitionsByFilter(DB_NAME, tableName,
        "month like \"M%\"", (short) -1);
    Assert.assertTrue(partitions.isEmpty());
    client.dropTable(DB_NAME, tableName);
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByFilterInvalidFilter() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyy=\"2017\"", (short)101);
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionsByFilterHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", (short)101);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter(DB_NAME, "", "yyyy=\"2017\"", (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionsByFilter("", TABLE_NAME, "yyyy=\"2017\"", (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoTable() throws Exception {
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByFilterNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByFilterNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsByFilter(DB_NAME, null, "yyyy=\"2017\"", (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByFilterNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionsByFilter(null, TABLE_NAME, "yyyy=\"2017\"", (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionsByFilterNullFilter() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME, null,
            (short)-1);
    assertEquals(4, partitions.size());
    assertPartitionsHaveCorrectParams(partitions);
  }

  @Test
  public void testListPartitionsByFilterEmptyFilter() throws Exception {
    createTable4PartColsParts(client);
    List<Partition> partitions = client.listPartitionsByFilter(DB_NAME, TABLE_NAME, "", (short)-1);
    assertEquals(4, partitions.size());
    assertPartitionsHaveCorrectParams(partitions);
  }



  /**
   * Testing listPartitionSpecsByFilter(String,String,String,int) ->
   *         get_part_specs_by_filter(String,String,String,int).
   */
  @Test
  public void testListPartitionsSpecsByFilter() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    PartitionSpecProxy partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", -1);

    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(1, 4));

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", 2);
    assertPartitionsSpecProxy(partSpecProxy, testValues.subList(1, 3));

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" OR " + "mm=\"02\"", 0);
    assertPartitionsSpecProxy(partSpecProxy, Lists.newArrayList());

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"20177\"", -1);
    assertPartitionsSpecProxy(partSpecProxy, Lists.newArrayList());

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
          "yYyY=\"2017\"", -1);
      assertPartitionsSpecProxy(partSpecProxy, testValues.subList(2, 4));
    }

    partSpecProxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME,
            "yyyy=\"2017\" AND mm=\"99\"", -1);
    assertPartitionsSpecProxy(partSpecProxy, Lists.newArrayList());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterInvalidFilter() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyy=\"2017\"", 101);
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionSpecsByFilterHighMaxParts() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", 101);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, "", "yyyy=\"2017\"", -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter("", TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoTable() throws Exception {
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionSpecsByFilterNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterNullTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(DB_NAME, null, "yyyy=\"2017\"", -1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionSpecsByFilterNullDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionSpecsByFilter(null, TABLE_NAME, "yyyy=\"2017\"", -1);
  }

  @Test
  public void testListPartitionSpecsByFilterNullFilter() throws Exception {
    List<List<String>> values = createTable4PartColsParts(client).testValues;
    PartitionSpecProxy pproxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, null, -1);
    assertPartitionsSpecProxy(pproxy, values);
  }

  @Test
  public void testListPartitionSpecsByFilterEmptyFilter() throws Exception {
    List<List<String>> values = createTable4PartColsParts(client).testValues;
    PartitionSpecProxy pproxy = client.listPartitionSpecsByFilter(DB_NAME, TABLE_NAME, "", -1);
    assertPartitionsSpecProxy(pproxy, values);
  }



  /**
   * Testing getNumPartitionsByFilter(String,String,String) ->
   *         get_num_partitions_by_filter(String,String,String).
   */
  @Test
  public void testGetNumPartitionsByFilter() throws Exception {
    createTable4PartColsParts(client);
    int n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\" OR " +
            "mm=\"02\"");
    assertEquals(3, n);

    n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "");
    assertEquals(4, n);

    n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"20177\"");
    assertEquals(0, n);

    // HIVE-18977
    if (MetastoreConf.getBoolVar(metaStore.getConf(), MetastoreConf.ConfVars.TRY_DIRECT_SQL)) {
      n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yYyY=\"2017\"");
      assertEquals(2, n);
    }

    n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\" AND mm=\"99\"");
    assertEquals(0, n);

  }

  @Test(expected = MetaException.class)
  public void testGetNumPartitionsByFilterInvalidFilter() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyy=\"2017\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(DB_NAME, "", "yyyy=\"2017\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter("", TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoTable() throws Exception {
    client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetNumPartitionsByFilterNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test(expected = MetaException.class)
  public void testGetNumPartitionsByFilterNullTblName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(DB_NAME, null, "yyyy=\"2017\"");
  }

  @Test(expected = MetaException.class)
  public void testGetNumPartitionsByFilterNullDbName() throws Exception {
    createTable4PartColsParts(client);
    client.getNumPartitionsByFilter(null, TABLE_NAME, "yyyy=\"2017\"");
  }

  @Test
  public void testGetNumPartitionsByFilterNullFilter() throws Exception {
    createTable4PartColsParts(client);
    int n = client.getNumPartitionsByFilter(DB_NAME, TABLE_NAME, null);
    assertEquals(4, n);
  }



  /**
   * Testing listPartitionNames(String,String,short) ->
   *         get_partition_names(String,String,short).
   */
  @Test
  public void testListPartitionNames() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)-1);
    assertCorrectPartitionNames(partitionNames, testValues, Lists.newArrayList("yyyy", "mm",
            "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)2);
    assertCorrectPartitionNames(partitionNames, testValues.subList(0, 2),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)0);
    assertTrue(partitionNames.isEmpty());

    //This method does not depend on MetastoreConf.LIMIT_PARTITION_REQUEST setting:
    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)101);
    assertCorrectPartitionNames(partitionNames, testValues, Lists.newArrayList("yyyy", "mm",
            "dd"));

  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    try {
      client.listPartitionNames("", TABLE_NAME, (short) -1);
    } catch (NoSuchObjectException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
      throw new MetaException(e.getMessage());
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    try {
      client.listPartitionNames(DB_NAME, "", (short) -1);
    } catch (NoSuchObjectException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
      throw new MetaException(e.getMessage());
    }
  }

  @Test
  public void testListPartitionNamesNoTable() throws Exception {
    List<String> names = client.listPartitionNames(DB_NAME, TABLE_NAME, (short)-1);
    Assert.assertEquals(0, names.size());
  }

  @Test
  public void testListPartitionNamesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionNames(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(null, TABLE_NAME, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(DB_NAME, (String)null, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }



  /**
   * Testing listPartitionNames(String,String,List(String),short) ->
   *         get_partition_names_ps(String,String,List(String),short).
   */
  @Test
  public void testListPartitionNamesByValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)-1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)101);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
            Lists.newArrayList("yyyy", "mm", "dd"));

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short)0);
    assertTrue(partitionNames.isEmpty());

    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017", "10"), (short)-1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
            Lists.newArrayList("yyyy", "mm", "dd"));

  }

  /**
   * Testing listPartitionNamesRequest(GetPartitionNamesPsRequest) ->
   *         get_partition_names_ps_req(GetPartitionNamesPsRequest).
   */
  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionNamesRequestByValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    GetPartitionNamesPsRequest req = new GetPartitionNamesPsRequest();
    req.setCatName(MetaStoreUtils.getDefaultCatalog(metaStore.getConf()));
    req.setDbName(DB_NAME);
    req.setTblName(TABLE_NAME);
    req.setPartValues(Lists.newArrayList("2017"));
    req.setMaxParts((short)-1);
    GetPartitionNamesPsResponse res = client.listPartitionNamesRequest(req);
    List<String> partitionNames = res.getNames();
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
        Lists.newArrayList("yyyy", "mm", "dd"));

    req.setMaxParts((short)101);
    res = client.listPartitionNamesRequest(req);
    partitionNames = res.getNames();
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
        Lists.newArrayList("yyyy", "mm", "dd"));

    req.setMaxParts((short)1);
    res = client.listPartitionNamesRequest(req);
    partitionNames = res.getNames();
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
        Lists.newArrayList("yyyy", "mm", "dd"));

    req.setMaxParts((short)0);
    res = client.listPartitionNamesRequest(req);
    partitionNames = res.getNames();
    assertTrue(partitionNames.isEmpty());

    req.setMaxParts((short)-1);
    req.setPartValues(Lists.newArrayList("2017", "10"));
    res = client.listPartitionNamesRequest(req);
    partitionNames = res.getNames();
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
        Lists.newArrayList("yyyy", "mm", "dd"));

  }

  @Test
  public void testListPartitionNamesByValuesMaxPartCountUnlimited() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    //TODO: due to value 101 this probably should throw an exception
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("2017"), (short) 101);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
            Lists.newArrayList("yyyy", "mm", "dd"));
  }

  @Test
  public void testListPartitionNamesByValuesLowPartCount() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    List<String> partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
        Lists.newArrayList("2017"), (short) 1);
    assertTrue(partitionNames.size() == 1);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 3),
        Lists.newArrayList("yyyy", "mm", "dd"));
    partitionNames = client.listPartitionNames(DB_NAME, TABLE_NAME,
        Lists.newArrayList("2017"), (short) -1);
    assertTrue(partitionNames.size() == 2);
    assertCorrectPartitionNames(partitionNames, testValues.subList(2, 4),
        Lists.newArrayList("yyyy", "mm", "dd"));
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesNoPartVals() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList(), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesTooManyVals() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList("1", "2", "3", "4"),
            (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames("", TABLE_NAME, Lists.newArrayList("2017"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, "", Lists.newArrayList("2017"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoTable() throws Exception {
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList("2017"), (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionNamesByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionNames(DB_NAME, TABLE_NAME, Lists.newArrayList("2017"), (short)-1);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(null, TABLE_NAME, Lists.newArrayList("2017"), (short) -1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionNames(DB_NAME, null, Lists.newArrayList("2017"), (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testListPartitionNamesByValuesNullValues() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionNames(DB_NAME, TABLE_NAME, (List<String>)null, (short)-1);
  }



  /**
   * Testing listPartitionValues(PartitionValuesRequest) ->
   *         get_partition_values(PartitionValuesRequest).
   */
  @Test
  public void testListPartitionValues() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client).testValues;
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
            partitionSchema);
    PartitionValuesResponse response = client.listPartitionValues(request);
    assertCorrectPartitionValuesResponse(testValues, response);

  }

  @Test
  public void testListPartitionValuesEmptySchema() throws Exception {
    try {
      List<List<String>> testValues = createTable4PartColsParts(client).testValues;
      List<FieldSchema> partitionSchema = Lists.newArrayList();

      PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
              partitionSchema);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (IndexOutOfBoundsException | TException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionValuesNoDbName() throws Exception {
    createTable4PartColsParts(client);
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest("", TABLE_NAME,
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionValuesNoTblName() throws Exception {
    createTable4PartColsParts(client);
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, "",
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionValuesNoTable() throws Exception {
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionValuesNoDb() throws Exception {
    List<FieldSchema> partitionSchema = Lists.newArrayList(
            new FieldSchema("yyyy", "string", ""),
            new FieldSchema("mm", "string", ""));

    PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
            partitionSchema);
    client.listPartitionValues(request);
  }

  @Test
  public void testListPartitionValuesNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      List<FieldSchema> partitionSchema = Lists.newArrayList(
              new FieldSchema("yyyy", "string", ""),
              new FieldSchema("mm", "string", ""));

      PartitionValuesRequest request = new PartitionValuesRequest(null, TABLE_NAME,
              partitionSchema);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (NullPointerException | TProtocolException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void testListPartitionValuesNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      List<FieldSchema> partitionSchema = Lists.newArrayList(
              new FieldSchema("yyyy", "string", ""),
              new FieldSchema("mm", "string", ""));

      PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, null,
              partitionSchema);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (NullPointerException | TProtocolException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionValuesNullSchema() throws Exception {
    try {
      createTable4PartColsParts(client);
      PartitionValuesRequest request = new PartitionValuesRequest(DB_NAME, TABLE_NAME,
              null);
      client.listPartitionValues(request);
      fail("Should have thrown exception");
    } catch (NullPointerException | TProtocolException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testListPartitionValuesNullRequest() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionValues(null);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void otherCatalog() throws TException {
    String catName = "list_partition_catalog";
    Catalog cat = new CatalogBuilder()
        .setName(catName)
        .setLocation(MetaStoreTestUtils.getTestWarehouseDir(catName))
        .build();
    client.createCatalog(cat);

    String dbName = "list_partition_database_in_other_catalog";
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setCatalogName(catName)
        .create(client, metaStore.getConf());

    String tableName = "table_in_other_catalog";
    Table table = new TableBuilder()
        .inDb(db)
        .setTableName(tableName)
        .addCol("id", "int")
        .addCol("name", "string")
        .addPartCol("partcol", "string")
        .create(client, metaStore.getConf());

    Partition[] parts = new Partition[5];
    for (int i = 0; i < parts.length; i++) {
      parts[i] = new PartitionBuilder()
          .inTable(table)
          .addValue("a" + i)
          .build(metaStore.getConf());
    }
    addPartitions(client, Arrays.asList(parts));

    List<Partition> fetched = client.listPartitions(catName, dbName, tableName, -1);
    Assert.assertEquals(parts.length, fetched.size());
    Assert.assertEquals(catName, fetched.get(0).getCatName());
    assertPartitionsHaveCorrectParams(fetched);

    fetched = client.listPartitions(catName, dbName, tableName,
        Collections.singletonList("a0"), -1);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(catName, fetched.get(0).getCatName());
    assertPartitionsHaveCorrectParams(fetched);

    PartitionSpecProxy proxy = client.listPartitionSpecs(catName, dbName, tableName, -1);
    Assert.assertEquals(parts.length, proxy.size());
    Assert.assertEquals(catName, proxy.getCatName());

    fetched = client.listPartitionsByFilter(catName, dbName, tableName, "partcol=\"a0\"", -1);
    Assert.assertEquals(1, fetched.size());
    Assert.assertEquals(catName, fetched.get(0).getCatName());
    assertPartitionsHaveCorrectParams(fetched);

    proxy = client.listPartitionSpecsByFilter(catName, dbName, tableName, "partcol=\"a0\"", -1);
    Assert.assertEquals(1, proxy.size());
    Assert.assertEquals(catName, proxy.getCatName());

    Assert.assertEquals(1, client.getNumPartitionsByFilter(catName, dbName, tableName,
        "partcol=\"a0\""));

    List<String> names = client.listPartitionNames(catName, dbName, tableName, 57);
    Assert.assertEquals(parts.length, names.size());

    names = client.listPartitionNames(catName, dbName, tableName, Collections.singletonList("a0"),
        Short.MAX_VALUE + 1);
    Assert.assertEquals(1, names.size());

    PartitionValuesRequest rqst = new PartitionValuesRequest(dbName,
        tableName, Lists.newArrayList(new FieldSchema("partcol", "string", "")));
    rqst.setCatName(catName);
    PartitionValuesResponse rsp = client.listPartitionValues(rqst);
    Assert.assertEquals(5, rsp.getPartitionValuesSize());
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionsBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitions("bogus", DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionsWithPartialValuesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitions("bogus", DB_NAME, TABLE_NAME, Collections.singletonList("a0"), -1);
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionsSpecsBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionSpecs("bogus", DB_NAME, TABLE_NAME, -1);
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionsByFilterBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionsByFilter("bogus", DB_NAME, TABLE_NAME, "partcol=\"a0\"", -1);
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionSpecsByFilterBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionSpecsByFilter("bogus", DB_NAME, TABLE_NAME, "partcol=\"a0\"", -1);
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void getNumPartitionsByFilterBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.getNumPartitionsByFilter("bogus", DB_NAME, TABLE_NAME, "partcol=\"a0\"");
  }

  @Test
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionNamesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    List<String> parts = client.listPartitionNames("bogus", DB_NAME, TABLE_NAME, -1);
    Assert.assertEquals(0, parts.size());
  }

  @Test(expected = NoSuchObjectException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionNamesPartialValsBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    client.listPartitionNames("bogus", DB_NAME, TABLE_NAME, Collections.singletonList("a0"), -1);
  }

  @Test(expected = MetaException.class)
  @ConditionalIgnoreOnSessionHiveMetastoreClient
  public void listPartitionValuesBogusCatalog() throws TException {
    createTable3PartCols1Part(client);
    PartitionValuesRequest rqst = new PartitionValuesRequest(DB_NAME,
        TABLE_NAME, Lists.newArrayList(new FieldSchema("partcol", "string", "")));
    rqst.setCatName("bogus");
    client.listPartitionValues(rqst);
  }
}
