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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * API tests for HMS client's getPartitions methods.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetPartitions {

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should remove our own copy
  private static Set<AbstractMetaStoreService> metaStoreServices = null;
  private AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  private static final String DB_NAME = "testpartdb";
  private static final String TABLE_NAME = "testparttable";


  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> getMetaStoreToTest() throws Exception {
    List<Object[]> result = MetaStoreFactoryForTests.getMetaStores();
    metaStoreServices = result.stream()
            .map(test -> (AbstractMetaStoreService)test[1])
            .collect(Collectors.toSet());
    return result;
  }

  public TestGetPartitions(String name, AbstractMetaStoreService metaStore) throws Exception {
    this.metaStore = metaStore;
    this.metaStore.start();
  }

  // Needed until there is no junit release with @BeforeParam, @AfterParam (junit 4.13)
  // https://github.com/junit-team/junit4/commit/1bf8438b65858565dbb64736bfe13aae9cfc1b5a
  // Then we should move this to @AfterParam
  @AfterClass
  public static void stopMetaStores() throws Exception {
    for(AbstractMetaStoreService metaStoreService : metaStoreServices) {
      metaStoreService.stop();
    }
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
        client.close();
      }
    } finally {
      client = null;
    }
  }

  private void createDB(String dbName) throws TException {
    Database db = new DatabaseBuilder().
            setName(dbName).
            build();
    client.createDatabase(db);
  }


  private static Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                       List<String> partCols, boolean setPartitionLevelPrivilages)
          throws Exception {
    TableBuilder builder = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("id", "int")
            .addCol("name", "string");

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build();

    if (setPartitionLevelPrivilages) {
      table.putToParameters("PARTITION_LEVEL_PRIVILEGE", "true");
    }

    client.createTable(table);
    return table;
  }

  private static void addPartition(IMetaStoreClient client, Table table, List<String> values)
          throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().fromTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    client.add_partition(partitionBuilder.build());
  }

  private static void createTable3PartCols1PartGeneric(IMetaStoreClient client, boolean authOn)
          throws Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm",
            "dd"), authOn);
    addPartition(client, t, Lists.newArrayList("1997", "05", "16"));
  }

  private static void createTable3PartCols1Part(IMetaStoreClient client) throws Exception {
    createTable3PartCols1PartGeneric(client, false);
  }

  private static void createTable3PartCols1PartAuthOn(IMetaStoreClient client) throws Exception {
    createTable3PartCols1PartGeneric(client, true);
  }

  private static List<List<String>> createTable4PartColsParts(IMetaStoreClient client) throws
          Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"),
            false);
    List<List<String>> testValues = Lists.newArrayList(
            Lists.newArrayList("1999", "01", "02"),
            Lists.newArrayList("2009", "02", "10"),
            Lists.newArrayList("2017", "10", "26"),
            Lists.newArrayList("2017", "11", "27"));

    for(List<String> vals : testValues){
      addPartition(client, t, vals);
    }

    return testValues;
  }

  private static void assertAuthInfoReturned(String user, String group, Partition partition) {
    assertNotNull(partition.getPrivileges());
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getUserPrivileges().get(user));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getGroupPrivileges().get(group));
    assertEquals(Lists.newArrayList(),
            partition.getPrivileges().getRolePrivileges().get("public"));
  }



  /**
   * Testing getPartition(String,String,String) ->
   *         get_partition_by_name(String,String,String).
   * @throws Exception
   */
  @Test
  public void testGetPartition() throws Exception {
    createTable3PartCols1Part(client);
    Partition partition = client.getPartition(DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
    assertNotNull(partition);
    assertEquals(Lists.newArrayList("1997", "05", "16"), partition.getValues());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionCaseSensitive() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, "YyYy=1997/mM=05/dD=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionIncompletePartName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, "yyyy=1997/mm=05");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionEmptyPartName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, "");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNonexistingPart() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=99");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition("", TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, "", "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoTable() throws Exception {
    client.getPartition(DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.getPartition(DB_NAME, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionNullDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(null, TABLE_NAME, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionNullTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, null, "yyyy=1997/mm=05/dd=16");
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionNullPartName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, (String)null);
  }



  /**
   * Testing getPartition(String,String,List(String)) ->
   *         get_partition(String,String,List(String)).
   * @throws Exception
   */
  @Test
  public void testGetPartitionByValues() throws Exception {
    createTable3PartCols1Part(client);
    List<String> parts = Lists.newArrayList("1997", "05", "16");
    Partition partition = client.getPartition(DB_NAME, TABLE_NAME, parts);
    assertNotNull(partition);
    assertEquals(parts, partition.getValues());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesWrongPart() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "66"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesWrongNumOfPartVals() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesEmptyPartVals() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition("", TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, "", Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoTable() throws Exception {
    client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionByValuesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.getPartition(DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesNullDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(null, TABLE_NAME, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesNullTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, null, Lists.newArrayList("1997", "05", "16"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionByValuesNullValues() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartition(DB_NAME, TABLE_NAME, (List<String>)null);
  }



  /**
   * Testing getPartitionsByNames(String,String,List(String)) ->
   *         get_partitions_by_names(String,String,List(String)).
   * @throws Exception
   */
  @Test
  public void testGetPartitionsByNames() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);

    //TODO: partition names in getPartitionsByNames are not case insensitive
    List<Partition> partitions = client.getPartitionsByNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("yYYy=2017/MM=11/DD=27", "yYyY=1999/mM=01/dD=02"));
    assertEquals(0, partitions.size());

    partitions = client.getPartitionsByNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("yyyy=2017/mm=11/dd=27", "yyyy=1999/mm=01/dd=02"));
    assertEquals(2, partitions.size());
    assertEquals(testValues.get(0), partitions.get(0).getValues());
    assertEquals(testValues.get(3), partitions.get(1).getValues());


    partitions = client.getPartitionsByNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("yyyy=2017", "yyyy=1999/mm=01/dd=02"));
    assertEquals(testValues.get(0), partitions.get(0).getValues());
  }

  @Test
  public void testGetPartitionsByNamesEmptyParts() throws Exception {
    List<List<String>> testValues = createTable4PartColsParts(client);

    List<Partition> partitions = client.getPartitionsByNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList("", ""));
    assertEquals(0, partitions.size());

    partitions = client.getPartitionsByNames(DB_NAME, TABLE_NAME,
            Lists.newArrayList());
    assertEquals(0, partitions.size());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoDbName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames("", TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoTblName() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames(DB_NAME, "", Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoTable() throws Exception {
    client.getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionsByNamesNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
  }

  @Test
  public void testGetPartitionsByNamesNullDbName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.getPartitionsByNames(null, TABLE_NAME, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testGetPartitionsByNamesNullTblName() throws Exception {
    try {
      createTable3PartCols1Part(client);
      client.getPartitionsByNames(DB_NAME, null, Lists.newArrayList("yyyy=2000/mm=01/dd=02"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionsByNamesNullNames() throws Exception {
    createTable3PartCols1Part(client);
    client.getPartitionsByNames(DB_NAME, TABLE_NAME, (List<String>)null);
  }



  /**
   * Testing getPartitionWithAuthInfo(String,String,List(String),String,List(String)) ->
   *         get_partition_with_auth(String,String,List(String),String,List(String)).
   * @throws Exception
   */
  @Test
  public void testGetPartitionWithAuthInfoNoPrivilagesSet() throws Exception {
    createTable3PartCols1Part(client);
    Partition partition = client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME, Lists.newArrayList(
            "1997", "05", "16"), "", Lists.newArrayList());
    assertNotNull(partition);
    assertNull(partition.getPrivileges());
  }

  @Test
  public void testGetPartitionWithAuthInfo() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    Partition partition = client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
    assertNotNull(partition);
    assertAuthInfoReturned("user0", "group0", partition);
  }

  @Test
  public void testGetPartitionWithAuthInfoEmptyUserGroup() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    Partition partition = client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "", Lists.newArrayList(""));
    assertNotNull(partition);
    assertAuthInfoReturned("", "", partition);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNoDbName() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo("", TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNoTblName() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DB_NAME, "",
            Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNoSuchPart() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "66"), "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionWithAuthInfoWrongNumOfPartVals() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05"), "user0", Lists.newArrayList("group0"));
  }

  @Test
  public void testGetPartitionWithAuthInfoNullDbName() throws Exception {
    try {
      createTable3PartCols1PartAuthOn(client);
      client.getPartitionWithAuthInfo(null, TABLE_NAME,
              Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test
  public void testGetPartitionWithAuthInfoNullTblName() throws Exception {
    try {
      createTable3PartCols1PartAuthOn(client);
      client.getPartitionWithAuthInfo(DB_NAME, null,
              Lists.newArrayList("1997", "05", "16"), "user0", Lists.newArrayList("group0"));
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
      //TODO: should not throw different exceptions for different HMS deployment types
    }
  }

  @Test(expected = MetaException.class)
  public void testGetPartitionWithAuthInfoNullValues() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
            null, "user0", Lists.newArrayList("group0"));
  }

  @Test(expected = NoSuchObjectException.class)
  public void testGetPartitionWithAuthInfoNullUser() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DB_NAME, "",
            Lists.newArrayList("1997", "05", "16"), null, Lists.newArrayList("group0"));
  }

  @Test
  public void testGetPartitionWithAuthInfoNullGroups() throws Exception {
    createTable3PartCols1PartAuthOn(client);
    client.getPartitionWithAuthInfo(DB_NAME, TABLE_NAME,
            Lists.newArrayList("1997", "05", "16"), "user0", null);
  }


}
