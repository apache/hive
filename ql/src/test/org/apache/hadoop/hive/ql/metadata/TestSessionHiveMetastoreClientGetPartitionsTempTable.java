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

package org.apache.hadoop.hive.ql.metadata;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestGetPartitions;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * Test class for get partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientGetPartitionsTempTable extends TestGetPartitions {

  private HiveConf conf;

  private static final String USER_NAME = "user0";
  private static final List<String> GROUPS = Lists.newArrayList("group0", "group1");
  private static final String PART_PRIV = "PARTITION_LEVEL_PRIVILEGE";

  public TestSessionHiveMetastoreClientGetPartitionsTempTable(String name, AbstractMetaStoreService metaStore) {
    super(name, metaStore);
    ignoreRule = new CustomIgnoreRule();
  }

  @Before
  public void setUp() throws Exception {
    initHiveConf();
    SessionState.start(conf);
    super.setClient(Hive.get(conf).getMSC());
    getClient().dropDatabase(DB_NAME, true, true, true);
  }

  private void initHiveConf() throws HiveException {
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
      List<String> partCols, boolean setPartitionLevelPrivileges) throws TException {
    TableBuilder builder =
        new TableBuilder().setDbName(dbName).setTableName(tableName).addCol("id", "int").addCol("name", "string")
            .setTemporary(true);

    partCols.forEach(col -> builder.addPartCol(col, "string"));
    Table table = builder.build(conf);

    if (setPartitionLevelPrivileges) {
      table.putToParameters(PART_PRIV, "true");
    }

    client.createTable(table);
    return table;
  }

  @Override
  protected void addPartition(IMetaStoreClient client, Table table, List<String> values) throws TException {
    PartitionBuilder builder = new PartitionBuilder().inTable(table);
    values.forEach(builder::addValue);
    Partition partition = builder.build(conf);
    if (table.getParameters().containsKey(PART_PRIV) && table.getParameters().get(PART_PRIV).equals("true")) {
      PrincipalPrivilegeSet privileges = new PrincipalPrivilegeSet();
      Map<String, List<PrivilegeGrantInfo>> userPrivileges = new HashMap<>();
      userPrivileges.put(USER_NAME, new ArrayList<>());
      privileges.setUserPrivileges(userPrivileges);

      Map<String, List<PrivilegeGrantInfo>> groupPrivileges = new HashMap<>();
      GROUPS.forEach(g -> groupPrivileges.put(g, new ArrayList<>()));
      privileges.setGroupPrivileges(groupPrivileges);
      partition.setPrivileges(privileges);
    }
    client.add_partition(partition);
  }

  @Test(expected = MetaException.class)
  @Override
  public void testGetPartitionsByNamesEmptyParts() throws Exception {
    createTable4PartColsParts(getClient());
    getClient().getPartitionsByNames(DB_NAME, TABLE_NAME, Lists.newArrayList("", ""));
  }

  @Test(expected = MetaException.class)
  @Override
  public void testGetPartitionsByNamesNullTblName() throws Exception {
    super.testGetPartitionsByNamesNullTblName();
  }

  @Test
  @Override
  public void testGetPartitionWithAuthInfo() throws Exception {
    createTable3PartCols1PartAuthOn(getClient());
    Partition partition = getClient()
        .getPartitionWithAuthInfo(DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"), USER_NAME, GROUPS);
    assertNotNull(partition);
    assertAuthInfoReturned(USER_NAME, GROUPS, partition);
  }

  @Test
  @Override
  public void testGetPartitionWithAuthInfoEmptyUserGroup() throws Exception {
    createTable3PartCols1PartAuthOn(getClient());
    Partition partition = getClient()
        .getPartitionWithAuthInfo(DB_NAME, TABLE_NAME, Lists.newArrayList("1997", "05", "16"), "",
            Lists.newArrayList());
    assertNotNull(partition);
    assertAuthInfoReturned(USER_NAME, GROUPS, partition);
  }

  @Test(expected = MetaException.class)
  @Override
  public void testGetPartitionWithAuthInfoNullDbName()
      throws Exception {
    super.testGetPartitionWithAuthInfoNullDbName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testGetPartitionWithAuthInfoNullTblName()
      throws Exception {
    super.testGetPartitionWithAuthInfoNullTblName();
  }

  private void assertAuthInfoReturned(String userName, List<String> groups, Partition partition) {
    PrincipalPrivilegeSet privileges = partition.getPrivileges();
    assertNotNull(privileges);
    assertTrue(privileges.getUserPrivileges().containsKey(userName));
    for (String group : groups) {
      assertTrue(privileges.getGroupPrivileges().containsKey(group));
    }
  }

}
