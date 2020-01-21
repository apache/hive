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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TestMetastoreExpr;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestListPartitions;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test class for list partitions related methods on temporary tables.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestSessionHiveMetastoreClientListPartitionsTempTable
    extends TestListPartitions {

  private HiveConf conf;

  private static final String PART_PRIV = "PARTITION_LEVEL_PRIVILEGE";

  public TestSessionHiveMetastoreClientListPartitionsTempTable(String name, AbstractMetaStoreService metaStore) {
    super(name, metaStore);
    ignoreRule = new CustomIgnoreRule();
  }

  @Before
  public void setUp() throws Exception {
    initHiveConf();
    SessionState.start(conf);
    setClient(Hive.get(conf).getMSC());
    getClient().dropDatabase(DB_NAME, true, true, true);
    getMetaStore().cleanWarehouseDirs();
  }

  private void initHiveConf() throws HiveException {
    conf = Hive.get().getConf();
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
  }

  @Override
  protected Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
      List<String> partCols, boolean setPartitionLevelPrivileges) throws TException {
    TableBuilder builder =
        new TableBuilder().setDbName(dbName).setTableName(tableName).addCol("id", "int")
            .addCol("name", "string").setTemporary(true);

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
      groupPrivileges.put(GROUP, new ArrayList<>());
      privileges.setGroupPrivileges(groupPrivileges);
      partition.setPrivileges(privileges);
    }
    client.add_partition(partition);
  }

  @Override
  protected void assertAuthInfoReturned(String userName, String group, Partition partition) {
    PrincipalPrivilegeSet privileges = partition.getPrivileges();
    assertNotNull(privileges);
    assertTrue(privileges.getUserPrivileges().containsKey(userName));
    assertTrue(privileges.getGroupPrivileges().containsKey(group));
  }


  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionsAllNullTblName() throws Exception {
    super.testListPartitionsAllNullTblName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionsAllNullDbName() throws Exception {
    super.testListPartitionsAllNullDbName();
  }


  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionSpecsNullTblName() throws Exception {
    super.testListPartitionSpecsNullTblName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionsWithAuthByValuesNullDbName()
      throws Exception {
    super.testListPartitionsWithAuthByValuesNullDbName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionsWithAuthByValuesNullTblName()
      throws Exception {
    super.testListPartitionsWithAuthByValuesNullTblName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionNamesNullDbName() throws Exception {
    super.testListPartitionNamesNullDbName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionNamesNullTblName() throws Exception {
    super.testListPartitionNamesNullTblName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionNamesByValuesNullDbName()
      throws Exception {
    super.testListPartitionNamesByValuesNullDbName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionNamesByValuesNullTblName()
      throws Exception {
    super.testListPartitionNamesByValuesNullTblName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionsByFilterNullTblName() throws Exception {
    super.testListPartitionsByFilterNullTblName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionsByFilterNullDbName() throws Exception {
    super.testListPartitionsByFilterNullDbName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionValuesNullDbName() throws Exception {
    super.testListPartitionValuesNullDbName();
  }

  @Test(expected = MetaException.class)
  @Override
  public void testListPartitionValuesNullTblName() throws Exception {
    super.testListPartitionValuesNullTblName();
  }

  @Test(expected = NoSuchObjectException.class)
  @Override
  public void testListPartitionNamesNoDb() throws Exception {
    super.testListPartitionNamesNoDb();
  }

  @Test
  @Override
  public void testListPartitionsAllNoTable() throws Exception {
    super.testListPartitionsAllNoTable();
  }

  @Test
  public void testListPartitionsByExpr() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    checkExpr(2, e.strCol("yyyy").val("2017").pred("=", 2).build());
    checkExpr(3, e.strCol("mm").val("11").pred(">", 2).build());
    checkExpr(4, e.strCol("dd").val("29").pred(">=", 2).build());
    checkExpr(2, e.strCol("yyyy").val("2017").pred("!=", 2).build());
    checkExpr(1, e.strCol("yyyy").val("2017").pred("=", 2)
        .strCol("mm").val("10").pred(">=", 2).pred("and", 2).build());
    checkExpr(3, e.strCol("dd").val("10").pred("<", 2).strCol("yyyy")
        .val("2009").pred("!=", 2).pred("or", 2).build());
    checkExpr(0, e.strCol("yyyy").val("2019").pred("=", 2).build());
  }

  @Test(expected = AssertionError.class)
  public void testListPartitionsByExprNullResult() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeExpressionToKryo(
        e.strCol("yyyy").val("2017").pred("=", 2).build()), null, (short)-1, null);
  }

  @Test
  public void testListPartitionsByExprDefMaxParts() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    List<Partition> result = new ArrayList<>();
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeExpressionToKryo(
        e.strCol("yyyy").val("2017").pred(">=", 2).build()), null, (short)3, result);
    assertEquals(3, result.size());
  }

  @Test
  public void testListPartitionsByExprHighMaxParts() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    List<Partition> result = new ArrayList<>();
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeExpressionToKryo(
        e.strCol("yyyy").val("2017").pred(">=", 2).build()), null, (short)100, result);
    assertEquals(4, result.size());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByExprNoDb() throws Exception {
    getClient().dropDatabase(DB_NAME);
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByExprNoTbl() throws Exception {
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByExprEmptyDbName() throws Exception {
    getClient().listPartitionsByExpr("", TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByExprEmptyTblName() throws Exception {
    createTable3PartCols1Part(getClient());
    getClient().listPartitionsByExpr(DB_NAME, "", new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByExprNullDbName() throws Exception {
    getClient().listPartitionsByExpr(null, TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsByExprNullTblName() throws Exception {
    getClient().listPartitionsByExpr(DB_NAME, null, new byte[] {'f', 'o', 'o' },
        null, (short)-1, new ArrayList<>());
  }

  private void checkExpr(int numParts, ExprNodeGenericFuncDesc expr) throws Exception {
    List<Partition> parts = new ArrayList<>();
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeExpressionToKryo(expr),
        null, (short) -1, parts);
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, parts.size());
  }

}
