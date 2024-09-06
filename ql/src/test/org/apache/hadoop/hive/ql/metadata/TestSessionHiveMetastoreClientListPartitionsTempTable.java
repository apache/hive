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
import org.apache.hadoop.hive.metastore.TestMetastoreExpr;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.CustomIgnoreRule;
import org.apache.hadoop.hive.metastore.client.TestListPartitions;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.LineageState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
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
    QueryState queryState = QueryState.getNewQueryState(conf, new LineageState());
    queryState.createHMSCache();
    SessionState.get().addQueryState(queryState.getQueryId(), queryState);
    // setup metastore client cache
    if (conf.getBoolVar(HiveConf.ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(conf);
    }
    setClient(Hive.get(conf).getMSC());
    getClient().dropDatabase(DB_NAME, true, true, true);
    getMetaStore().cleanWarehouseDirs();
  }

  private void initHiveConf() throws HiveException {
    conf = new HiveConfForTest(Hive.get().getConf(), getClass());
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

  private void checkPartitionNames(List<String> expected, short numParts, String order,
      String defaultPartName, ExprNodeGenericFuncDesc expr, Table t) throws Exception {
    PartitionsByExprRequest request = new PartitionsByExprRequest();
    request.setDbName(DB_NAME);
    request.setTblName(TABLE_NAME);
    byte[] exprs = {(byte)-1};
    if (expr != null) {
      exprs = SerializationUtilities.serializeObjectWithTypeInformation(expr);
    }
    request.setExpr(exprs);
    request.setMaxParts(numParts);
    request.setOrder(order);
    request.setDefaultPartitionName(defaultPartName);
    request.setId(t.getId());
    List<String> partitionNames = getClient().listPartitionNames(request);
    assertArrayEquals(expected.toArray(), partitionNames.toArray());
  }

  @Test
  public void testListPartitionNames() throws Exception {
    Table t = createTable4PartColsParts(getClient()).table;
    String defaultPartitionName = HiveConf.getVar(conf, HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    List<List<String>> testValues = Lists.newArrayList(
        Lists.newArrayList("1999", defaultPartitionName, "02"),
        Lists.newArrayList(defaultPartitionName, "02", "10"),
        Lists.newArrayList("2017", "10", defaultPartitionName));

    for(List<String> vals : testValues) {
      addPartition(getClient(), t, vals);
    }
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    checkPartitionNames(Lists.newArrayList("yyyy=2017/mm=10/dd=26",
        "yyyy=2017/mm=10/dd=__HIVE_DEFAULT_PARTITION__",
        "yyyy=2017/mm=11/dd=27"),
        (short)3, null, defaultPartitionName,
        e.val("2017").strCol("yyyy").pred(">=", 2).build(), t);

    checkPartitionNames(Lists.newArrayList(
        "yyyy=2017/mm=11/dd=27",
        "yyyy=2017/mm=10/dd=26"),
        (short)2, "1,2:-+", defaultPartitionName,
        e.val("2017").strCol("yyyy").pred(">=", 2).build(), t);

    checkPartitionNames(Lists.newArrayList("yyyy=1999/mm=01/dd=02",
        "yyyy=1999/mm=__HIVE_DEFAULT_PARTITION__/dd=02",
        "yyyy=2009/mm=02/dd=10"),
        (short)3, null, defaultPartitionName, null, t);

    checkPartitionNames(Lists.newArrayList("yyyy=__HIVE_DEFAULT_PARTITION__/mm=02/dd=10",
        "yyyy=2017/mm=10/dd=26",
        "yyyy=2017/mm=10/dd=__HIVE_DEFAULT_PARTITION__"),
        (short)3, "0,1:-+", defaultPartitionName, null, t);

    checkPartitionNames(Lists.newArrayList("yyyy=1999/mm=01/dd=02",
        "yyyy=1999/mm=__HIVE_DEFAULT_PARTITION__/dd=02",
        "yyyy=2009/mm=02/dd=10"),
        (short)3, null, defaultPartitionName, null, t);
  }

  private void checkPartitionNames(int numParts, List<String> partVals) throws Exception {
    GetPartitionNamesPsRequest request = new GetPartitionNamesPsRequest();
    request.setDbName(DB_NAME);
    request.setTblName(TABLE_NAME);
    request.setPartValues(partVals);
    request.setMaxParts((short)-1);
    List<String> partNames = getClient().listPartitionNamesRequest(request).getNames();
    assertTrue(partNames.size() == numParts);
  }

  @Test
  public void testListPartitionNamesRequest() throws Exception {
    createTable4PartColsParts(getClient());
    checkPartitionNames(1, Lists.newArrayList("1999", "01", "02"));
    checkPartitionNames(2, Lists.newArrayList("2017", "", ""));
    checkPartitionNames(0, Lists.newArrayList("2008", "02", "10"));
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
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeObjectWithTypeInformation(
        e.strCol("yyyy").val("2017").pred("=", 2).build()), null,
        (short)-1, null);
  }

  @Test
  public void testListPartitionsByExprDefMaxParts() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    List<Partition> result = new ArrayList<>();
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeObjectWithTypeInformation(
        e.strCol("yyyy").val("2017").pred(">=", 2).build()), null, (short)3, result);
    assertEquals(3, result.size());
  }

  @Test
  public void testListPartitionsByExprHighMaxParts() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    List<Partition> result = new ArrayList<>();
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeObjectWithTypeInformation(
        e.strCol("yyyy").val("2017").pred(">=", 2).build()), null, (short)100, result);
    assertEquals(4, result.size());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByExprNoDb() throws Exception {
    getClient().dropDatabase(DB_NAME);
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByExprNoTbl() throws Exception {
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsByExprEmptyDbName() throws Exception {
    getClient().listPartitionsByExpr("", TABLE_NAME, new byte[] {'f', 'o', 'o'},
        null, (short)-1, new ArrayList<>());
  }

  @Test(expected = NoSuchObjectException.class)
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
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeObjectWithTypeInformation(expr),
        null, (short) -1, parts);
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, parts.size());
  }

  @Test
  public void testListPartitionsSpecByExpr() throws Exception {
    Table t = createTable4PartColsParts(getClient()).table;
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    checkExprPartitionSpec(2, e.strCol("yyyy").val("2017").pred("=", 2).build(), t);
    checkExprPartitionSpec(3, e.strCol("mm").val("11").pred(">", 2).build(), t);
    checkExprPartitionSpec(4, e.strCol("dd").val("29").pred(">=", 2).build(), t);
    checkExprPartitionSpec(2, e.strCol("yyyy").val("2017").pred("!=", 2).build(), t);
    checkExprPartitionSpec(1, e.strCol("yyyy").val("2017").pred("=", 2)
        .strCol("mm").val("10").pred(">=", 2).pred("and", 2).build(), t);
    checkExprPartitionSpec(3, e.strCol("dd").val("10").pred("<", 2).strCol("yyyy")
        .val("2009").pred("!=", 2).pred("or", 2).build(), t);
    checkExprPartitionSpec(0, e.strCol("yyyy").val("2019").pred("=", 2).build(), t);
  }

  @Test(expected = AssertionError.class)
  public void testListPartitionsSpecByExprNullResult() throws Exception {
    Table t = createTable4PartColsParts(getClient()).table;

    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);

    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
            ByteBuffer.wrap(SerializationUtilities.serializeObjectWithTypeInformation(
                    e.strCol("yyyy").val("2017").pred("=", 2).build())));
    req.setMaxParts((short)-1);
    req.setId(t.getId());

    getClient().listPartitionsSpecByExpr(req, null);
  }



  @Test
  public void testListPartitionsSpecByExprDefMaxParts() throws Exception {
    Table t = createTable4PartColsParts(getClient()).table;
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    List<PartitionSpec> result = new ArrayList<>();

    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
            ByteBuffer.wrap(SerializationUtilities.serializeObjectWithTypeInformation(
                    e.strCol("yyyy").val("2017").pred(">=", 2).build())));
    req.setMaxParts((short)3);
    req.setId(t.getId());

    getClient().listPartitionsSpecByExpr(req, result);
    assertEquals(3, result.iterator().next().getSharedSDPartitionSpec().getPartitionsSize());
  }

  @Test
  public void testListPartitionsSpecByExprHighMaxParts() throws Exception {
    Table t = createTable4PartColsParts(getClient()).table;
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    List<PartitionSpec> result = new ArrayList<>();

    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
            ByteBuffer.wrap(SerializationUtilities.serializeObjectWithTypeInformation(
                    e.strCol("yyyy").val("2017").pred(">=", 2).build())));
    req.setMaxParts((short)100);
    req.setId(t.getId());

    getClient().listPartitionsSpecByExpr(req, result);
    assertEquals(4, result.iterator().next().getSharedSDPartitionSpec().getPartitionsSize());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsSpecByExprNoDb() throws Exception {
    getClient().dropDatabase(DB_NAME);

    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
            ByteBuffer.wrap(new byte[] {'f', 'o', 'o'}));
    req.setMaxParts((short)-1);

    getClient().listPartitionsSpecByExpr(req, null);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsSpecByExprNoTbl() throws Exception {
    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
            ByteBuffer.wrap(new byte[] {'f', 'o', 'o'}));
    req.setMaxParts((short)-1);

    getClient().listPartitionsSpecByExpr(req, new ArrayList<>());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsSpecByExprEmptyDbName() throws Exception {
    PartitionsByExprRequest req = new PartitionsByExprRequest("", TABLE_NAME,
            ByteBuffer.wrap(new byte[] {'f', 'o', 'o'}));
    req.setMaxParts((short)-1);

    getClient().listPartitionsSpecByExpr(req, new ArrayList<>());
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionsSpecByExprEmptyTblName() throws Exception {
    Table t = createTable3PartCols1Part(getClient());

    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, "",
            ByteBuffer.wrap(new byte[] {'f', 'o', 'o'}));
    req.setMaxParts((short)-1);
    req.setId(t.getId());

    getClient().listPartitionsSpecByExpr(req, new ArrayList<>());
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsSpecByExprNullDbName() throws Exception {
    PartitionsByExprRequest req = new PartitionsByExprRequest(null, TABLE_NAME,
            ByteBuffer.wrap(new byte[] {'f', 'o', 'o'}));
    req.setMaxParts((short)-1);

    getClient().listPartitionsSpecByExpr(req, null);
  }

  @Test(expected = MetaException.class)
  public void testListPartitionsSpecByExprNullTblName() throws Exception {
    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, null,
            ByteBuffer.wrap(new byte[] {'f', 'o', 'o'}));
    req.setMaxParts((short)-1);

    getClient().listPartitionsSpecByExpr(req, null);
  }

  private void checkExprPartitionSpec(int numParts, ExprNodeGenericFuncDesc expr, Table t) throws Exception {
    List<Partition> parts = new ArrayList<>();
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, SerializationUtilities.serializeObjectWithTypeInformation(expr),
        null, (short) -1, parts);
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, parts.size());
    // check with partition spec as well
    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
            ByteBuffer.wrap(SerializationUtilities.serializeObjectWithTypeInformation(expr)));
    req.setMaxParts((short)-1);
    req.setId(t.getId());

    List<PartitionSpec> partSpec = new ArrayList<>();
    getClient().listPartitionsSpecByExpr(req, partSpec);
    int partSpecSize = 0;
    if(!partSpec.isEmpty()) {
      partSpecSize = partSpec.iterator().next().getSharedSDPartitionSpec().getPartitionsSize();
    }
    assertEquals("Partition Spec check failed: " + expr.getExprString(), numParts, partSpecSize);
  }

  @Test(expected = NoSuchObjectException.class)
  @Override
  public void testListPartitionNamesNoTable() throws Exception {
    super.testListPartitionNamesNoTable();
  }
}
