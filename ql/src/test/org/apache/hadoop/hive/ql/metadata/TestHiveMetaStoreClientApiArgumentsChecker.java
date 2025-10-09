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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.thrift.TException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TestHiveMetaStoreClientApiArgumentsChecker
 *
 * This class works with {@link TestHiveMetaStoreClient} in order to verify the arguments that
 * are sent from HS2 to HMS APIs.
 *
 */
public class TestHiveMetaStoreClientApiArgumentsChecker {

  private Hive hive;
  private IMetaStoreClient msc;
  private FileSystem fs;
  final static String DB_NAME = "db";
  final static String TABLE_NAME = "table";
  private IMetaStoreClient client;
  private Table t;

  protected static final String USER_NAME = "user0";

  @Before
  public void setUp() throws Exception {

    client = new TestHiveMetaStoreClient(new HiveConf(Hive.class));
    hive = Hive.get(client);
    HiveConf conf = new HiveConfForTest(hive.getConf(), getClass());
    conf.set(MetastoreConf.ConfVars.FS_HANDLER_THREADS_COUNT.getVarname(), "15");
    conf.set(MetastoreConf.ConfVars.MSCK_PATH_VALIDATION.getVarname(), "throw");
    msc = new HiveMetaStoreClient(conf);

    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.set(ValidTxnList.VALID_TXNS_KEY, "1:");
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, TABLE_NAME + ":1:");
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.TestTxnManager");
    // Pick a small number for the batch size to easily test code with multiple batches.
    conf.setIntVar(HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX, 2);
    SessionState.start(conf);
    SessionState.get().initTxnMgr(conf);
    Context ctx = new Context(conf);
    SessionState.get().getTxnMgr().openTxn(ctx, USER_NAME);

    t = new Table();
    org.apache.hadoop.hive.metastore.api.Table tTable = new org.apache.hadoop.hive.metastore.api.Table();
    tTable.setId(Long.MAX_VALUE);
    t.setTTable(tTable);
    Map<String, String> parameters = new HashMap<>();
    parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    tTable.setParameters(parameters);
    tTable.setTableType(TableType.MANAGED_TABLE.toString());
    t.setTableName(TABLE_NAME);
    t.setDbName(DB_NAME);

    List<FieldSchema> partCols = new ArrayList<>();
    partCols.add(new FieldSchema());
    t.setPartCols(partCols);

  }

  @Test
  public void testGetPartition() throws HiveException {
    hive.getPartition(t, DB_NAME, TABLE_NAME, null);
  }

  @Test
  public void testGetPartitions() throws HiveException {
    hive.getPartitions(t);
  }

  @Test
  public void testGetPartitionNames() throws HiveException {
    hive.getPartitionNames(DB_NAME, TABLE_NAME, null, (short) -1);
  }

  @Test
  public void testGetPartitionNames2() throws HiveException {
    hive.getPartitionNames(t, null, null, (short) -1);
  }

  @Test
  public void testGetPartitionsByNames1() throws HiveException {
    GetPartitionsByNamesRequest req = new GetPartitionsByNamesRequest();
    req.setDb_name(DB_NAME);
    req.setTbl_name(TABLE_NAME);
    hive.getPartitionsByNames(req, t);
  }

  @Test
  public void testGetPartitionsByNames2() throws HiveException {
    hive.getPartitionsByNames(DB_NAME,TABLE_NAME,null, t);
  }

  @Test
  public void testGetPartitionsByNames3() throws HiveException {
    hive.getPartitionsByNames(t, new ArrayList<>(), true);
  }

  @Test
  public void testGetPartitionsByNamesWithSingleBatch() throws HiveException {
    hive.getPartitionsByNames(t, Arrays.asList("Greece", "Italy"), true);
  }

  @Test
  public void testGetPartitionsByNamesWithMultipleEqualSizeBatches()
      throws HiveException {
    List<String> names = Arrays.asList("Greece", "Italy", "France", "Spain");
    hive.getPartitionsByNames(t, names, true);
  }

  @Test
  public void testGetPartitionsByNamesWithMultipleUnequalSizeBatches()
      throws HiveException {
    List<String> names =
        Arrays.asList("Greece", "Italy", "France", "Spain", "Hungary");
    hive.getPartitionsByNames(t, names, true);
  }

  @Test
  public void testGetPartitionsByExpr() throws HiveException, TException {
    List<Partition> partitions = new ArrayList<>();
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "rid",
        null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "f");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node =
        new ExprNodeGenericFuncDesc(TypeInfoFactory.stringTypeInfo,
            new GenericUDFOPEqualOrGreaterThan(), children);
    hive.getPartitionsByExpr(t, node, hive.getConf(), partitions);
  }
}


