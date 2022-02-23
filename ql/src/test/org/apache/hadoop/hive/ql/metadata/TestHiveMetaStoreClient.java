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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 *  TestHiveMetaStoreClient
 *
 *  This class provides implementation of certain methods from IMetaStoreClient interface.
 *  It's mainly used to verify the arguments that are sent from HS2 to HMS APIs.
 *  This class' methods are used when HS2 methods are invoked from
 *  {@link TestHiveMetaStoreClientApiArgumentsChecker}.
 *
 *  Tests in this class ensure that both tableId and validWriteIdList are sent from HS2 in the input of
 *  HMS get_* APIs.
 *  tableId and validWriteIdList are used to determine whether the response of a given get_* API should
 *  be served from the cache or the backing DB.
 *  So, if we want consistent read from cache, it is important to send both validWriteIdList and tableId
 *  in the get_* API request, so that we can compare them and decide where to send the data from.
 *  Right now only few get_* APIs take validWriteIdList and tableId in the input.
 *  As we support more APIs, we should add them here with appropriate test cases.
 *
 */
public class TestHiveMetaStoreClient extends HiveMetaStoreClientWithLocalCache implements IMetaStoreClient {

  public TestHiveMetaStoreClient(Configuration conf) throws MetaException {
    super(conf);
  }

  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req)
      throws NoSuchObjectException, MetaException, TException {
    assertNotNull(req.getId());
    assertNotNull(req.getValidWriteIdList());
    GetPartitionResponse res = new GetPartitionResponse();
    return res;
  }

  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    assertNotNull(req.getId());
    assertNotNull(req.getValidWriteIdList());
    GetPartitionNamesPsResponse res = new GetPartitionNamesPsResponse();
    return res;
  }

  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest req)
      throws MetaException, TException, NoSuchObjectException {
    assertNotNull(req.getId());
    assertNotNull(req.getValidWriteIdList());
    GetPartitionsPsWithAuthResponse res = new GetPartitionsPsWithAuthResponse();
    return res;

  }

  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result) throws TException {
    assertNotNull(req.getId());
    assertNotNull(req.getValidWriteIdList());
    return false;
  }

  public Table getTable(String dbName, String tableName, boolean getColumnStats, String engine)
      throws MetaException, TException, NoSuchObjectException {
    GetTableRequest getTableRequest = new GetTableRequest( dbName, tableName);
    getTableRequest.setGetColumnStats(getColumnStats);
    getTableRequest.setEngine(engine);
    return getTable(getTableRequest);

  }

  public Table getTable(GetTableRequest getTableRequest)
      throws MetaException, TException, NoSuchObjectException {
    Table tTable = new Table();
    tTable.setId(Long.MAX_VALUE);
    Map<String, String> parameters = new HashMap<>();
    parameters.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    tTable.setParameters(parameters);
    Map<String, String> sdParameters = new HashMap<>();
    StorageDescriptor sd = new StorageDescriptor();
    sd.setParameters(sdParameters);
    SerDeInfo si = new SerDeInfo();
    sd.setSerdeInfo(si);
    tTable.setSd(sd);

    return tTable;
  }

  public GetPartitionsByNamesResult getPartitionsByNamesInternal(GetPartitionsByNamesRequest req)
      throws NoSuchObjectException, MetaException, TException {
    assertNotNull(req.getId());
    assertNotNull(req.getValidWriteIdList());
    GetPartitionsByNamesResult res = new GetPartitionsByNamesResult();
    return res;
  }

}
