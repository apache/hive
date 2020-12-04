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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.thrift.TException;

public class TestHMSHandler extends HiveMetaStore.HMSHandler {
  public TestHMSHandler(String name, Configuration conf, Boolean init) throws MetaException {
    super(name, conf, init);
  }

  @Override
  public void init() throws MetaException {
    super.init();
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException,
     NoSuchObjectException {
    LogHelper console = SessionState.getConsole();

    if (console != null) {
      console.printInfo("HMS CALL get_table_req FOR " + req.getTblName(), false);
    }
    return super.get_table_req(req);
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request) throws TException {
    LogHelper console = SessionState.getConsole();

    if (console != null) {
      console.printInfo("HMS CALL get_foreign_keys FOR " + request.getForeign_tbl_name(), false);
    }
    return super.get_foreign_keys(request);
  }
}
