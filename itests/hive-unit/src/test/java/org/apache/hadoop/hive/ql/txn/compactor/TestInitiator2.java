/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestInitiator2 extends CompactorTest {
  @Test
  public void dbNoAutoCompactSetTrue() throws Exception {
    String dbName = "test";
    Map<String, String> dbParams = new HashMap<String, String>(1);
    dbParams.put("no_auto_compaction", "true");
    Database db = new Database(dbName, "", null, dbParams);
    ms.createDatabase(db);
    Map<String, String> parameters = new HashMap<>(1);
    parameters.put("no_auto_compaction", "true");
    Table t = newTable(dbName, "dbnacst", false, parameters);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, dbName);
      comp.setTablename("dbnacst");
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(0, rsp.getCompactsSize());
  }

  @Test
  public void dbNoAutoCompactSetFalseUpperCase() throws Exception {
    boolean useCleanerForAbortCleanup = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEAN_ABORTS_USING_CLEANER);
    String dbName = "test1";
    Map<String, String> params = new HashMap<String, String>(1);
    params.put("NO_AUTO_COMPACTION", "false");
    Database db = new Database(dbName, "", null, params);
    ms.createDatabase(db);
    Map<String, String> parameters = new HashMap<>(1);
    parameters.put("no_auto_compaction", "true");
    Table t = newTable(dbName, "dbnacsf", false, parameters);

    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD, 10);

    for (int i = 0; i < 11; i++) {
      long txnid = openTxn();
      LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, dbName);
      comp.setTablename("dbnacsf");
      comp.setOperationType(DataOperationType.UPDATE);
      List<LockComponent> components = new ArrayList<LockComponent>(1);
      components.add(comp);
      LockRequest req = new LockRequest(components, "me", "localhost");
      req.setTxnid(txnid);
      txnHandler.lock(req);
      txnHandler.abortTxn(new AbortTxnRequest(txnid));
    }

    startInitiator();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(useCleanerForAbortCleanup ? 0 : 1, rsp.getCompactsSize());
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
