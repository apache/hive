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
package org.apache.hadoop.hive.metastore.txn;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.txn.compactor.TestCleaner;
import org.junit.Before;

/**
 * Same as TestCleaner but in legacy mode - when it works by not setting CQ_NEXT_TXN_ID.
 */
public class TestCleanerLegacy extends TestCleaner {

  public static class LegacyTxnHandler extends org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler {
    @Override
    protected void updateWSCommitIdAndCleanUpMetadata(Statement stmt, long txnid, TxnType txnType, Long commitId,
        long tempId) throws SQLException, MetaException {
      super.updateWSCommitIdAndCleanUpMetadataInternal(stmt, txnid, txnType, commitId, tempId);
    }
  }

  @Before
  public void setup() throws Exception {
    HiveConf conf = new HiveConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_USE_MIN_HISTORY_LEVEL, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TXN_STORE_IMPL, LegacyTxnHandler.class.getName());
    setup(conf);
  }

}
