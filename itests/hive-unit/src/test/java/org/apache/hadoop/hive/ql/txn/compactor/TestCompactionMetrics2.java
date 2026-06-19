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

import org.apache.hadoop.hive.metastore.HMSMetricsListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TestCompactionMetrics2 extends CompactorTest {
  @Test
  public void testWritesToDisabledCompactionDatabase() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS, HMSMetricsListener.class.getName());
    txnHandler = TxnUtils.getTxnStore(conf);

    String dbName = "test";
    Map<String, String> dbParams = new HashMap<String, String>(1);
    dbParams.put("NO_AUTO_COMPACTION", "true");
    ms.createDatabase(new Database(dbName, "", null, dbParams));

    Map<String, String> params = new HashMap<>();
    params.put(hive_metastoreConstants.NO_AUTO_COMPACT, "false");
    Table t =  newTable(dbName, "comp_disabled", false, params);
    burnThroughTransactions(dbName, t.getTableName(), 1, null, null);
    burnThroughTransactions(dbName, t.getTableName(), 1, null, new HashSet<>(
        Collections.singletonList(2L)));

    Assert.assertEquals(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE + " value incorrect",
        2, Metrics.getOrCreateGauge(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE).intValue());
  }

  @Test
  public void testWritesToEnabledCompactionDatabase() throws Exception {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS, HMSMetricsListener.class.getName());
    txnHandler = TxnUtils.getTxnStore(conf);

    String dbName = "test1";
    Map<String, String> dbParams = new HashMap<String, String>(1);
    dbParams.put("no_auto_compaction", "false");
    ms.createDatabase(new Database(dbName, "", null, dbParams));

    Map<String, String> params = new HashMap<>();
    params.put(hive_metastoreConstants.NO_AUTO_COMPACT, "true");
    Table t =  newTable(dbName, "comp_enabled", false, params);
    burnThroughTransactions(dbName, t.getTableName(), 1, null, null);
    burnThroughTransactions(dbName, t.getTableName(), 1, null, new HashSet<>(
        Collections.singletonList(2L)));

    Assert.assertEquals(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE + " value incorrect",
        0, Metrics.getOrCreateGauge(MetricsConstants.WRITES_TO_DISABLED_COMPACTION_TABLE).intValue());
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
