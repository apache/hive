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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.SortedMap;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.*;
import static org.junit.Assert.assertNotNull;

public class TestDBMetrics {

  @Before
  public void setUp() throws Exception {
    Configuration metastoreConf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(metastoreConf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    MetastoreConf.setClass(metastoreConf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
            MockPartitionExpressionForMetastore.class, PartitionExpressionProxy.class);
    MetastoreConf.setBoolVar(metastoreConf, MetastoreConf.ConfVars.TRY_DIRECT_SQL_DDL, false);
    MetastoreConf.setBoolVar(metastoreConf, MetastoreConf.ConfVars.DATANUCLEUS_ENABLE_STATISTICS, true);
    MetaStoreTestUtils.setConfForStandloneMode(metastoreConf);
    if (MetastoreConf.getBoolVar(metastoreConf, MetastoreConf.ConfVars.METRICS_ENABLED)) {
      try {
        Metrics.initialize(metastoreConf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    MetaStoreServerUtils.startMetaStore(metastoreConf);
  }

  @After
  public void tearDown() throws Exception {
    ThreadPool.shutdown();
  }

  @Test
  public void testGetMetrics() throws Exception {
    MetricRegistry metrics = Metrics.getRegistry();
    SortedMap<String, Gauge> gauges = metrics.getGauges();
    assertNotNull(gauges.get(DB_NUMBER_OF_DATASTORE_READS).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_DATASTORE_WRITES).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_DATASTORE_READS_IN_LATES_TXN).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_DATASTORE_WRITES_IN_LATEST_TXN).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_OBJECT_DELETES).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_OBJECTS_FETCHES).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_OBJECT_INSERT).getValue());
    assertNotNull(gauges.get(DB_NUMBER_OF_OBJECT_UPDATES).getValue());
    assertNotNull(gauges.get(DB_CONNECTION_ACTIVE_CURRENT).getValue());
    assertNotNull(gauges.get(DB_CONNECTION_ACTIVE_TOTAL).getValue());
    assertNotNull(gauges.get(DB_CONNECTION_ACTIVE_HIGH).getValue());
    assertNotNull(gauges.get(DB_QUERY_EXECUTION_TIME_AVERAGE).getValue());
    assertNotNull(gauges.get(DB_QUERY_EXECUTION_TIME_HIGH).getValue());
    assertNotNull(gauges.get(DB_QUERY_ACTIVE_TOTAL_COUNT).getValue());
    assertNotNull(gauges.get(DB_QUERY_ERROR_TOTAL_COUNT).getValue());
    assertNotNull(gauges.get(DB_QUERY_EXECUTION_TIME_LOW).getValue());
    assertNotNull(gauges.get(DB_QUERY_EXECUTIO_TOTAL_COUNT).getValue());
    assertNotNull(gauges.get(DB_QUERY_EXECUTION_TOTAL_TIME).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_EXECUTION_TIME_AVERAGE).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_EXECUTION_TIME_HIGH).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_ACTIVE_TOTAL_COUNT).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_COMMITTED_TOTAL_COUNT).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_EXECUTION_TIME_LOW).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_EXECUTION_TOTAL_TIME).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_ROLLEDBACK_TOTAL_COUNT).getValue());
    assertNotNull(gauges.get(DB_TRANSACTION_TOTAL_COUNT).getValue());
  }

}
