/**
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

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.metrics.MetricsTestUtils.verifyMetrics;

@Category(MetastoreUnitTest.class)
public class TestRetryingHMSHandlerMetrics {
  private MetricRegistry metrics;
  protected Configuration conf;
  private IHMSHandler hmsHandler;

  @Before
  public void setup() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);

    MetaStoreTestUtils.setConfForStandloneMode(conf);

    hmsHandler = RetryingHMSHandler.getProxy(conf, new HMSHandler("test-hms-handler", conf), true);
    metrics = Metrics.getRegistry();
  }

  @Test
  public void testGetDatabasesCount() throws Exception {
    hmsHandler.get_databases("test_db");

    verifyMetrics(metrics, MetricsTestUtils.TIMER, "api_get_databases", 1);
    verifyMetrics(metrics, MetricsTestUtils.TIMER, "api_RetryingHMSHandler.get_databases", 1);
  }
}
