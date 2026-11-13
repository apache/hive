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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest.standalone.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.extension.RESTCatalogSchemaInfo;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

@Category(MetastoreCheckinTest.class)
@DisplayName("HMS readiness health indicator")
class TestHMSReadinessHealthIndicator {

  @Test
  @DisplayName("Down without details when Thrift URIs are not configured")
  void downWhenNotConfigured() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "");

    Health health = new HMSReadinessHealthIndicator(conf).health();

    assertEquals(Status.DOWN, health.getStatus());
    assertTrue(health.getDetails().isEmpty(), "unexpected details: " + health.getDetails());
  }

  @Test
  @DisplayName("Down without details when HMS is unreachable")
  void downWhenUnreachable() throws Exception {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 1);
    MetastoreConf.setTimeVar(conf, ConfVars.CLIENT_CONNECT_RETRY_DELAY, 0, TimeUnit.SECONDS);

    Health health = new HMSReadinessHealthIndicator(conf).health();

    assertEquals(Status.DOWN, health.getStatus());
    assertTrue(health.getDetails().isEmpty(), "unexpected details: " + health.getDetails());
  }

  @Test
  @DisplayName("Up without details when HMS is reachable")
  void upWhenReachable(@TempDir Path warehouse) throws Exception {
    System.setProperty("derby.stream.error.file", warehouse.resolve("derby.log").toString());
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setVar(conf, ConfVars.SCHEMA_INFO_CLASS, RESTCatalogSchemaInfo.class.getCanonicalName());
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, warehouse.resolve("managed").toString());
    MetastoreConf.setVar(conf, ConfVars.WAREHOUSE_EXTERNAL, warehouse.resolve("external").toString());
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(conf);
    try {
      Health health = new HMSReadinessHealthIndicator(conf).health();

      assertEquals(Status.UP, health.getStatus());
      assertTrue(health.getDetails().isEmpty(), "unexpected details: " + health.getDetails());
    } finally {
      MetaStoreTestUtils.close(port);
    }
  }
}
