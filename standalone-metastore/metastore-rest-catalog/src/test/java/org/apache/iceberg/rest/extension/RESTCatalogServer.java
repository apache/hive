/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest.extension;

import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

  private Path warehouseDir;
  private int hmsPort = -1;
  private int restPort = -1;

  private static int createMetastoreServerWithRESTCatalog(int restPort, Configuration conf) throws Exception {
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT, restPort);
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf,
        true, false, false, false);
  }

  void start(Configuration conf) throws Exception {
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    // Avoid reusing the JVM-level caching across Hive Metastore servers
    conf.set("metastore.in.test.iceberg.catalog.servlet.id", UUID.randomUUID().toString());

    String uniqueTestKey = String.format("RESTCatalogServer_%s", UUID.randomUUID());
    warehouseDir = Path.of(MetaStoreTestUtils.getTestWarehouseDir(uniqueTestKey));
    String jdbcUrl = String.format("jdbc:derby:memory:%s;create=true",
        warehouseDir.resolve("metastore_db").toAbsolutePath());
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, jdbcUrl);
    String managedPath = warehouseDir.resolve("managed").toAbsolutePath().toString();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE, managedPath);
    String externalPath = warehouseDir.resolve("external").toAbsolutePath().toString();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL, externalPath);
    conf.set(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname, externalPath);

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.SCHEMA_INFO_CLASS,
        RESTCatalogSchemaInfo.class.getCanonicalName());

    for (int i = 0; i < MetaStoreTestUtils.RETRY_COUNT; i++) {
      try {
        restPort = MetaStoreTestUtils.findFreePort();
        hmsPort = createMetastoreServerWithRESTCatalog(restPort, conf);
        break;
      } catch (Exception e) {
        LOG.error("Failed to start HMS with Iceberg REST Catalog(port={})", restPort, e);
        if (i == MetaStoreTestUtils.RETRY_COUNT - 1) {
          throw e;
        }
      }
    }

    LOG.info("Starting HMS(port={}) with Iceberg REST Catalog(port={})", hmsPort, restPort);
  }

  void stop() {
    MetaStoreTestUtils.close(hmsPort);
  }

  Path getWarehouseDir() {
    return warehouseDir;
  }

  String getRestEndpoint() {
    return String.format("http://localhost:%d/iceberg", restPort);
  }
}
