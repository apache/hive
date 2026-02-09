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
package org.apache.iceberg.rest;

import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;

class HiveIcebergRESTCatalogClient {
  private static final Namespace DEFAULT_NS = Namespace.of("default");

  private final RESTCatalog restCatalog;
  private final Configuration conf;

  HiveIcebergRESTCatalogClient() throws Exception {
    this(Collections.emptyMap());
  }

  HiveIcebergRESTCatalogClient(Map<String, String> additionalProperties) throws Exception {
    var properties = new HashMap<>(additionalProperties);
    properties.put(CatalogProperties.URI, "http://localhost:9001/iceberg");
    restCatalog = RCKUtils.initCatalogClient(properties);
    conf = new Configuration(false);
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.endpoint", "http://localhost:9878");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.change.detection.version.required", "false");
    conf.set("fs.s3a.change.detection.mode", "none");
    conf.set("fs.s3a.access.key", "hadoop");
    conf.set("fs.s3a.secret.key", "dummy");
    conf.set("hadoop.tmp.dir", Files.createTempDirectory("hive-docker-test").toString());
  }

  RESTCatalog getRestCatalog() {
    return restCatalog;
  }

  void cleanupWarehouse() throws Exception {
    restCatalog.listNamespaces().stream().filter(namespace -> !DEFAULT_NS.equals(namespace)).forEach(namespace -> {
      restCatalog.listTables(namespace).forEach(restCatalog::dropTable);
      restCatalog.listViews(namespace).forEach(restCatalog::dropView);
      restCatalog.dropNamespace(namespace);
    });
    // Delete the DB directories explicitly since HiveCatalog#dropNamespace does not wipe them out
    var warehouseRoot = new Path("s3a://test/test-warehouse");
    var fs = warehouseRoot.getFileSystem(conf);
    if (!fs.exists(warehouseRoot)) {
      return;
    }
    for (FileStatus fileStatus : fs.listStatus(warehouseRoot)) {
      fs.delete(fileStatus.getPath(), true);
    }
  }

  void close() throws Exception {
    cleanupWarehouse();
    restCatalog.close();
  }
}
