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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveRESTCatalogServerExtension implements BeforeAllCallback, AfterAllCallback {
  private static final Logger LOG = LoggerFactory.getLogger(HiveRESTCatalogServerExtension.class);

  private final Configuration conf;
  private final JwksServer jwksServer;
  private final RESTCatalogServer restCatalogServer;

  public HiveRESTCatalogServerExtension(boolean jwtEnabled) {
    this.conf = MetastoreConf.newMetastoreConf();
    if (jwtEnabled) {
      jwksServer = new JwksServer();
      MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_AUTH, "jwt");
      MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
          String.format("http://localhost:%d/jwks", jwksServer.getPort()));
    } else {
      jwksServer = null;
      MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_AUTH, "simple");
    }
    restCatalogServer = new RESTCatalogServer();
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (jwksServer != null) {
      jwksServer.start();
    }
    restCatalogServer.start(conf);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (jwksServer != null) {
      jwksServer.stop();
    }
    restCatalogServer.stop();
  }

  public String getRestEndpoint() {
    return restCatalogServer.getRestEndpoint();
  }

  public void reset() throws IOException {
    try (Stream<Path> children = Files.list(restCatalogServer.getWarehouseDir())) {
      children
          .filter(path -> !path.getFileName().toString().equals("derby.log"))
          .filter(path -> !path.getFileName().toString().equals("metastore_db"))
          .forEach(path -> {
            try {
              if (Files.isDirectory(path)) {
                FileUtils.deleteDirectory(path.toFile());
              } else {
                Files.delete(path);
              }
            } catch (IOException e) {
              LOG.error("Failed to delete path: {}", path, e);
            }
          });
    }
  }

  public static class Builder {
    private boolean jwtEnabled = false;

    private Builder() {
    }

    public Builder jwt() {
      jwtEnabled = true;
      return this;
    }

    public HiveRESTCatalogServerExtension build() {
      return new HiveRESTCatalogServerExtension(jwtEnabled);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
