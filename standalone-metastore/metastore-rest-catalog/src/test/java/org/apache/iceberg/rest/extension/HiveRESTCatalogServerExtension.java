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
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveRESTCatalogServerExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback {
  private static final Logger LOG = LoggerFactory.getLogger(HiveRESTCatalogServerExtension.class);

  private final Configuration conf;
  private final JwksServer jwksServer;
  private final RESTCatalogServer restCatalogServer;

  private HiveRESTCatalogServerExtension(AuthType authType, Configuration configuration, Class<? extends MetaStoreSchemaInfo> schemaInfoClass) {
    this.conf = configuration == null ? MetastoreConf.newMetastoreConf() : configuration;
    MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, authType.name());
    if (authType == AuthType.JWT) {
      jwksServer = new JwksServer();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "jwt");
      MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
          String.format("http://localhost:%d/jwks", jwksServer.getPort()));
    } else {
      jwksServer = null;
    }
    restCatalogServer = new RESTCatalogServer();
    if (schemaInfoClass != null) {
      restCatalogServer.setSchemaInfoClass(schemaInfoClass);
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (jwksServer != null) {
      jwksServer.start();
    }
    restCatalogServer.start(conf);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws IOException {
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

  public static class Builder {
    private final AuthType authType;
    private Class<? extends MetaStoreSchemaInfo> schemaInfoClass;

    public Builder addMetaStoreSchemaClassName(Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass) {
      this.schemaInfoClass = metaStoreSchemaClass;
      return this;
    }

    private Builder(AuthType authType) {
      this.authType = authType;
    }

    public HiveRESTCatalogServerExtension build() {
      return new HiveRESTCatalogServerExtension(authType, null, null);
    }

    public HiveRESTCatalogServerExtension build(Configuration configuration) {
      return new HiveRESTCatalogServerExtension(authType, configuration, schemaInfoClass );
    }
  }

  public static Builder builder(AuthType authType) {
    return new Builder(authType);
  }
}
