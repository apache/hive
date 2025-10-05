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

package org.apache.iceberg.rest.extension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
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
  private final OAuth2AuthorizationServer authorizationServer;
  private final RESTCatalogServer restCatalogServer;

  private HiveRESTCatalogServerExtension(AuthType authType, Class<? extends MetaStoreSchemaInfo> schemaInfoClass,
      Map<String, String> configurations) {
    this.conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, authType.name());
    if (authType == AuthType.JWT) {
      jwksServer = new JwksServer();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "jwt");
      MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
          String.format("http://localhost:%d/jwks", jwksServer.getPort()));
    } else {
      jwksServer = null;
    }
    if (authType == AuthType.OAUTH2) {
      authorizationServer = new OAuth2AuthorizationServer();
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH, "oauth2");
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_CLIENT_ID, OAuth2AuthorizationServer.HMS_ID);
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_CLIENT_SECRET,
          OAuth2AuthorizationServer.HMS_SECRET);
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_AUDIENCE, OAuth2AuthorizationServer.HMS_ID);
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_PRINCIPAL_MAPPER_REGEX_FIELD, "email");
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_PRINCIPAL_MAPPER_REGEX_PATTERN,
          "(.*)@example.com");
    } else {
      authorizationServer = null;
    }
    configurations.forEach(conf::set);
    restCatalogServer = new RESTCatalogServer();
    if (schemaInfoClass != null) {
      restCatalogServer.setSchemaInfoClass(schemaInfoClass);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (jwksServer != null) {
      jwksServer.start();
    }
    if (authorizationServer != null) {
      authorizationServer.start();
      LOG.info("An authorization server {} started", authorizationServer.getIssuer());
      MetastoreConf.setVar(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_ISSUER, authorizationServer.getIssuer());
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
    if (authorizationServer != null) {
      authorizationServer.stop();
    }
    restCatalogServer.stop();
  }

  public String getRestEndpoint() {
    return restCatalogServer.getRestEndpoint();
  }

  public String getOAuth2TokenEndpoint() {
    return authorizationServer.getTokenEndpoint();
  }

  public String getOAuth2ClientCredential() {
    return authorizationServer.getClientCredential();
  }

  public String getOAuth2AccessToken() {
    return authorizationServer.getAccessToken();
  }

  public static class Builder {
    private final AuthType authType;
    private Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass;
    private final Map<String, String> configurations = new HashMap<>();

    private Builder(AuthType authType) {
      this.authType = authType;
    }

    public Builder addMetaStoreSchemaClassName(Class<? extends MetaStoreSchemaInfo> metaStoreSchemaClass) {
      this.metaStoreSchemaClass = metaStoreSchemaClass;
      return this;
    }

    public Builder configure(String key, String value) {
      configurations.put(key, value);
      return this;
    }

    public HiveRESTCatalogServerExtension build() {
      return new HiveRESTCatalogServerExtension(authType, metaStoreSchemaClass, configurations);
    }
  }

  public static Builder builder(AuthType authType) {
    return new Builder(authType);
  }
}
