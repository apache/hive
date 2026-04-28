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

package org.apache.iceberg.rest;

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the configured HiveAuthorizerFactory to enforce write-access checks at the REST
 * catalog layer before any underlying catalog operation is attempted. When no authorizer is
 * available the helper logs a warning and fails open (allows the request through).
 */
class PrivilegeHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeHelper.class);

  private final HiveAuthorizer authorizer;
  private final HiveAuthzContext authzContext;

  PrivilegeHelper(Configuration conf) {
    HiveAuthorizer auth = null;
    HiveConf hiveConf = (conf instanceof HiveConf) ? (HiveConf) conf
        : new HiveConf(conf, PrivilegeHelper.class);
    try {
      HiveAuthorizerFactory factory = HiveUtils.getAuthorizerFactory(
          hiveConf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);
      if (factory != null) {
        HiveAuthzSessionContext.Builder authzContextBuilder = new HiveAuthzSessionContext.Builder();
        authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVEMETASTORE);
        authzContextBuilder.setSessionString("REST_CATALOG_PrivilegeHelper");
        HiveAuthzSessionContext sessionContext = authzContextBuilder.build();

        HiveAuthenticationProvider authenticator = HiveUtils.getAuthenticator(
            hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        if (authenticator != null) {
          authenticator.setConf(hiveConf);
        }

        auth = factory.createHiveAuthorizer(
            new HiveMetastoreClientFactoryImpl(hiveConf), hiveConf, authenticator, sessionContext);
        LOG.info("PrivilegeHelper initialized with authorizer: {}", auth.getClass().getName());
      } else {
        LOG.warn("PrivilegeHelper: no authorizer factory configured; write-access enforcement is disabled");
      }
    } catch (Exception e) {
      LOG.error("PrivilegeHelper: failed to initialize authorizer; write-access enforcement is disabled", e);
    }
    this.authorizer = auth;
    HiveAuthzContext.Builder builder = new HiveAuthzContext.Builder();
    builder.setCommandString("REST catalog write operation");
    this.authzContext = builder.build();
  }

  boolean isAvailable() {
    return authorizer != null;
  }

  /**
   * Throws RestCatalogWriteDeniedException if the current user lacks write access on the
   * given table. Internally calls checkPrivileges(QUERY, [], [table], ctx) which, for
   * SQLStdHiveAuthorizerFactory, requires at least INSERT privilege on the table.
   */
  void requireWriteAccess(String dbName, String tableName) {
    if (!isAvailable()) {
      return;
    }
    HivePrivilegeObject output =
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tableName);
    try {
      authorizer.checkPrivileges(
          HiveOperationType.QUERY,
          Collections.emptyList(),
          Collections.singletonList(output),
          authzContext);
    } catch (HiveAccessControlException e) {
      LOG.warn("Write access denied on table {}.{}: {}", dbName, tableName, e.getMessage());
      throw new RestCatalogWriteDeniedException(
          String.format("Write access denied on table %s.%s", dbName, tableName));
    } catch (Exception e) {
      LOG.error("Error checking write access for table {}.{}", dbName, tableName, e);
      throw new RestCatalogWriteDeniedException(
          String.format("Write access check failed for table %s.%s", dbName, tableName));
    }
  }

  /**
   * Throws RestCatalogWriteDeniedException if the current user cannot create tables in
   * the given database. Internally calls checkPrivileges(CREATETABLE, [], [db], ctx).
   */
  void requireCreateAccess(String dbName) {
    if (!isAvailable()) {
      return;
    }
    HivePrivilegeObject dbOutput =
        new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, dbName, null);
    try {
      authorizer.checkPrivileges(
          HiveOperationType.CREATETABLE,
          Collections.emptyList(),
          Collections.singletonList(dbOutput),
          authzContext);
    } catch (HiveAccessControlException e) {
      LOG.warn("Create access denied on namespace {}: {}", dbName, e.getMessage());
      throw new RestCatalogWriteDeniedException(
          String.format("Write access denied on namespace %s", dbName));
    } catch (Exception e) {
      LOG.error("Error checking create access for namespace {}", dbName, e);
      throw new RestCatalogWriteDeniedException(
          String.format("Write access check failed for namespace %s", dbName));
    }
  }
}
