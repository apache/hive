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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactoryImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.iceberg.rest.HMSPrivilegeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Implements {@link HMSPrivilegeHelper} by calling Ranger's {@code showPrivileges} API directly,
 * without routing through the Hive metastore Thrift endpoint.
 *
 * <h3>Initialization</h3>
 * {@link #create(Configuration)} reads {@code HIVE_AUTHORIZATION_MANAGER} from the supplied
 * configuration to locate a {@link HiveAuthorizerFactory}. Three outcomes are possible:
 * <ul>
 *   <li><b>Factory found</b> – a fully wired {@code RangerPrivilegeHelper} is returned and
 *       {@link #isAvailable()} returns {@code true}.</li>
 *   <li><b>No factory configured</b> – a pass-through helper that grants {@code READ_WRITE}
 *       on every object is returned and {@link #isAvailable()} returns {@code false}.</li>
 *   <li><b>Initialization exception</b> – a {@code RangerPrivilegeHelper} with a {@code null}
 *       authorizer is returned; {@link #isAvailable()} returns {@code false} and every check
 *       returns {@link HMSPrivilegeHelper.AccessLevel#NONE}.</li>
 * </ul>
 *
 * <h3>Privilege mapping</h3>
 * Both {@link #getAccessLevel} and {@link #getNamespaceAccessLevel} delegate to the shared
 * {@link #queryPrivileges} method. The algorithm scans the privilege list returned by
 * {@link HiveAuthorizer#showPrivileges}:
 * <ol>
 *   <li>If any privilege is in the object's <em>write set</em>, return {@code READ_WRITE}
 *       immediately (short-circuit).</li>
 *   <li>If any privilege is in the <em>read set</em>, set a read flag.</li>
 *   <li>After all privileges are scanned, return {@code READ_ONLY} if the read flag is set,
 *       otherwise {@code NONE}.</li>
 * </ol>
 * Ranger sometimes appends qualifiers to privilege names (e.g. {@code "SELECT(ACCESS_CONDITIONAL)"}).
 * These are stripped before the comparison.
 *
 * <p>Privilege sets (aligned with Ranger's Hive access-type model):</p>
 * <ul>
 *   <li><b>Read</b> (shared) – {@code SELECT} (SQL standard), {@code READ} (Ranger data-plane alias)</li>
 *   <li><b>Table / view write</b> (DML / data-plane) – {@code UPDATE} (Ranger's single
 *       data-mutation access type, covering insert/update/delete), {@code WRITE} (Ranger
 *       data-plane write alias), {@code ALL}. DDL privileges ({@code ALTER}, {@code DROP}) are
 *       authorized at the namespace level, not per-table.</li>
 *   <li><b>Namespace (database) write</b> (DDL) – {@code CREATE}, {@code ALTER}, {@code DROP},
 *       {@code ALL}. Data-plane grants ({@code UPDATE}, {@code WRITE}) are table-scoped and do
 *       not imply DDL access on the namespace.</li>
 * </ul>
 *
 * <p>Any exception thrown by the Ranger API is caught and logged; the result is {@code NONE}.</p>
 */
public class RangerPrivilegeHelper implements HMSPrivilegeHelper {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPrivilegeHelper.class);

  // Privileges that imply read access on any object type.
  // SELECT is the SQL-standard read privilege; READ is Ranger's data-plane alias.
  private static final Set<String> READ_PRIVILEGES = Set.of("SELECT", "READ");
  // Privileges that grant READ_WRITE at the table / view level (DML / data-plane only).
  // Ranger's Hive access-type model uses UPDATE to cover all data mutation (insert/update/delete);
  // there are no separate INSERT or DELETE access types in Ranger. WRITE is Ranger's data-plane
  // write alias (parallel to READ). DDL privileges (ALTER, DROP) are authorized at the namespace
  // level, not per-table, so they are intentionally absent here.
  private static final Set<String> TABLE_WRITE_PRIVILEGES =
      Set.of("UPDATE", "WRITE", "ALL");
  // Privileges that grant READ_WRITE at the namespace (database) level (DDL).
  // CREATE/ALTER/DROP are the DDL operations authorized at the database level (including for the
  // tables it contains). UPDATE/WRITE are table-scoped data-plane grants and do not belong here.
  private static final Set<String> NAMESPACE_WRITE_PRIVILEGES =
      Set.of("CREATE", "ALTER", "DROP", "ALL");

  // The Ranger authorizer instance, or null if initialization failed.
  private final HiveAuthorizer authorizer;

  protected RangerPrivilegeHelper(HiveAuthorizer auth) {
    this.authorizer = auth;
  }

  /**
   * Creates a new {@code RangerPrivilegeHelper} from the supplied configuration.
   *
   * <p>If the configuration does not specify a {@code HiveAuthorizerFactory}, a pass-through
   * helper is returned that grants {@code READ_WRITE} on every object. If an exception occurs
   * during initialization, a helper with a {@code null} authorizer is returned; every check
   * will return {@link HMSPrivilegeHelper.AccessLevel#NONE}.
   *
   * @param conf the Hive configuration to read
   * @return a new {@code RangerPrivilegeHelper}
   */
  public static HMSPrivilegeHelper create(Configuration conf) {
    HiveAuthorizer auth = null;
    HiveConf hiveConf = (conf instanceof HiveConf) ? (HiveConf) conf : new HiveConf(conf, RangerPrivilegeHelper.class);
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      LOG.warn("RangerPrivilegeHelper: authorization is disabled ({}=false), all access granted.",
          HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED.varname);
      return new HMSPrivilegeHelper() {
        @Override
        public AccessLevel getAccessLevel(String dbName, String tableName, String userName) {
          return AccessLevel.READ_WRITE;
        }
        @Override
        public AccessLevel getNamespaceAccessLevel(String dbName, String userName) {
          return AccessLevel.READ_WRITE;
        }
      };
    }
    try {
      HiveAuthorizerFactory authorizerFactory = HiveUtils.getAuthorizerFactory(hiveConf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER);
      if (authorizerFactory != null) {
        LOG.debug("Using HiveAuthorizerFactory: {}", authorizerFactory.getClass().getName());

        HiveAuthzSessionContext.Builder ctxBuilder = new HiveAuthzSessionContext.Builder();
        ctxBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.OTHER);
        ctxBuilder.setSessionString("IcebergRESTCatalog");
        HiveAuthzSessionContext sessionContext = ctxBuilder.build();

        HiveAuthenticationProvider authenticator = HiveUtils.getAuthenticator(
          hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHENTICATOR_MANAGER);
        if (authenticator != null) {
          authenticator.setConf(hiveConf);
        }

        HiveMetastoreClientFactoryImpl clientFactory = new HiveMetastoreClientFactoryImpl(hiveConf);
        auth = authorizerFactory.createHiveAuthorizer(
          clientFactory, hiveConf, authenticator, sessionContext);
        LOG.info("RangerPrivilegeHelper initialized with authorizer: {}", auth.getClass().getName());
      } else {
        LOG.warn("RangerPrivilegeHelper: no authorizer factory found, all access granted. " +
            "Check your Hive configuration for {}",
          HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname);
        return new HMSPrivilegeHelper() {
          @Override
          public AccessLevel getAccessLevel(String dbName, String tableName, String userName) {
            return AccessLevel.READ_WRITE;
          }
          @Override
          public AccessLevel getNamespaceAccessLevel(String dbName, String userName) {
            return AccessLevel.READ_WRITE;
          }
        };
      }
    } catch (Exception e) {
      LOG.warn("RangerPrivilegeHelper: failed to initialize authorizer", e);
    }
    return new RangerPrivilegeHelper(auth);
  }

  @Override
  public boolean isAvailable() {
    return authorizer != null;
  }

  /**
   * Returns the access level {@code userName} has on the table or view {@code dbName.tableName}.
   */
  @Override
  public AccessLevel getAccessLevel(String dbName, String tableName, String userName) {
    return queryPrivileges(
        userName,
        new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, null, dbName, tableName),
        TABLE_WRITE_PRIVILEGES);
  }

  /**
   * Returns the access level {@code userName} has on the namespace (database) {@code dbName}.
   */
  @Override
  public AccessLevel getNamespaceAccessLevel(String dbName, String userName) {
    return queryPrivileges(
        userName,
        new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, null, dbName, (String) null),
        NAMESPACE_WRITE_PRIVILEGES);
  }

  /**
   * Core privilege evaluation: calls {@link HiveAuthorizer#showPrivileges} and maps the
   * resulting list to an {@link AccessLevel} using the supplied {@code writePrivileges} set.
   *
   * <p>Returns {@link AccessLevel#NONE} immediately if the authorizer is {@code null}.
   * Any exception from the Ranger API is caught, logged, and treated as {@code NONE}.
   *
   * @param userName        the short user name to evaluate
   * @param privObj         the object to check (table, view, or database)
   * @param writePrivileges upper-cased privilege names (object-type-specific) that grant
   *                        {@code READ_WRITE}; read-only access is determined by
   *                        {@link #READ_PRIVILEGES}
   * @return the resolved access level
   */
  private AccessLevel queryPrivileges(String userName, HivePrivilegeObject privObj,
      Set<String> writePrivileges) {
    if (authorizer == null) {
      LOG.debug("No authorizer available, defaulting to NONE for {} user={}", privObj, userName);
      return AccessLevel.NONE;
    }
    try {
      HivePrincipal principal = new HivePrincipal(userName, HivePrincipal.HivePrincipalType.USER);
      List<HivePrivilegeInfo> privileges = authorizer.showPrivileges(principal, privObj);
      if (privileges == null || privileges.isEmpty()) {
        LOG.debug("No privileges found for user {} on {}", userName, privObj);
        return AccessLevel.NONE;
      }
      boolean hasRead = false;
      for (HivePrivilegeInfo info : privileges) {
        String raw = info.getPrivilege().getName();
        // Ranger sometimes appends qualifiers: "SELECT(ACCESS_CONDITIONAL)" or "SELECT something".
        int sep = raw.indexOf('(');
        if (sep < 0) {
          sep = raw.indexOf(' ');
        }
        String privName = (sep < 0 ? raw : raw.substring(0, sep)).trim().toUpperCase();
        LOG.debug("Privilege {} for user {} on {}", privName, userName, privObj);
        if (writePrivileges.contains(privName)) {
          return AccessLevel.READ_WRITE;
        }
        if (READ_PRIVILEGES.contains(privName)) {
          hasRead = true;
        }
      }
      return hasRead ? AccessLevel.READ_ONLY : AccessLevel.NONE;
    } catch (Exception e) {
      LOG.warn("Failed to check privileges for user {} on {}", userName, privObj, e);
      return AccessLevel.NONE;
    }
  }
}
