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

package org.apache.iceberg.hive;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveHadoopUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveHadoopUtil.class);

  /**
   * Whether the HMS owner of a table or view created through this catalog may be taken from the client-supplied
   * property {@link HiveCatalog#HMS_TABLE_OWNER} instead of the authenticated caller. On by default, as engines
   * embedding the catalog (Spark, Flink, ...) rely on it. The HMS REST catalog always sets it, from
   * {@code metastore.iceberg.catalog.client-supplied-owner.enabled} (off by default).
   */
  public static final String CLIENT_SUPPLIED_OWNER_ENABLED = "iceberg.hive.client-supplied-owner.enabled";
  public static final boolean CLIENT_SUPPLIED_OWNER_ENABLED_DEFAULT = true;

  private HiveHadoopUtil() {
  }

  public static String currentUser() {
    String username = null;
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Failed to get Hadoop user", e);
    }

    if (username != null) {
      return username;
    } else {
      LOG.warn("Hadoop user is null, defaulting to user.name");
      return System.getProperty("user.name");
    }
  }

  /**
   * Resolves the owner to record in HMS for a table or view being created: the client-supplied
   * {@link HiveCatalog#HMS_TABLE_OWNER} when {@link #CLIENT_SUPPLIED_OWNER_ENABLED} allows it, the authenticated
   * caller otherwise.
   *
   * @param conf the catalog configuration, may be null
   * @param properties the table or view properties, may be null
   * @return the owner name
   */
  public static String resolveTableOwner(Configuration conf, Map<String, String> properties) {
    String currentUser = currentUser();
    String requestedOwner = properties == null ? null : properties.get(HiveCatalog.HMS_TABLE_OWNER);
    if (requestedOwner == null || requestedOwner.equals(currentUser)) {
      return currentUser;
    }
    boolean enabled = conf == null ?
        CLIENT_SUPPLIED_OWNER_ENABLED_DEFAULT :
        conf.getBoolean(CLIENT_SUPPLIED_OWNER_ENABLED, CLIENT_SUPPLIED_OWNER_ENABLED_DEFAULT);
    if (enabled) {
      return requestedOwner;
    }
    // Debug level: expected on every such request when the setting is off
    LOG.debug("Ignoring client-supplied {}; recording the authenticated caller {} as owner",
        HiveCatalog.HMS_TABLE_OWNER, currentUser);
    return currentUser;
  }
}
