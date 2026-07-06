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

package org.apache.hive.service.cli.session;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves server-side LLAP cluster routing rules and applies the target cluster's
 * namespace configs to the session. No-op when routing rules are not configured.
 */
public final class LlapClusterRouter {

  private static final Logger LOG = LoggerFactory.getLogger(LlapClusterRouter.class);
  private static final String CLUSTER_PREFIX = "hive.llap.cluster.";
  private static final String SESSIONS_NS_SUFFIX = ".sessions.namespace";
  private static final String REGISTRY_NS_SUFFIX = ".registry.namespace";
  private static final String SERVICE_HOSTS_SUFFIX = ".service.hosts";

  private LlapClusterRouter() {
  }

  /**
   * Resolves routing rules and applies LLAP cluster configs to the session.
   * No-op if routing rules are not configured or empty.
   */
  public static void applyRouting(HiveConf sessionConf, String username) {
    String rules = HiveConf.getVar(sessionConf, HiveConf.ConfVars.LLAP_CLUSTER_ROUTING_RULES);
    if (rules == null || rules.isEmpty()) {
      return;
    }

    String cluster = resolveCluster(rules, username);
    if (cluster == null) {
      return;
    }

    // Cast to Configuration to avoid HiveConf warnings for non-ConfVars keys
    Configuration conf = sessionConf;
    String sessionsNs = conf.get(CLUSTER_PREFIX + cluster + SESSIONS_NS_SUFFIX);
    String registryNs = conf.get(CLUSTER_PREFIX + cluster + REGISTRY_NS_SUFFIX);
    String serviceHosts = conf.get(CLUSTER_PREFIX + cluster + SERVICE_HOSTS_SUFFIX);
    if (serviceHosts == null) {
      serviceHosts = "@" + cluster;
    }

    if (sessionsNs != null) {
      sessionConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE, sessionsNs);
    }
    if (registryNs != null) {
      sessionConf.set("tez.am.registry.namespace", registryNs);
    }
    sessionConf.setVar(HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, serviceHosts);
    LOG.info("Routed user {} to LLAP cluster '{}' (sessions.namespace={}, registry.namespace={}, "
        + "service.hosts={})", username, cluster, sessionsNs, registryNs, serviceHosts);
  }

  static String resolveCluster(String rules, String username) {
    String userMatch = null;
    String groupMatch = null;
    String defaultCluster = null;

    for (String rule : rules.split(",")) {
      rule = rule.trim();
      if (rule.startsWith("user:")) {
        String[] kv = rule.substring(5).split("=", 2);
        if (kv.length == 2 && kv[0].equals(username)) {
          userMatch = kv[1];
        }
      } else if (rule.startsWith("group:")) {
        String[] kv = rule.substring(6).split("=", 2);
        if (kv.length == 2 && groupMatch == null) {
          try {
            String[] groups = UserGroupInformation.createRemoteUser(username).getGroupNames();
            for (String g : groups) {
              if (g.equals(kv[0])) {
                groupMatch = kv[1];
                break;
              }
            }
          } catch (Exception e) {
            LOG.debug("Failed to resolve groups for user {}", username, e);
          }
        }
      } else if (rule.startsWith("default=")) {
        defaultCluster = rule.substring(8);
      }
    }

    if (userMatch != null) {
      return userMatch;
    }
    if (groupMatch != null) {
      return groupMatch;
    }
    return defaultCluster;
  }
}
