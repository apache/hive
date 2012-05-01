/**
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

package org.apache.hadoop.hive.ql.hooks.conf;

/**
 * Hive Configuration needed for testing various hooks
 */
public class FBHiveConf {
  public final static String CONNECTION_FACTORY = "fbhive.urlfactory";

  // StartFinishHook.java
  public final static String STARTFINISH_CONNECTION_FACTORY = "fbhive.startfinishhook.urlfactory";
  public final static String STARTFINISH_HOST_DATABASE_VAR_NAME = "fbhive.startfinish.mysql";
  public final static String STARTFINISH_MYSQL_TIER_VAR_NAME = "fbhive.startfinish.mysql.tier";

  // AuditJoinHook.java and AuditLocalModeHook.java
  public final static String AUDIT_CONNECTION_FACTORY = "fbhive.audit.urlfactory";
  public final static String AUDIT_HOST_DATABASE_VAR_NAME = "fbhive.audit.mysql";
  public final static String AUDIT_MYSQL_TIER_VAR_NAME = "fbhive.audit.mysql.tier";

  // BaseReplicationHook.java
  public final static String REPLICATION_CONNECTION_FACTORY = "fbhive.replication.urlfactory";
  public final static String REPLICATION_HOST_DATABASE_VAR_NAME = "fbhive.replication.mysql";
  public final static String REPLICATION_MYSQL_TIER_VAR_NAME = "fbhive.replication.mysql.tier";

  // JobStatsHook.java
  public final static String JOBSTATS_CONNECTION_FACTORY = "fbhive.jobstats.urlfactory";
  public final static String JOBSTATS_HOST_DATABASE_VAR_NAME = "fbhive.jobstats.mysql";
  public final static String JOBSTATS_MYSQL_TIER_VAR_NAME = "fbhive.jobstats.mysql.tier";

  // Lineage.java
  public final static String LINEAGE_CONNECTION_FACTORY = "fbhive.lineage.urlfactory";
  public final static String LINEAGE_HOST_DATABASE_VAR_NAME = "fbhive.lineage.mysql";
  public final static String LINEAGE_MYSQL_TIER_VAR_NAME = "fbhive.lineage.mysql.tier";

  // QueryPlanHook.java
  public final static String QUERYPLAN_CONNECTION_FACTORY = "fbhive.queryplan.urlfactory";
  public final static String QUERYPLAN_HOST_DATABASE_VAR_NAME = "fbhive.queryplan.mysql";
  public final static String QUERYPLAN_MYSQL_TIER_VAR_NAME = "fbhive.queryplan.mysql.tier";

  // ExternalInputsHook.java
  public final static String METASTORE_CONNECTION_FACTORY = "fbhive.metastore.urlfactory";
  public final static String METASTORE_HOST_DATABASE_VAR_NAME = "fbhive.metastore.mysql";
  public final static String METASTORE_MYSQL_TIER_VAR_NAME = "fbhive.metastore.smc.tier";

  // SMCStatsDBHook.java
  public final static String STATS_CONNECTION_FACTORY = "fbhive.stats.urlfactory";
  public final static String STATS_HOST_DATABASE_VAR_NAME = "fbhive.stats.mysql";
  public final static String STATS_MYSQL_TIER_VAR_NAME = "fbhive.stats.mysql.tier";

  // QueryDroppedPartitionsHook.java
  public final static String QUERYDROPPED_PARTITIONS_CONNECTION_FACTORY =
    "fbhive.querydropped.partitions.urlfactory";
  public final static String QUERYDROPPED_PARTITIONS_HOST_DATABASE_VAR_NAME =
    "fbhive.querydropped.partitions.mysql";
  public final static String QUERYDROPPED_PARTITIONS_MYSQL_TIER_VAR_NAME =
    "fbhive.querydropped.partitions.mysql.tier";

  // JobTrackerHook.java -- all of them will be set to null
  public final static String JOBTRACKER_CONNECTION_FACTORY = "fbhive.jobtracker.urlfactory";
  public final static String JOBTRACKER_HOST_DATABASE_VAR_NAME = "fbhive.jobtracker.mysql";
  public final static String JOBTRACKER_MYSQL_TIER_VAR_NAME = "fbhive.jobtracker.mysql.tier";

  public final static String ENABLE_PARTIAL_CONCURRENCY = "fbhive.concurrency.percent";
  public final static String NO_RETENTION_WARNING_ONLY = "fbhive.retention.warningOnly";
  public static final String FB_CURRENT_CLUSTER = "fbhive.package.name";
  public static final String HIVE_CONFIG_TIER = "fbhive.config.tier";

  public static final String HIVE_METRICS_PUBLISHER = "hive.metrics.publisher";

  public static final String ENABLE_PARTIAL_CHANGEDFS =
    "fbhive.changde.dfs.percent";
  public static final String SECONDARYMETASTOREWAREHOUSE =
    "fbhive.secondary.metastore.warehouse.dir";

  public static final String ENABLED_CONFIG = "fbhive.smc.config.hook.enabled";

  // used by MysqlSmcHook
  public static final String METASTORE_SMC_URL = "fbhive.smc.url";
  public static final String METASTORE_MYSQL_PROPS =
    "fbhive.metastore.mysql.props";

  // used by CounterMetaStoreEndFunctionListener
  public static final String METASTORE_LISTENER_STATS_MANAGER =
    "fbhive.metastore.listener.statsmanager";

  public static final String FBHIVE_DB_USERNAME =
    "fbhive.db.username";

  public static final String FBHIVE_DB_PASSWORD =
    "fbhive.db.password";

  public static final String FBHIVE_SILVER_DFS_PREFIX =
    "fbhive.silver.dfs.prefix";
  public static final String FBHIVE_SILVER_DFS_PREFIX2 =
    "fbhive.silver.dfs.prefix2";
  public static final String FBHIVE_SILVER_DFS_PREFIX3 =
    "fbhive.silver.dfs.prefix3";
  public static final String FBHIVE_PLATINUM_DFS_PREFIX =
    "fbhive.platinum.dfs.prefix";

  public static final String FBHIVE_BRONZE_JOBTRACKER =
    "fbhive.bronze.jobtracker";
}
