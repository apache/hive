/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.conf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hive.common.util.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * A set of definitions of config values used by the Metastore.  One of the key aims of this
 * class is to provide backwards compatibility with existing Hive configuration keys while
 * allowing the metastore to have its own, Hive independent keys.   For this reason access to the
 * underlying Configuration object should always be done via the static methods provided here
 * rather than directly via {@link Configuration#get(String)} and
 * {@link Configuration#set(String, String)}.  All the methods of this class will handle checking
 * both the MetastoreConf key and the Hive key.  The algorithm is, on reads, to check first the
 * MetastoreConf key, then the Hive key, then return the default if neither are set.  On write
 * the Metastore key only is set.
 *
 * This class does not extend Configuration.  Rather it provides static methods for operating on
 * a Configuration object.  This allows it to work on HiveConf objects, which otherwise would not
 * be the case.
 */
public class MetastoreConf {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreConf.class);
  private static final Pattern TIME_UNIT_SUFFIX = Pattern.compile("([0-9]+)([a-zA-Z]+)");

  @VisibleForTesting
  static final String DEFAULT_STORAGE_SCHEMA_READER_CLASS =
      "org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader";
  @VisibleForTesting
  static final String SERDE_STORAGE_SCHEMA_READER_CLASS =
      "org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader";
  @VisibleForTesting
  static final String HIVE_ALTER_HANDLE_CLASS =
      "org.apache.hadoop.hive.metastore.HiveAlterHandler";
  @VisibleForTesting
  static final String MATERIALZIATIONS_REBUILD_LOCK_CLEANER_TASK_CLASS =
      "org.apache.hadoop.hive.metastore.MaterializationsRebuildLockCleanerTask";
  @VisibleForTesting
  static final String METASTORE_TASK_THREAD_CLASS =
      "org.apache.hadoop.hive.metastore.MetastoreTaskThread";
  @VisibleForTesting
  static final String RUNTIME_STATS_CLEANER_TASK_CLASS =
      "org.apache.hadoop.hive.metastore.RuntimeStatsCleanerTask";
  static final String PARTITION_MANAGEMENT_TASK_CLASS =
    "org.apache.hadoop.hive.metastore.PartitionManagementTask";
  @VisibleForTesting
  static final String EVENT_CLEANER_TASK_CLASS =
      "org.apache.hadoop.hive.metastore.events.EventCleanerTask";
  static final String ACID_METRICS_TASK_CLASS =
      "org.apache.hadoop.hive.metastore.metrics.AcidMetricService";
  static final String ACID_METRICS_LOGGER_CLASS =
      "org.apache.hadoop.hive.metastore.metrics.AcidMetricLogger";
  @VisibleForTesting
  static final String METASTORE_DELEGATION_MANAGER_CLASS =
      "org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager";
  @VisibleForTesting
  static final String ACID_HOUSEKEEPER_SERVICE_CLASS =
      "org.apache.hadoop.hive.metastore.txn.service.AcidHouseKeeperService";
  @VisibleForTesting
  static final String COMPACTION_HOUSEKEEPER_SERVICE_CLASS =
      "org.apache.hadoop.hive.metastore.txn.service.CompactionHouseKeeperService";
  @VisibleForTesting
  static final String ACID_TXN_CLEANER_SERVICE_CLASS =
      "org.apache.hadoop.hive.metastore.txn.service.AcidTxnCleanerService";
  @VisibleForTesting
  static final String ACID_OPEN_TXNS_COUNTER_SERVICE_CLASS =
      "org.apache.hadoop.hive.metastore.txn.service.AcidOpenTxnsCounterService";

  public static final String METASTORE_AUTHENTICATION_LDAP_USERMEMBERSHIPKEY_NAME =
          "metastore.authentication.ldap.userMembershipKey";
  public static final String METASTORE_RETRYING_HANDLER_CLASS =
          "org.apache.hadoop.hive.metastore.RetryingHMSHandler";

  private static final Map<String, ConfVars> metaConfs = new HashMap<>();
  private static volatile URL hiveSiteURL = null;
  private static URL hiveDefaultURL = null;
  private static URL hiveMetastoreSiteURL = null;
  private static URL metastoreSiteURL = null;
  private static AtomicBoolean beenDumped = new AtomicBoolean();

  private static Map<String, ConfVars> keyToVars;

  static {
    keyToVars = new HashMap<>(ConfVars.values().length * 2);
    for (ConfVars var : ConfVars.values()) {
      keyToVars.put(var.varname, var);
      keyToVars.put(var.hiveName, var);
    }
  }

  @VisibleForTesting
  static final String TEST_ENV_WORKAROUND = "metastore.testing.env.workaround.dont.ever.set.this.";

  public static enum StatsUpdateMode {
    NONE, EXISTING, ALL
  }

  private static class TimeValue {
    final long val;
    final TimeUnit unit;

    private TimeValue(long val, TimeUnit unit) {
      this.val = val;
      this.unit = unit;
    }

    @Override
    public String toString() {
      switch (unit) {
      case NANOSECONDS: return Long.toString(val) + "ns";
      case MICROSECONDS: return Long.toString(val) + "us";
      case MILLISECONDS: return Long.toString(val) + "ms";
      case SECONDS: return Long.toString(val) + "s";
      case MINUTES: return Long.toString(val) + "m";
      case HOURS: return Long.toString(val) + "h";
      case DAYS: return Long.toString(val) + "d";
      }
      throw new RuntimeException("Unknown time unit " + unit);
    }
  }

  /**
   * Metastore related options that the db is initialized against. When a conf
   * var in this is list is changed, the metastore instance for the CLI will
   * be recreated so that the change will take effect.
   * TODO - I suspect the vast majority of these don't need to be here.  But it requires testing
   * before just pulling them out.
   */
  @SuppressFBWarnings(value = "MS_MUTABLE_ARRAY")
  public static final MetastoreConf.ConfVars[] metaVars = {
      ConfVars.WAREHOUSE,
      ConfVars.REPLDIR,
      ConfVars.THRIFT_URIS,
      ConfVars.SERVER_PORT,
      ConfVars.THRIFT_BIND_HOST,
      ConfVars.THRIFT_ZOOKEEPER_CLIENT_PORT,
      ConfVars.THRIFT_ZOOKEEPER_NAMESPACE,
      ConfVars.THRIFT_CONNECTION_RETRIES,
      ConfVars.THRIFT_FAILURE_RETRIES,
      ConfVars.CLIENT_CONNECT_RETRY_DELAY,
      ConfVars.CLIENT_SOCKET_TIMEOUT,
      ConfVars.CLIENT_SOCKET_LIFETIME,
      ConfVars.PWD,
      ConfVars.CONNECT_URL_HOOK,
      ConfVars.CONNECT_URL_KEY,
      ConfVars.SERVER_MIN_THREADS,
      ConfVars.SERVER_MAX_THREADS,
      ConfVars.TCP_KEEP_ALIVE,
      ConfVars.KERBEROS_KEYTAB_FILE,
      ConfVars.KERBEROS_PRINCIPAL,
      ConfVars.USE_THRIFT_SASL,
      ConfVars.METASTORE_CLIENT_AUTH_MODE,
      ConfVars.METASTORE_CLIENT_PLAIN_USERNAME,
      ConfVars.CACHE_PINOBJTYPES,
      ConfVars.CONNECTION_POOLING_TYPE,
      ConfVars.VALIDATE_TABLES,
      ConfVars.DATANUCLEUS_INIT_COL_INFO,
      ConfVars.VALIDATE_COLUMNS,
      ConfVars.VALIDATE_CONSTRAINTS,
      ConfVars.STORE_MANAGER_TYPE,
      ConfVars.AUTO_CREATE_ALL,
      ConfVars.DATANUCLEUS_TRANSACTION_ISOLATION,
      ConfVars.DATANUCLEUS_CACHE_LEVEL2,
      ConfVars.DATANUCLEUS_CACHE_LEVEL2_TYPE,
      ConfVars.IDENTIFIER_FACTORY,
      ConfVars.DATANUCLEUS_PLUGIN_REGISTRY_BUNDLE_CHECK,
      ConfVars.AUTHORIZATION_STORAGE_AUTH_CHECKS,
      ConfVars.BATCH_RETRIEVE_MAX,
      ConfVars.EVENT_LISTENERS,
      ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
      ConfVars.EVENT_CLEAN_FREQ,
      ConfVars.EVENT_EXPIRY_DURATION,
      ConfVars.EVENT_MESSAGE_FACTORY,
      ConfVars.FILTER_HOOK,
      ConfVars.RAW_STORE_IMPL,
      ConfVars.END_FUNCTION_LISTENERS,
      ConfVars.PART_INHERIT_TBL_PROPS,
      ConfVars.BATCH_RETRIEVE_OBJECTS_MAX,
      ConfVars.INIT_HOOKS,
      ConfVars.PRE_EVENT_LISTENERS,
      ConfVars.HMS_HANDLER_ATTEMPTS,
      ConfVars.HMS_HANDLER_INTERVAL,
      ConfVars.HMS_HANDLER_FORCE_RELOAD_CONF,
      ConfVars.PARTITION_NAME_WHITELIST_PATTERN,
      ConfVars.ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS,
      ConfVars.USERS_IN_ADMIN_ROLE,
      ConfVars.HIVE_TXN_MANAGER,
      ConfVars.TXN_TIMEOUT,
      ConfVars.TXN_MAX_OPEN_BATCH,
      ConfVars.TXN_RETRYABLE_SQLEX_REGEX,
      ConfVars.STATS_NDV_TUNER,
      ConfVars.STATS_NDV_DENSITY_FUNCTION,
      ConfVars.AGGREGATE_STATS_CACHE_ENABLED,
      ConfVars.AGGREGATE_STATS_CACHE_SIZE,
      ConfVars.AGGREGATE_STATS_CACHE_MAX_PARTITIONS,
      ConfVars.AGGREGATE_STATS_CACHE_FPP,
      ConfVars.AGGREGATE_STATS_CACHE_MAX_VARIANCE,
      ConfVars.AGGREGATE_STATS_CACHE_TTL,
      ConfVars.AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT,
      ConfVars.AGGREGATE_STATS_CACHE_MAX_READER_WAIT,
      ConfVars.AGGREGATE_STATS_CACHE_MAX_FULL,
      ConfVars.AGGREGATE_STATS_CACHE_CLEAN_UNTIL,
      ConfVars.DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES,
      ConfVars.FILE_METADATA_THREADS,
      ConfVars.METASTORE_CLIENT_FILTER_ENABLED,
      ConfVars.METASTORE_SERVER_FILTER_ENABLED,
      ConfVars.METASTORE_PARTITIONS_PARAMETERS_INCLUDE_PATTERN,
      ConfVars.METASTORE_PARTITIONS_PARAMETERS_EXCLUDE_PATTERN
  };

  /**
   * User configurable Metastore vars
   */
  private static final MetastoreConf.ConfVars[] metaConfVars = {
      ConfVars.TRY_DIRECT_SQL,
      ConfVars.TRY_DIRECT_SQL_DDL,
      ConfVars.CLIENT_SOCKET_TIMEOUT,
      ConfVars.PARTITION_NAME_WHITELIST_PATTERN,
      ConfVars.PARTITION_ORDER_EXPR,
      ConfVars.CAPABILITY_CHECK,
      ConfVars.DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES,
      ConfVars.EXPRESSION_PROXY_CLASS
  };

  static {
    for (ConfVars confVar : metaConfVars) {
      metaConfs.put(confVar.varname, confVar);
      metaConfs.put(confVar.hiveName, confVar);
    }
  }

  /**
   * Variables that we should never print the value of for security reasons.
   */
  private static final Set<String> unprintables = StringUtils.asSet(
      ConfVars.PWD.varname,
      ConfVars.PWD.hiveName,
      ConfVars.SSL_KEYSTORE_PASSWORD.varname,
      ConfVars.SSL_KEYSTORE_PASSWORD.hiveName,
      ConfVars.SSL_TRUSTSTORE_PASSWORD.varname,
      ConfVars.SSL_TRUSTSTORE_PASSWORD.hiveName,
      ConfVars.DBACCESS_SSL_TRUSTSTORE_PASSWORD.varname,
      ConfVars.DBACCESS_SSL_TRUSTSTORE_PASSWORD.hiveName,
      ConfVars.THRIFT_ZOOKEEPER_SSL_KEYSTORE_PASSWORD.varname,
      ConfVars.THRIFT_ZOOKEEPER_SSL_KEYSTORE_PASSWORD.hiveName,
      ConfVars.THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD.varname,
      ConfVars.THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD.hiveName
  );

  public static ConfVars getMetaConf(String name) {
    return metaConfs.get(name);
  }

  public enum ConfVars {
    // alpha order, PLEASE!
    ACID_HOUSEKEEPER_SERVICE_INTERVAL("metastore.acid.housekeeper.interval",
        "hive.metastore.acid.housekeeper.interval", 60, TimeUnit.SECONDS,
        "Time interval describing how often the acid housekeeper runs."),
    COMPACTION_HOUSEKEEPER_SERVICE_INTERVAL("metastore.compaction.housekeeper.interval",
        "hive.metastore.compaction.housekeeper.interval", 300, TimeUnit.SECONDS,
        "Time interval describing how often the acid compaction housekeeper runs."),
    ACID_TXN_CLEANER_INTERVAL("metastore.acid.txn.cleaner.interval",
        "hive.metastore.acid.txn.cleaner.interval", 10, TimeUnit.SECONDS,
        "Time interval describing how often aborted and committed txns are cleaned."),
    ADDED_JARS("metastore.added.jars.path", "hive.added.jars.path", "",
        "This an internal parameter."),
    AGGREGATE_STATS_CACHE_CLEAN_UNTIL("metastore.aggregate.stats.cache.clean.until",
        "hive.metastore.aggregate.stats.cache.clean.until", 0.8,
        "The cleaner thread cleans until cache reaches this % full size."),
    AGGREGATE_STATS_CACHE_ENABLED("metastore.aggregate.stats.cache.enabled",
        "hive.metastore.aggregate.stats.cache.enabled", false,
        "Whether aggregate stats caching is enabled or not."),
    AGGREGATE_STATS_CACHE_FPP("metastore.aggregate.stats.cache.fpp",
        "hive.metastore.aggregate.stats.cache.fpp", 0.01,
        "Maximum false positive probability for the Bloom Filter used in each aggregate stats cache node (default 1%)."),
    AGGREGATE_STATS_CACHE_MAX_FULL("metastore.aggregate.stats.cache.max.full",
        "hive.metastore.aggregate.stats.cache.max.full", 0.9,
        "Maximum cache full % after which the cache cleaner thread kicks in."),
    AGGREGATE_STATS_CACHE_MAX_PARTITIONS("metastore.aggregate.stats.cache.max.partitions",
        "hive.metastore.aggregate.stats.cache.max.partitions", 10000,
        "Maximum number of partitions that are aggregated per cache node."),
    AGGREGATE_STATS_CACHE_MAX_READER_WAIT("metastore.aggregate.stats.cache.max.reader.wait",
        "hive.metastore.aggregate.stats.cache.max.reader.wait", 1000, TimeUnit.MILLISECONDS,
        "Number of milliseconds a reader will wait to acquire the readlock before giving up."),
    AGGREGATE_STATS_CACHE_MAX_VARIANCE("metastore.aggregate.stats.cache.max.variance",
        "hive.metastore.aggregate.stats.cache.max.variance", 0.01,
        "Maximum tolerable variance in number of partitions between a cached node and our request (default 1%)."),
    AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT("metastore.aggregate.stats.cache.max.writer.wait",
        "hive.metastore.aggregate.stats.cache.max.writer.wait", 5000, TimeUnit.MILLISECONDS,
        "Number of milliseconds a writer will wait to acquire the writelock before giving up."),
    AGGREGATE_STATS_CACHE_SIZE("metastore.aggregate.stats.cache.size",
        "hive.metastore.aggregate.stats.cache.size", 10000,
        "Maximum number of aggregate stats nodes that we will place in the metastore aggregate stats cache."),
    AGGREGATE_STATS_CACHE_TTL("metastore.aggregate.stats.cache.ttl",
        "hive.metastore.aggregate.stats.cache.ttl", 600, TimeUnit.SECONDS,
        "Number of seconds for a cached node to be active in the cache before they become stale."),
    ALLOW_TENANT_BASED_STORAGE("metastore.warehouse.tenant.colocation", "hive.metastore.warehouse.tenant.colocation", false,
        "Allows managed and external tables for a tenant to have a common parent directory\n" +
        "For example: /user/warehouse/user1/managed and /user/warehouse/user1/external\n" +
        "This allows users to be able to set quotas on user1 directory. These locations have to be defined on the\n" +
        "database object explicitly when creating the DB or via alter database."),
    ALTER_HANDLER("metastore.alter.handler", "hive.metastore.alter.impl",
        HIVE_ALTER_HANDLE_CLASS,
        "Alter handler.  For now defaults to the Hive one.  Really need a better default option"),
    ASYNC_LOG_ENABLED("metastore.async.log.enabled", "hive.async.log.enabled", true,
        "Whether to enable Log4j2's asynchronous logging. Asynchronous logging can give\n" +
            " significant performance improvement as logging will be handled in separate thread\n" +
            " that uses LMAX disruptor queue for buffering log messages.\n" +
            " Refer https://logging.apache.org/log4j/2.x/manual/async.html for benefits and\n" +
            " drawbacks."),
    AUTHORIZATION_STORAGE_AUTH_CHECKS("metastore.authorization.storage.checks",
        "hive.metastore.authorization.storage.checks", false,
        "Should the metastore do authorization checks against the underlying storage (usually hdfs) \n" +
            "for operations like drop-partition (disallow the drop-partition if the user in\n" +
            "question doesn't have permissions to delete the corresponding directory\n" +
            "on the storage)."),
    AUTO_CREATE_ALL("datanucleus.schema.autoCreateAll", "datanucleus.schema.autoCreateAll", false,
        "Auto creates necessary schema on a startup if one doesn't exist. Set this to false, after creating it once."
            + "To enable auto create also set hive.metastore.schema.verification=false. Auto creation is not "
            + "recommended for production use cases, run schematool command instead." ),
    BATCH_RETRIEVE_MAX("metastore.batch.retrieve.max", "hive.metastore.batch.retrieve.max", 300,
            new RangeValidator(1, null),
        "Maximum number of objects (tables/partitions) can be retrieved from metastore in one batch. \n" +
            "The higher the number, the less the number of round trips is needed to the Hive metastore server, \n" +
            "but it may also cause higher memory requirement at the client side. Batch value should be greater than 0"),
    BATCH_RETRIEVE_OBJECTS_MAX("metastore.batch.retrieve.table.partition.max",
        "hive.metastore.batch.retrieve.table.partition.max", 1000,
        "Maximum number of objects that metastore internally retrieves in one batch."),
    CACHE_PINOBJTYPES("metastore.cache.pinobjtypes", "hive.metastore.cache.pinobjtypes",
        "Table,StorageDescriptor,SerDeInfo,Partition,Database,Type,FieldSchema,Order",
        "List of comma separated metastore object types that should be pinned in the cache"),
    CACHED_RAW_STORE_IMPL("metastore.cached.rawstore.impl",
        "hive.metastore.cached.rawstore.impl", "org.apache.hadoop.hive.metastore.ObjectStore",
        "Name of the wrapped RawStore class"),
    CACHED_RAW_STORE_CACHE_UPDATE_FREQUENCY("metastore.cached.rawstore.cache.update.frequency",
        "hive.metastore.cached.rawstore.cache.update.frequency", 60, TimeUnit.SECONDS,
        "The time after which metastore cache is updated from metastore DB."),
    CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST("metastore.cached.rawstore.cached.object.whitelist",
        "hive.metastore.cached.rawstore.cached.object.whitelist", ".*", "Comma separated list of regular expressions \n " +
        "to select the tables (and its partitions, stats etc) that will be cached by CachedStore. \n" +
        "This can be used in conjunction with hive.metastore.cached.rawstore.cached.object.blacklist. \n" +
        "Example: .*, db1.*, db2\\.tbl.*. The last item can potentially override patterns specified before."),
    CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST("metastore.cached.rawstore.cached.object.blacklist",
         "hive.metastore.cached.rawstore.cached.object.blacklist", "", "Comma separated list of regular expressions \n " +
         "to filter out the tables (and its partitions, stats etc) that will be cached by CachedStore. \n" +
         "This can be used in conjunction with hive.metastore.cached.rawstore.cached.object.whitelist. \n" +
         "Example: db2.*, db3\\.tbl1, db3\\..*. The last item can potentially override patterns specified before. \n" +
         "The blacklist also overrides the whitelist."),
    CACHED_RAW_STORE_MAX_CACHE_MEMORY("metastore.cached.rawstore.max.cache.memory",
        "hive.metastore.cached.rawstore.max.cache.memory", "1Gb", new SizeValidator(),
        "The maximum memory in bytes that the cached objects can use. "
        + "Memory used is calculated based on estimated size of tables and partitions in the cache. "
        + "Setting it to a negative value disables memory estimation."),
    CAPABILITY_CHECK("metastore.client.capability.check",
        "hive.metastore.client.capability.check", true,
        "Whether to check client capabilities for potentially breaking API usage."),
    CATALOG_DEFAULT("metastore.catalog.default", "metastore.catalog.default", "hive",
        "The default catalog to use when a catalog is not specified.  Default is 'hive' (the " +
            "default catalog)."),
    CATALOGS_TO_CACHE("metastore.cached.rawstore.catalogs", "metastore.cached.rawstore.catalogs",
        "hive", "Comma separated list of catalogs to cache in the CachedStore. Default is 'hive' " +
        "(the default catalog).  Empty string means all catalogs will be cached."),
    CLIENT_CONNECT_RETRY_DELAY("metastore.client.connect.retry.delay",
        "hive.metastore.client.connect.retry.delay", 1, TimeUnit.SECONDS,
        "Number of seconds for the client to wait between consecutive connection attempts"),
    CLIENT_KERBEROS_PRINCIPAL("metastore.client.kerberos.principal",
        "hive.metastore.client.kerberos.principal",
        "", // E.g. "hive-metastore/_HOST@EXAMPLE.COM".
        "The Kerberos principal associated with the HA cluster of hcat_servers."),
    CLIENT_SOCKET_LIFETIME("metastore.client.socket.lifetime",
        "hive.metastore.client.socket.lifetime", 0, TimeUnit.SECONDS,
        "MetaStore Client socket lifetime in seconds. After this time is exceeded, client\n" +
            "reconnects on the next MetaStore operation. A value of 0s means the connection\n" +
            "has an infinite lifetime."),
    CLIENT_SOCKET_TIMEOUT("metastore.client.socket.timeout", "hive.metastore.client.socket.timeout", 600,
            TimeUnit.SECONDS, "MetaStore Client socket timeout in seconds"),
    CLIENT_CONNECTION_TIMEOUT("metastore.client.connection.timeout", "hive.metastore.client.connection.timeout", 600,
            TimeUnit.SECONDS, "MetaStore Client connection timeout in seconds"),
    COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE("metastore.compactor.history.retention.did.not.initiate",
        "hive.compactor.history.retention.did.not.initiate", 2,
        new RangeValidator(0, 100), "Determines how many compaction records in state " +
        "'did not initiate' will be retained in compaction history for a given table/partition.",
        // deprecated keys:
        "metastore.compactor.history.retention.attempted", "hive.compactor.history.retention.attempted"),
    COMPACTOR_HISTORY_RETENTION_FAILED("metastore.compactor.history.retention.failed",
        "hive.compactor.history.retention.failed", 3,
        new RangeValidator(0, 100), "Determines how many failed compaction records will be " +
        "retained in compaction history for a given table/partition."),
    COMPACTOR_HISTORY_RETENTION_SUCCEEDED("metastore.compactor.history.retention.succeeded",
        "hive.compactor.history.retention.succeeded", 3,
        new RangeValidator(0, 100), "Determines how many successful compaction records will be " +
        "retained in compaction history for a given table/partition."),
    COMPACTOR_HISTORY_RETENTION_REFUSED("metastore.compactor.history.retention.refused",
        "hive.compactor.history.retention.refused", 3,
        new RangeValidator(0, 100), "Determines how many refused compaction records will be " +
        "retained in compaction history for a given table/partition."),
    COMPACTOR_HISTORY_RETENTION_TIMEOUT("metastore.compactor.history.retention.timeout",
            "hive.compactor.history.retention.timeout", 7, TimeUnit.DAYS,
            "Determines how long failed, not initiated and refused compaction records will be " +
            "retained in compaction history if there is a more recent succeeded compaction on the table/partition."),
    COMPACTOR_INITIATOR_FAILED_THRESHOLD("metastore.compactor.initiator.failed.compacts.threshold",
        "hive.compactor.initiator.failed.compacts.threshold", 2,
        new RangeValidator(1, 20), "Number of consecutive compaction failures (per table/partition) " +
        "after which automatic compactions will not be scheduled any more.  Note that this must be less " +
        "than hive.compactor.history.retention.failed."),
    COMPACTOR_INITIATOR_FAILED_RETRY_TIME("metastore.compactor.initiator.failed.retry.time",
        "hive.compactor.initiator.failed.retry.time", 7, TimeUnit.DAYS,
        "Time after Initiator will ignore metastore.compactor.initiator.failed.compacts.threshold "
            + "and retry with compaction again. This will try to auto heal tables with previous failed compaction "
            + "without manual intervention. Setting it to 0 or negative value will disable this feature."),
    COMPACTOR_RUN_AS_USER("metastore.compactor.run.as.user", "hive.compactor.run.as.user", "",
        "Specify the user to run compactor Initiator and Worker as. If empty string, defaults to table/partition " +
        "directory owner."),
    COMPACTOR_USE_CUSTOM_POOL("metastore.compactor.use.custom.pool", "hive.compactor.use.custom.pool",
            false, "internal usage only -- use custom connection pool specific to compactor components."
    ),
    COMPACTOR_WORKER_POOL_TIMEOUT(
        "metastore.compactor.worker.pool.timeout",
        "hive.compactor.worker.pool.timeout",
        12, TimeUnit.HOURS,
        "Time in hours after which a compaction assigned to a custom compaction Worker pool can be picked " +
            "up by the default compaction Worker pool."),
    COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_WARNING(
        "metastore.compactor.oldest.replication.open.txn.threshold.warning",
        "hive.compactor.oldest.replication.open.txn.threshold.warning",
        14, TimeUnit.DAYS,
        "Age of open replication transaction after which a warning will be logged. Default time unit: days"),
    COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_ERROR(
        "metastore.compactor.oldest.replication.open.txn.threshold.error",
        "hive.compactor.oldest.replication.open.txn.threshold.error",
        21, TimeUnit.DAYS,
        "Age of open replication transaction after which an error will be logged. Default time unit: days"),
    COMPACTOR_OLDEST_OPENTXN_THRESHOLD_WARNING(
        "metastore.compactor.oldest.open.txn.threshold.warning",
        "hive.compactor.oldest.open.txn.threshold.warning",
        24, TimeUnit.HOURS,
        "Age of oldest open non-replication transaction after which a warning will be logged. " +
            "Default time unit: hours"),
    COMPACTOR_OLDEST_OPENTXN_THRESHOLD_ERROR(
        "metastore.compactor.oldest.open.txn.threshold.error",
        "hive.compactor.oldest.open.txn.threshold.error",
        72, TimeUnit.HOURS,
        "Age of oldest open non-replication transaction after which an error will be logged. "
            + "Default time unit: hours"),
    COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_WARNING(
        "metastore.compactor.oldest.uncleaned.aborted.txn.time.threshold.warning",
        "hive.compactor.oldest.uncleaned.aborted.txn.time.threshold.warning",
        24, TimeUnit.HOURS,
        "Age of oldest aborted transaction after which a warning will be logged. Default time unit: hours"),
    COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_ERROR(
        "metastore.compactor.oldest.uncleaned.aborted.txn.time.threshold.error",
        "hive.compactor.oldest.uncleaned.aborted.txn.time.threshold.error",
        48, TimeUnit.HOURS,
        "Age of oldest aborted transaction after which an error will be logged. Default time unit: hours"),
    COMPACTOR_TABLES_WITH_ABORTEDTXN_THRESHOLD(
        "metastore.compactor.tables.with.aborted.txn.threshold",
        "hive.compactor.tables.with.aborted.txn.threshold", 1,
        "Number of tables has not been compacted and have more than " +
            "hive.metastore.acidmetrics.table.aborted.txns.threshold (default 1500) aborted transactions. If this " +
            "threshold is passed, a warning will be logged."),
    COMPACTOR_OLDEST_UNCLEANED_COMPACTION_TIME_THRESHOLD(
        "metastore.compactor.oldest.uncleaned.compaction.time.threshold",
        "hive.compactor.oldest.uncleaned.compaction.time.threshold",
        24, TimeUnit.HOURS,
        "Age of oldest ready for cleaning compaction in the compaction queue. If this threshold is passed, " +
            "a warning will be logged. Default time unit is: hours"),
    COMPACTOR_FAILED_COMPACTION_RATIO_THRESHOLD(
        "metastore.compactor.failed.compaction.ratio.threshold",
        "hive.compactor.failed.compaction.ratio.threshold", .01,
        "Ratio between the number of failed compactions + not initiated compactions and number of failed " +
            "compactions + not initiated compactions + succeeded compactions. If this threshold is passed, a warning " +
            "will be logged."),
    COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_WARNING(
        "metastore.compactor.oldest.initiated.compaction.time.threshold.warning",
        "hive.compactor.oldest.initiated.compaction.time.threshold.warning",
        1, TimeUnit.HOURS,
        "Age of oldest initiated compaction in the compaction queue after which a warning will be logged. " +
            "Default time unit is: hours"),
    COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_ERROR(
        "metastore.compactor.oldest.initiated.compaction.time.threshold.error",
        "hive.compactor.oldest.initiated.compaction.time.threshold.error",
        12, TimeUnit.HOURS,
        "Age of oldest initiated compaction in the compaction queue after which an error will be logged. " +
            "Default time unit is: hours"),
    COMPACTOR_LONG_RUNNING_INITIATOR_THRESHOLD_WARNING(
        "metastore.compactor.long.running.initiator.threshold.warning",
        "hive.compactor.long.running.initiator.threshold.warning",
        6, TimeUnit.HOURS,
        "Initiator cycle duration after which a warning will be logged. " +
            "Default time unit is: hours"),
    COMPACTOR_LONG_RUNNING_INITIATOR_THRESHOLD_ERROR(
        "metastore.compactor.long.running.initiator.threshold.error",
        "hive.compactor.long.running.initiator.threshold.error",
        12, TimeUnit.HOURS,
        "Initiator cycle duration after which an error will be logged. " +
            "Default time unit is: hours"),
    COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_WARNING(
        "metastore.compactor.completed.txn.components.record.threshold.warning",
        "hive.compactor.completed.txn.components.record.threshold.warning",
        500000,
        "Number of records in COMPLETED_TXN_COMPONENTS table, after which a warning will be logged."),
    COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_ERROR(
        "metastore.compactor.completed.txn.components.record.threshold.error",
        "hive.compactor.completed.txn.components.record.threshold.error",
        1000000,
        "Number of records in COMPLETED_TXN_COMPONENTS table, after which an error will be logged."),
    COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_WARNING(
        "metastore.compactor.txn.to.writeid.record.threshold.warning",
        "hive.compactor.txn.to.writeid.record.threshold.warning",
        500000,
        "Number of records in TXN_TO_WRITEID table, after which a warning will be logged."),
    COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_ERROR(
        "metastore.compactor.txn.to.writeid.record.threshold.error",
        "hive.compactor.txn.to.writeid.record.threshold.error",
        1000000,
        "Number of records in TXN_TO_WRITEID table, after which an error will be logged."),
    COMPACTOR_NUMBER_OF_DISABLED_COMPACTION_TABLES_THRESHOLD(
        "metastore.compactor.number.of.disabled.compaction.tables.threshold",
        "hive.compactor.number.of.disabled.compaction.tables.threshold",
        1,
        "If the number of writes to tables where auto-compaction is disabled reaches this threshold, a " +
            "warning will be logged after every subsequent write to any table where auto-compaction is disabled."),
    COMPACTOR_ACID_METRICS_LOGGER_FREQUENCY(
        "metastore.compactor.acid.metrics.logger.frequency",
        "hive.compactor.acid.metrics.logger.frequency",
        360, TimeUnit.MINUTES,
        "Logging frequency of ACID related metrics. Set this value to 0 to completely turn off logging. " +
            "Default time unit: minutes"),
    COMPACTOR_FETCH_SIZE(
            "metastore.compactor.fetch.size",
            "hive.compactor.fetch.size",
            1000,
            new RangeValidator(100, 5000),
            "Limits the number of items fetched during cleaning, abort and finding potential compactions. " +
                    "Allowed values between 100 and 5000"),
    METASTORE_HOUSEKEEPING_LEADER_HOSTNAME("metastore.housekeeping.leader.hostname",
            "hive.metastore.housekeeping.leader.hostname", "",
"If there are multiple Thrift metastore services running, the hostname of Thrift metastore " +
        "service to run housekeeping tasks at. By default this values is empty, which " +
        "means that the current metastore will run the housekeeping tasks. If configuration" +
        "metastore.thrift.bind.host is set on the intended leader metastore, this value should " +
        "match that configuration. Otherwise it should be same as the hostname returned by " +
        "InetAddress#getLocalHost#getCanonicalHostName(). Given the uncertainty in the later " +
        "it is desirable to configure metastore.thrift.bind.host on the intended leader HMS."),
    METASTORE_HOUSEKEEPING_LEADER_ELECTION("metastore.housekeeping.leader.election",
        "metastore.housekeeping.leader.election",
        "host", new StringSetValidator("host", "lock"),
        "Set to host, HMS will choose the leader by the configured metastore.housekeeping.leader.hostname.\n" +
        "Set to lock, HMS will use the Hive lock to elect the leader."),
    METASTORE_HOUSEKEEPING_LEADER_AUDITTABLE("metastore.housekeeping.leader.auditTable",
        "metastore.housekeeping.leader.auditTable", "",
        "Audit the leader election event to a plain json table when configured."),
    METASTORE_HOUSEKEEPING_LEADER_NEW_AUDIT_FILE("metastore.housekeeping.leader.newAuditFile",
        "metastore.housekeeping.leader.newAuditFile", false,
        "Whether to create a new audit file in response to the new election event " +
        "when the metastore.housekeeping.leader.auditTable is not empty.\n" +
        "True for creating a new file, false otherwise."),
    METASTORE_HOUSEKEEPING_LEADER_AUDIT_FILE_LIMIT("metastore.housekeeping.leader.auditFiles.limit",
        "metastore.housekeeping.leader.auditFiles.limit", 10,
        "Limit the number of small audit files when metastore.housekeeping.leader.newAuditFile is true.\n" +
        "If the number of audit files exceeds the limit, then the oldest will be deleted."),
    METASTORE_HOUSEKEEPING_LEADER_LOCK_NAMESPACE("metastore.housekeeping.leader.lock.namespace",
        "metastore.housekeeping.leader.lock.namespace", "",
        "The database where the Hive lock sits when metastore.housekeeping.leader.election is set to lock."),
    METASTORE_HOUSEKEEPING_THREADS_ON("metastore.housekeeping.threads.on",
        "hive.metastore.housekeeping.threads.on", false,
        "Whether to run the tasks under metastore.task.threads.remote on this metastore instance or not.\n" +
            "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
            "on Hive transactions. For a complete list of parameters required for turning on\n" +
            "transactions, see hive.txn.manager."),
    METASTORE_ACIDMETRICS_THREAD_ON("metastore.acidmetrics.thread.on",
        "hive.metastore.acidmetrics.thread.on", true,
        "Whether to run acid related metrics collection on this metastore instance."),
    METASTORE_ACIDMETRICS_CHECK_INTERVAL("metastore.acidmetrics.check.interval",
        "hive.metastore.acidmetrics.check.interval", 300,
        TimeUnit.SECONDS,
        "Time in seconds between acid related metric collection runs."),
    METASTORE_ACIDMETRICS_EXT_ON("metastore.acidmetrics.ext.on", "hive.metastore.acidmetrics.ext.on", true,
        "Whether to collect additional acid related metrics outside of the acid metrics service. "
            + "(metastore.metrics.enabled and/or hive.server2.metrics.enabled are also required to be set to true.)"),
    METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD("metastore.acidmetrics.table.aborted.txns.threshold",
        "hive.metastore.acidmetrics.table.aborted.txns.threshold", 1500,
        "The acid metrics system will collect the number of tables which have a large number of aborted transactions." +
            "This parameter controls the minimum number of aborted transaction required so that a table will be counted."),
    METASTORE_DELTAMETRICS_MAX_CACHE_SIZE("metastore.deltametrics.max.cache.size",
        "hive.txn.acid.metrics.max.cache.size",
        100, new RangeValidator(0, 500),
        "Size of the ACID metrics cache, i.e. max number of partitions and unpartitioned tables with the "
        + "most deltas that will be included in the lists of active, obsolete and small deltas. "
        + "Allowed range is 0 to 500."),
    METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD("metastore.deltametrics.delta.num.threshold",
        "hive.txn.acid.metrics.delta.num.threshold", 100,
        "The minimum number of active delta files a table/partition must have in order to be included in the ACID metrics report."),
    METASTORE_DELTAMETRICS_OBSOLETE_DELTA_NUM_THRESHOLD("metastore.deltametrics.obsolete.delta.num.threshold",
        "hive.txn.acid.metrics.obsolete.delta.num.threshold", 100,
        "The minimum number of obsolete delta files a table/partition must have in order to be included in the ACID metrics report."),
    METASTORE_DELTAMETRICS_DELTA_PCT_THRESHOLD("metastore.deltametrics.delta.pct.threshold",
        "hive.txn.acid.metrics.delta.pct.threshold", 0.01f,
        "Percentage (fractional) size of the delta files relative to the base directory. Deltas smaller than this threshold " +
            "count as small deltas. Default 0.01 = 1%.)"),
    COMPACTOR_INITIATOR_ON("metastore.compactor.initiator.on", "hive.compactor.initiator.on", false,
        "Whether to run the initiator thread on this metastore instance or not.\n" +
            "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
            "on Hive transactions. For a complete list of parameters required for turning on\n" +
            "transactions, see hive.txn.manager."),
    COMPACTOR_CLEANER_ON("metastore.compactor.cleaner.on", "hive.compactor.cleaner.on", false,
        "Whether to run the cleaner thread on this metastore instance or not.\n" +
            "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
            "on Hive transactions. For a complete list of parameters required for turning on\n" +
            "transactions, see hive.txn.manager."),
    COMPACTOR_INITIATOR_TABLECACHE_ON("metastore.compactor.initiator.tablecache.on",
      "hive.compactor.initiator.tablecache.on", true,
      "Enable table caching in the initiator. Currently the cache is cleaned after each cycle."),
    COMPACTOR_WORKER_THREADS("metastore.compactor.worker.threads",
        "hive.compactor.worker.threads", 0,
        "How many compactor worker threads to run on this metastore instance. Set this to a\n" +
            "positive number on one or more instances of the Thrift metastore service as part of\n" +
            "turning on Hive transactions. For a complete list of parameters required for turning\n" +
            "on transactions, see hive.txn.manager.\n" +
            "Worker threads spawn MapReduce jobs to do compactions. They do not do the compactions\n" +
            "themselves. Increasing the number of worker threads will decrease the time it takes\n" +
            "tables or partitions to be compacted once they are determined to need compaction.\n" +
            "It will also increase the background load on the Hadoop cluster as more MapReduce jobs\n" +
            "will be running in the background."),
    COMPACTOR_WORKER_DETECT_MULTIPLE_VERSION_THRESHOLD("metastore.compactor.worker.detect.multiple.versions.threshold",
      "hive.metastore.compactor.worker.detect.multiple.versions.threshold", 24, TimeUnit.HOURS,
      "Defines a time-window in hours from the current time backwards\n," +
            "in which a warning is being raised if multiple worker version are detected.\n" +
            "The setting has no effect if the metastore.compactor.acid.metrics.logger.frequency is 0."),
    COMPACTOR_MINOR_STATS_COMPRESSION(
        "metastore.compactor.enable.stats.compression",
        "metastore.compactor.enable.stats.compression", true,
        "Can be used to disable compression and ORC indexes for files produced by minor compaction."),
    HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS("hive.compactor.cleaner.retry.maxattempts",
            "hive.compactor.cleaner.retry.maxattempts", 5, new RangeValidator(0, 10),
            "Maximum number of attempts to clean a table again after a failed cycle. Must be between 0 and 10," +
                "where 0 means the feature is turned off, the cleaner wont't retry after a failed cycle."),
    HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME("hive.compactor.cleaner.retry.retention.time",
            "hive.compactor.cleaner.retry.retentionTime", 300, TimeUnit.SECONDS, new TimeValidator(TimeUnit.SECONDS),
            "Initial value of the cleaner retry retention time. The delay has a backoff, and calculated the following way: " +
            "pow(2, number_of_failed_attempts) * HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME."),
    COMPACTOR_CLEANER_TABLECACHE_ON("metastore.compactor.cleaner.tablecache.on",
            "hive.compactor.cleaner.tablecache.on", true,
            "Enable table caching in the cleaner. Currently the cache is cleaned after each cycle."),
    COMPACTOR_CLEAN_ABORTS_USING_CLEANER("metastore.compactor.clean.aborts.using.cleaner", "hive.compactor.clean.aborts.using.cleaner", true,
            "Whether to use cleaner for cleaning aborted directories or not.\n" +
            "Set to true when cleaner is expected to clean delta/delete-delta directories from aborted transactions.\n" +
            "Otherwise the cleanup of such directories will take place within the compaction cycle."),
    HIVE_COMPACTOR_CONNECTION_POOLING_MAX_CONNECTIONS("metastore.compactor.connectionPool.maxPoolSize",
            "hive.compactor.connectionPool.maxPoolSize", 5,
            "Specify the maximum number of connections in the connection pool used by the compactor."),
    CONNECTION_DRIVER("javax.jdo.option.ConnectionDriverName",
        "javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver",
        "Driver class name for a JDBC metastore"),
    CONNECTION_POOLING_MAX_CONNECTIONS("datanucleus.connectionPool.maxPoolSize",
        "datanucleus.connectionPool.maxPoolSize", 10,
        "Specify the maximum number of connections in the connection pool. Note: The configured size will be used by\n" +
            "2 connection pools (TxnHandler and ObjectStore). When configuring the max connection pool size, it is\n" +
            "recommended to take into account the number of metastore instances and the number of HiveServer2 instances\n" +
            "configured with embedded metastore. To get optimal performance, set config to meet the following condition\n"+
            "(2 * pool_size * metastore_instances + 2 * pool_size * HS2_instances_with_embedded_metastore) = \n" +
            "(2 * physical_core_count + hard_disk_count)."),
    CONNECT_URL_HOOK("metastore.ds.connection.url.hook",
        "hive.metastore.ds.connection.url.hook", "",
        "Name of the hook to use for retrieving the JDO connection URL. If empty, the value in javax.jdo.option.ConnectionURL is used"),
    CONNECT_URL_KEY("javax.jdo.option.ConnectionURL",
        "javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=metastore_db;create=true",
        "JDBC connect string for a JDBC metastore.\n" +
            "To use SSL to encrypt/authenticate the connection, provide database-specific SSL flag in the connection URL.\n" +
            "For example, jdbc:postgresql://myhost/db?ssl=true for postgres database."),
    CONNECTION_POOLING_TYPE("datanucleus.connectionPoolingType",
        "datanucleus.connectionPoolingType", "HikariCP", new StringSetValidator("DBCP",
        "HikariCP", "NONE"),
        "Specify connection pool library for datanucleus"),
    CONNECTION_USER_NAME("javax.jdo.option.ConnectionUserName",
        "javax.jdo.option.ConnectionUserName", "APP",
        "Username to use against metastore database"),
    CREATE_TABLES_AS_ACID("metastore.create.as.acid", "hive.create.as.acid", false,
        "Whether the eligible tables should be created as full ACID by default. Does \n" +
            "not apply to external tables, the ones using storage handlers, etc."),
    COUNT_OPEN_TXNS_INTERVAL("metastore.count.open.txns.interval", "hive.count.open.txns.interval",
        1, TimeUnit.SECONDS, "Time in seconds between checks to count open transactions."),
    DATANUCLEUS_AUTOSTART("datanucleus.autoStartMechanismMode",
        "datanucleus.autoStartMechanismMode", "ignored", new StringSetValidator("ignored"),
        "Autostart mechanism for datanucleus.  Currently ignored is the only option supported."),
    DATANUCLEUS_CACHE_LEVEL2("datanucleus.cache.level2", "datanucleus.cache.level2", false,
        "Use a level 2 cache. Turn this off if metadata is changed independently of Hive metastore server"),
    DATANUCLEUS_CACHE_LEVEL2_TYPE("datanucleus.cache.level2.type",
        "datanucleus.cache.level2.type", "none", ""),
    DATANUCLEUS_INIT_COL_INFO("datanucleus.rdbms.initializeColumnInfo",
        "datanucleus.rdbms.initializeColumnInfo", "NONE",
        "initializeColumnInfo setting for DataNucleus; set to NONE at least on Postgres."),
    DATANUCLEUS_PLUGIN_REGISTRY_BUNDLE_CHECK("datanucleus.plugin.pluginRegistryBundleCheck",
        "datanucleus.plugin.pluginRegistryBundleCheck", "LOG",
        "Defines what happens when plugin bundles are found and are duplicated [EXCEPTION|LOG|NONE]"),
    DATANUCLEUS_TRANSACTION_ISOLATION("datanucleus.transactionIsolation",
        "datanucleus.transactionIsolation", "read-committed",
        "Default transaction isolation level for identity generation."),
    DATANUCLEUS_USE_LEGACY_VALUE_STRATEGY("datanucleus.rdbms.useLegacyNativeValueStrategy",
        "datanucleus.rdbms.useLegacyNativeValueStrategy", true, ""),
    DATANUCLEUS_QUERY_SQL_ALLOWALL("datanucleus.query.sql.allowAll", "datanucleus.query.sql.allowAll",
        true, "In strict JDO all SQL queries must begin with \"SELECT ...\", and consequently it "
        + "is not possible to execute queries that change data. This DataNucleus property when set to true allows "
        + "insert, update and delete operations from JDO SQL. Default value is true."),

    // Parameters for configuring SSL encryption to the database store
    // If DBACCESS_USE_SSL is false, then all other DBACCESS_SSL_* properties will be ignored
    DBACCESS_SSL_TRUSTSTORE_PASSWORD("metastore.dbaccess.ssl.truststore.password", "hive.metastore.dbaccess.ssl.truststore.password", "",
        "Password for the Java truststore file that is used when encrypting the connection to the database store. \n"
            + "metastore.dbaccess.ssl.use.SSL must be set to true for this property to take effect. \n"
            + "This directly maps to the javax.net.ssl.trustStorePassword Java system property. Defaults to jssecacerts, if it exists, otherwise uses cacerts. \n"
            + "It is recommended to specify the password using a credential provider so as to not expose it to discovery by other users. \n"
            + "One way to do this is by using the Hadoop CredentialProvider API and provisioning credentials for this property. Refer to the Hadoop CredentialProvider API Guide for more details."),
    DBACCESS_SSL_TRUSTSTORE_PATH("metastore.dbaccess.ssl.truststore.path", "hive.metastore.dbaccess.ssl.truststore.path", "",
        "Location on disk of the Java truststore file to use when encrypting the connection to the database store. \n"
            + "This file consists of a collection of certificates trusted by the metastore server. \n"
            + "metastore.dbaccess.ssl.use.SSL must be set to true for this property to take effect. \n"
            + "This directly maps to the javax.net.ssl.trustStore Java system property. Defaults to the default Java truststore file. \n"),
    DBACCESS_SSL_TRUSTSTORE_TYPE("metastore.dbaccess.ssl.truststore.type", "hive.metastore.dbaccess.ssl.truststore.type", "jks",
        new StringSetValidator("jceks", "jks", "dks", "pkcs11", "pkcs12", "bcfks"),
        "File type for the Java truststore file that is used when encrypting the connection to the database store. \n"
            + "metastore.dbaccess.ssl.use.SSL must be set to true for this property to take effect. \n"
            + "This directly maps to the javax.net.ssl.trustStoreType Java system property. \n"
            + "Types jceks, jks, dks, pkcs11, and pkcs12 can be read from Java 8 and beyond. Defaults to jks."),
    DBACCESS_USE_SSL("metastore.dbaccess.ssl.use.SSL", "hive.metastore.dbaccess.ssl.use.SSL", false,
        "Set this to true to use SSL encryption to the database store."),

    DEFAULTPARTITIONNAME("metastore.default.partition.name",
        "hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__",
        "The default partition name in case the dynamic partition column value is null/empty string or any other values that cannot be escaped. \n" +
            "This value must not contain any special character used in HDFS URI (e.g., ':', '%', '/' etc). \n" +
            "The user has to be aware that the dynamic partition value should not contain this value to avoid confusions."),
    DELEGATION_KEY_UPDATE_INTERVAL("metastore.cluster.delegation.key.update-interval",
        "hive.cluster.delegation.key.update-interval", 1, TimeUnit.DAYS, ""),
    DELEGATION_TOKEN_GC_INTERVAL("metastore.cluster.delegation.token.gc-interval",
        "hive.cluster.delegation.token.gc-interval", 15, TimeUnit.MINUTES, ""),
    DELEGATION_TOKEN_MAX_LIFETIME("metastore.cluster.delegation.token.max-lifetime",
        "hive.cluster.delegation.token.max-lifetime", 7, TimeUnit.DAYS, ""),
    DELEGATION_TOKEN_RENEW_INTERVAL("metastore.cluster.delegation.token.renew-interval",
      "hive.cluster.delegation.token.renew-interval", 1, TimeUnit.DAYS, ""),
    DELEGATION_TOKEN_STORE_CLS("metastore.cluster.delegation.token.store.class",
        "hive.cluster.delegation.token.store.class", METASTORE_DELEGATION_MANAGER_CLASS,
        "Class to store delegation tokens"),
    DETACH_ALL_ON_COMMIT("javax.jdo.option.DetachAllOnCommit",
        "javax.jdo.option.DetachAllOnCommit", true,
        "Detaches all objects from session so that they can be used after transaction is committed"),
    DIRECT_SQL_MAX_ELEMENTS_IN_CLAUSE("metastore.direct.sql.max.elements.in.clause",
        "hive.direct.sql.max.elements.in.clause", 1000,
        "The maximum number of values in a IN clause. Once exceeded, it will be broken into\n" +
            " multiple OR separated IN clauses."),
    DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE("metastore.direct.sql.max.elements.values.clause",
        "hive.direct.sql.max.elements.values.clause",
        1000, "The maximum number of values in a VALUES clause for INSERT statement."),
    DIRECT_SQL_MAX_PARAMETERS("metastore.direct.sql.max.parameters",
        "hive.direct.sql.max.parameters", 1000, "The maximum query parameters \n" +
            "backend sql engine can support."),
    DIRECT_SQL_MAX_QUERY_LENGTH("metastore.direct.sql.max.query.length",
        "hive.direct.sql.max.query.length", 100, "The maximum\n" +
        " size of a query string (in KB)."),
    DIRECT_SQL_PARTITION_BATCH_SIZE("metastore.direct.sql.batch.size",
        "hive.metastore.direct.sql.batch.size", 0,
        "Batch size for partition and other object retrieval from the underlying DB in direct\n" +
            "SQL. For some DBs like Oracle and MSSQL, there are hardcoded or perf-based limitations\n" +
            "that necessitate this. For DBs that can handle the queries, this isn't necessary and\n" +
            "may impede performance. -1 means no batching, 0 means automatic batching."),
    DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES("metastore.disallow.incompatible.col.type.changes",
        "hive.metastore.disallow.incompatible.col.type.changes", true,
        "If true, ALTER TABLE operations which change the type of a\n" +
            "column (say STRING) to an incompatible type (say MAP) are disallowed.\n" +
            "RCFile default SerDe (ColumnarSerDe) serializes the values in such a way that the\n" +
            "datatypes can be converted from string to any type. The map is also serialized as\n" +
            "a string, which can be read as a string as well. However, with any binary\n" +
            "serialization, this is not true. Blocking the ALTER TABLE prevents ClassCastExceptions\n" +
            "when subsequently trying to access old partitions.\n" +
            "\n" +
            "Primitive types like INT, STRING, BIGINT, etc., are compatible with each other and are\n" +
            "not blocked.\n" +
            "\n" +
            "See HIVE-4409 for more details."),
    ALLOW_INCOMPATIBLE_COL_TYPE_CHANGES_TABLE_SERDES("metastore.allow.incompatible.col.type.changes.serdes",
        "hive.metastore.allow.incompatible.col.type.changes.serdes",
        "org.apache.hadoop.hive.kudu.KuduSerDe,org.apache.iceberg.mr.hive.HiveIcebergSerDe",
        "Comma-separated list of table serdes which are allowed to make incompatible column type\n" +
        "changes. This configuration is only applicable if metastore.disallow.incompatible.col.type.changes\n" +
        "is true."),
    DUMP_CONFIG_ON_CREATION("metastore.dump.config.on.creation", "metastore.dump.config.on.creation", true,
        "If true, a printout of the config file (minus sensitive values) will be dumped to the " +
            "log whenever newMetastoreConf() is called.  Can produce a lot of logs"),
    END_FUNCTION_LISTENERS("metastore.end.function.listeners",
        "hive.metastore.end.function.listeners", "",
        "List of comma separated listeners for the end of metastore functions."),
    EVENT_CLEAN_FREQ("metastore.event.clean.freq", "hive.metastore.event.clean.freq", 0,
        TimeUnit.SECONDS, "Frequency at which timer task runs to purge expired events in metastore."),
    EVENT_EXPIRY_DURATION("metastore.event.expiry.duration", "hive.metastore.event.expiry.duration",
        0, TimeUnit.SECONDS, "Duration after which events expire from events table"),
    EVENT_LISTENERS("metastore.event.listeners", "hive.metastore.event.listeners", "",
        "A comma separated list of Java classes that implement the org.apache.riven.MetaStoreEventListener" +
            " interface. The metastore event and corresponding listener method will be invoked in separate JDO transactions. " +
            "Alternatively, configure hive.metastore.transactional.event.listeners to ensure both are invoked in same JDO transaction."),
    EVENT_MESSAGE_FACTORY("metastore.event.message.factory",
        "hive.metastore.event.message.factory",
        "org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder",
        "Factory class for making encoding and decoding messages in the events generated."),
    REPL_MESSAGE_FACTORY("metastore.repl.message.factory",
            "hive.metastore.repl.message.factory",
            "org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder",
            "Factory class to serialize and deserialize information in replication metrics table."),
    EVENT_NOTIFICATION_PARAMETERS_EXCLUDE_PATTERNS("metastore.notification.parameters.exclude.patterns",
        "hive.metastore.notification.parameters.exclude.patterns", "",
        "List of comma-separated regexes that are used to reduced the size of HMS Notification messages."
            + " The regexes are matched against each key of parameters map in Table or Partition object"
            + "present in HMS Notification. Any key-value pair whose key is matched with any regex will"
            +" be removed from Parameters map during Serialization of Table/Partition object."),
    EVENT_DB_LISTENER_TTL("metastore.event.db.listener.timetolive",
        "hive.metastore.event.db.listener.timetolive", 1, TimeUnit.DAYS,
        "time after which events will be removed from the database listener queue when repl.cm.enabled \n" +
         "is set to false. When set to true, the conf repl.event.db.listener.timetolive is used instead."),
    EVENT_CLEAN_MAX_EVENTS("metastore.event.db.clean.maxevents",
            "hive.metastore.event.db.clean.maxevents", 10000,
            "Limit on number events to be cleaned at a time in metastore cleanNotificationEvents " +
                    "call, to avoid OOM. The configuration is not effective when set to zero or " +
                    "a negative value."),
    EVENT_DB_LISTENER_CLEAN_INTERVAL("metastore.event.db.listener.clean.interval",
            "hive.metastore.event.db.listener.clean.interval", 7200, TimeUnit.SECONDS,
            "sleep interval between each run for cleanup of events from the database listener queue"),
    EVENT_DB_LISTENER_CLEAN_STARTUP_WAIT_INTERVAL("metastore.event.db.listener.clean.startup.wait.interval",
        "hive.metastore.event.db.listener.clean.startup.wait.interval", 1, TimeUnit.DAYS,
        "Wait interval post start of metastore after which the cleaner thread starts to work"),
    EVENT_DB_NOTIFICATION_API_AUTH("metastore.metastore.event.db.notification.api.auth",
        "hive.metastore.event.db.notification.api.auth", true,
        "Should metastore do authorization against database notification related APIs such as get_next_notification.\n" +
            "If set to true, then only the superusers in proxy settings have the permission"),
    EXECUTE_SET_UGI("metastore.execute.setugi", "hive.metastore.execute.setugi", true,
        "In unsecure mode, setting this property to true will cause the metastore to execute DFS operations using \n" +
            "the client's reported user and group permissions. Note that this property must be set on \n" +
            "both the client and server sides. Further note that its best effort. \n" +
            "If client sets its to true and server sets it to false, client setting will be ignored."),
    EXPRESSION_PROXY_CLASS("metastore.expression.proxy", "hive.metastore.expression.proxy",
        "org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore",
        "Class to use to process expressions in partition pruning."),
    DECODE_FILTER_EXPRESSION_TO_STRING("metastore.decode.filter.expression.tostring",
        "hive.metastore.decode.filter.expression.tostring", false,
        "If set to true convertExprToFilter method of PartitionExpressionForMetastore will decode \n" +
            "byte array into string rather than ExprNode. This is specially required for \n" +
            "msck command when used with filter conditions"),
    FILE_METADATA_THREADS("metastore.file.metadata.threads",
        "hive.metastore.hbase.file.metadata.threads", 1,
        "Number of threads to use to read file metadata in background to cache it."),
    FILTER_HOOK("metastore.filter.hook", "hive.metastore.filter.hook",
        org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl.class.getName(),
        "Metastore hook class for filtering the metadata read results. If hive.security.authorization.manager"
            + "is set to instance of HiveAuthorizerFactory, then this value is ignored."),
    FS_HANDLER_CLS("metastore.fs.handler.class", "hive.metastore.fs.handler.class",
        "org.apache.hadoop.hive.metastore.HiveMetaStoreFsImpl", ""),
    FS_HANDLER_THREADS_COUNT("metastore.fshandler.threads", "hive.metastore.fshandler.threads", 15,
        "Number of threads to be allocated for metastore handler for fs operations."),
    HMS_HANDLER_ATTEMPTS("metastore.hmshandler.retry.attempts", "hive.hmshandler.retry.attempts", 10,
        "The number of times to retry a HMSHandler call if there were a connection error."),
    HMS_HANDLER_FORCE_RELOAD_CONF("metastore.hmshandler.force.reload.conf",
        "hive.hmshandler.force.reload.conf", false,
        "Whether to force reloading of the HMSHandler configuration (including\n" +
            "the connection URL, before the next metastore query that accesses the\n" +
            "datastore. Once reloaded, this value is reset to false. Used for\n" +
            "testing only."),
    HMS_HANDLER_INTERVAL("metastore.hmshandler.retry.interval", "hive.hmshandler.retry.interval",
        2000, TimeUnit.MILLISECONDS, "The time between HMSHandler retry attempts on failure."),
    HMS_HANDLER_PROXY_CLASS("metastore.hmshandler.proxy", "hive.metastore.hmshandler.proxy",
        METASTORE_RETRYING_HANDLER_CLASS,
        "The proxy class name of HMSHandler, default is RetryingHMSHandler."),
    IDENTIFIER_FACTORY("datanucleus.identifierFactory",
        "datanucleus.identifierFactory", "datanucleus1",
        "Name of the identifier factory to use when generating table/column names etc. \n" +
            "'datanucleus1' is used for backward compatibility with DataNucleus v1"),
    INIT_HOOKS("metastore.init.hooks", "hive.metastore.init.hooks", "",
        "A comma separated list of hooks to be invoked at the beginning of HMSHandler initialization. \n" +
            "An init hook is specified as the name of Java class which extends org.apache.riven.MetaStoreInitListener."),
    INTEGER_JDO_PUSHDOWN("metastore.integral.jdo.pushdown",
        "hive.metastore.integral.jdo.pushdown", false,
        "Allow JDO query pushdown for integral partition columns in metastore. Off by default. This\n" +
            "improves metastore perf for integral columns, especially if there's a large number of partitions.\n" +
            "However, it doesn't work correctly with integral values that are not normalized (e.g. have\n" +
            "leading zeroes, like 0012). If metastore direct SQL is enabled and works, this optimization\n" +
            "is also irrelevant."),
    // Once exceeded, the queries should be broken into separate batches.
    // Note: This value is not passed into the JDBC driver, therefore this batch size limit is not automatically enforced.
    // Batch construction/splits should be done manually in code using this config value.
    JDBC_MAX_BATCH_SIZE("metastore.jdbc.max.batch.size", "hive.metastore.jdbc.max.batch.size",
            1000, new RangeValidator(1, null),
            "Maximum number of update/delete/insert queries in a single JDBC batch statement (including Statement/PreparedStatement)."),
    KERBEROS_KEYTAB_FILE("metastore.kerberos.keytab.file",
        "hive.metastore.kerberos.keytab.file", "",
        "The path to the Kerberos Keytab file containing the metastore Thrift server's service principal."),
    KERBEROS_PRINCIPAL("metastore.kerberos.principal", "hive.metastore.kerberos.principal",
        "hive-metastore/_HOST@EXAMPLE.COM",
        "The service principal for the metastore Thrift server. \n" +
            "The special string _HOST will be replaced automatically with the correct host name."),
    THRIFT_METASTORE_AUTHENTICATION("metastore.authentication", "hive.metastore.authentication",
            "NOSASL",
      new StringSetValidator("NOSASL", "NONE", "LDAP", "KERBEROS", "CUSTOM", "JWT"),
        "Client authentication types.\n" +
                "  NONE: no authentication check\n" +
                "  LDAP: LDAP/AD based authentication\n" +
                "  KERBEROS: Kerberos/GSSAPI authentication\n" +
                "  CUSTOM: Custom authentication provider\n" +
                "          (Use with property metastore.custom.authentication.class)\n" +
                "  CONFIG: username and password is specified in the config" +
                "  NOSASL:  Raw transport" +
                "  JWT:  JSON Web Token authentication via JWT token. Only supported in Http/Https mode"),
    THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL("metastore.authentication.jwt.jwks.url",
        "hive.metastore.authentication.jwt.jwks.url", "", "File URL from where URLBasedJWKSProvider "
        + "in metastore server will try to load JWKS to match a JWT sent in HTTP request header. Used only when "
        + "Hive metastore server is running in JWT auth mode"),
    METASTORE_CUSTOM_AUTHENTICATION_CLASS("metastore.custom.authentication.class",
            "hive.metastore.custom.authentication.class",
            "",
        "Custom authentication class. Used when property\n" +
        "'metastore.authentication' is set to 'CUSTOM'. Provided class\n" +
        "must be a proper implementation of the interface\n" +
        "org.apache.hadoop.hive.metastore.MetaStorePasswdAuthenticationProvider. MetaStore\n" +
        "will call its Authenticate(user, passed) method to authenticate requests.\n" +
        "The implementation may optionally implement Hadoop's\n" +
        "org.apache.hadoop.conf.Configurable class to grab MetaStore's Configuration object."),
    METASTORE_PLAIN_LDAP_URL("metastore.authentication.ldap.url",
            "hive.metastore.authentication.ldap.url", "",
"LDAP connection URL(s),\n" +
        "this value could contain URLs to multiple LDAP servers instances for HA,\n" +
        "each LDAP URL is separated by a SPACE character. URLs are used in the \n" +
        " order specified until a connection is successful."),
    METASTORE_PLAIN_LDAP_BASEDN("metastore.authentication.ldap.baseDN",
            "hive.metastore.authentication.ldap.baseDN", "", "LDAP base DN"),
    METASTORE_PLAIN_LDAP_DOMAIN("metastore.authentication.ldap.Domain",
            "hive.metastore.authentication.ldap.Domain", "", ""),
    METASTORE_PLAIN_LDAP_GROUPDNPATTERN("metastore.authentication.ldap.groupDNPattern",
            "hive.metastore.authentication.ldap.groupDNPattern", "",
"COLON-separated list of patterns to use to find DNs for group entities in this directory.\n" +
        "Use %s where the actual group name is to be substituted for.\n" +
        "For example: CN=%s,CN=Groups,DC=subdomain,DC=domain,DC=com."),
    METASTORE_PLAIN_LDAP_GROUPFILTER("metastore.authentication.ldap.groupFilter",
            "hive.metastore.authentication.ldap.groupFilter", "",
"COMMA-separated list of LDAP Group names (short name not full DNs).\n" +
        "For example: HiveAdmins,HadoopAdmins,Administrators"),
    METASTORE_PLAIN_LDAP_USERDNPATTERN("metastore.authentication.ldap.userDNPattern",
            "hive.metastore.authentication.ldap.userDNPattern", "",
"COLON-separated list of patterns to use to find DNs for users in this directory.\n" +
        "Use %s where the actual group name is to be substituted for.\n" +
        "For example: CN=%s,CN=Users,DC=subdomain,DC=domain,DC=com."),
    METASTORE_PLAIN_LDAP_USERFILTER("metastore.authentication.ldap.userFilter",
            "hive.metastore.authentication.ldap.userFilter", "",
"COMMA-separated list of LDAP usernames (just short names, not full DNs).\n" +
        "For example: hiveuser,impalauser,hiveadmin,hadoopadmin"),
    METASTORE_PLAIN_LDAP_GUIDKEY("metastore.authentication.ldap.guidKey",
            "hive.metastore.authentication.ldap.guidKey", "uid",
            "LDAP attribute name whose values are unique in this LDAP server.\n" +
                    "For example: uid or CN."),
    METASTORE_PLAIN_LDAP_GROUPMEMBERSHIP_KEY("metastore.authentication.ldap.groupMembershipKey",
            "hive.metastore.authentication.ldap.groupMembershipKey",
            "member",
    "LDAP attribute name on the group object that contains the list of distinguished names\n" +
            "for the user, group, and contact objects that are members of the group.\n" +
            "For example: member, uniqueMember or memberUid"),
    METASTORE_PLAIN_LDAP_USERMEMBERSHIP_KEY(METASTORE_AUTHENTICATION_LDAP_USERMEMBERSHIPKEY_NAME,
            "hive." + METASTORE_AUTHENTICATION_LDAP_USERMEMBERSHIPKEY_NAME,
            "",
            "LDAP attribute name on the user object that contains groups of which the user is\n" +
                    "a direct member, except for the primary group, which is represented by the\n" +
                    "primaryGroupId.\n" +
                    "For example: memberOf"),
    METASTORE_PLAIN_LDAP_GROUPCLASS_KEY("metastore.authentication.ldap.groupClassKey",
            "hive.metastore.authentication.ldap.groupClassKey",
            "groupOfNames",
    "LDAP attribute name on the group entry that is to be used in LDAP group searches.\n" +
            "For example: group, groupOfNames or groupOfUniqueNames."),
    METASTORE_PLAIN_LDAP_CUSTOMLDAPQUERY("metastore.authentication.ldap.customLDAPQuery",
            "hive.metastore.authentication.ldap.customLDAPQuery", "",
    "A full LDAP query that LDAP Atn provider uses to execute against LDAP Server.\n" +
            "If this query returns a null resultset, the LDAP Provider fails the Authentication\n" +
            "request, succeeds if the user is part of the resultset." +
            "For example: (&(objectClass=group)(objectClass=top)(instanceType=4)(cn=Domain*)) \n" +
            "(&(objectClass=person)(|(sAMAccountName=admin)(|(memberOf=CN=Domain Admins,CN=Users,DC=domain,DC=com)" +
            "(memberOf=CN=Administrators,CN=Builtin,DC=domain,DC=com))))"),
    METASTORE_PLAIN_LDAP_USERSEARCHFILTER("metastore.authentication.ldap.userSearchFilter",
        "hive.metastore.authentication.ldap.userSearchFilter", "",
        "User search filter to be used with baseDN to search for users\n" +
            "For example: (&(uid={0})(objectClass=person))"),
    METASTORE_PLAIN_LDAP_GROUPBASEDN("metastore.authentication.ldap.groupBaseDN",
        "hive.metastore.authentication.ldap.groupBaseDN", "",
        "BaseDN for Group Search. This is used in conjunction with metastore.authentication.ldap.baseDN\n" +
            "and \n" +
            "request, succeeds if the group is part of the resultset."),
    METASTORE_PLAIN_LDAP_GROUPSEARCHFILTER("metastore.authentication.ldap.groupSearchFilter",
        "hive.metastore.authentication.ldap.groupSearchFilter", "",
        "Group search filter to be used with baseDN, userSearchFilter, groupBaseDN to search for users in groups\n" +
            "For example: (&(|(memberUid={0})(memberUid={1}))(objectClass=posixGroup))\n"),
    METASTORE_PLAIN_LDAP_BIND_USER("metastore.authentication.ldap.binddn",
            "hive.metastore.authentication.ldap.binddn", "",
"The user with which to bind to the LDAP server, and search for the full domain name " +
        "of the user being authenticated.\n" +
        "This should be the full domain name of the user, and should have search access across all " +
        "users in the LDAP tree.\n" +
        "If not specified, then the user being authenticated will be used as the bind user.\n" +
        "For example: CN=bindUser,CN=Users,DC=subdomain,DC=domain,DC=com"),
    METASTORE_PLAIN_LDAP_BIND_PASSWORD("metastore.authentication.ldap.bindpw",
            "hive.metastore.authentication.ldap.bindpw", "",
"The password for the bind user, to be used to search for the full name of the user being authenticated.\n" +
        "If the username is specified, this parameter must also be specified."),
    LIMIT_PARTITION_REQUEST("metastore.limit.partition.request",
        "hive.metastore.limit.partition.request", -1,
        "This limits the number of partitions (whole partition objects) that can be requested " +
        "from the metastore for a give table. MetaStore API methods using this are: \n" +
                "get_partitions, \n" +
                "get_partitions_by_names, \n" +
                "get_partitions_with_auth, \n" +
                "get_partitions_by_filter, \n" +
                "get_partitions_spec_by_filter, \n" +
                "get_partitions_by_expr,\n" +
                "get_partitions_ps,\n" +
                "get_partitions_ps_with_auth.\n" +
            "The default value \"-1\" means no limit."),
    MSC_CACHE_ENABLED("metastore.client.cache.v2.enabled",
            "hive.metastore.client.cache.v2.enabled", true,
            "This property enables a Caffeine Cache for Metastore client"),
    MSC_CACHE_MAX_SIZE("metastore.client.cache.v2.maxSize",
            "hive.metastore.client.cache.v2.maxSize", "1Gb", new SizeValidator(),
            "Set the maximum size (number of bytes) of the metastore client cache (DEFAULT: 1GB). " +
                    "Only in effect when the cache is enabled"),
    MSC_CACHE_RECORD_STATS("metastore.client.cache.v2.recordStats",
            "hive.metastore.client.cache.v2.recordStats", false,
            "This property enables recording metastore client cache stats in DEBUG logs"),
    LOG4J_FILE("metastore.log4j.file", "hive.log4j.file", "",
        "Hive log4j configuration file.\n" +
            "If the property is not set, then logging will be initialized using metastore-log4j2.properties found on the classpath.\n" +
            "If the property is set, the value must be a valid URI (java.net.URI, e.g. \"file:///tmp/my-logging.xml\"), \n" +
            "which you can then extract a URL from and pass to PropertyConfigurator.configure(URL)."),
    MANAGER_FACTORY_CLASS("javax.jdo.PersistenceManagerFactoryClass",
        "javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory",
        "class implementing the jdo persistence"),
    MATERIALIZATIONS_INVALIDATION_CACHE_IMPL("metastore.materializations.invalidation.impl",
        "hive.metastore.materializations.invalidation.impl", "DEFAULT",
        new StringSetValidator("DEFAULT", "DISABLE"),
        "The implementation that we should use for the materializations invalidation cache. \n" +
            "  DEFAULT: Default implementation for invalidation cache\n" +
            "  DISABLE: Disable invalidation cache (debugging purposes)"),
    MATERIALIZATIONS_INVALIDATION_CACHE_CLEAN_FREQUENCY("metastore.materializations.invalidation.clean.frequency",
         "hive.metastore.materializations.invalidation.clean.frequency",
         3600, TimeUnit.SECONDS, "Frequency at which timer task runs to remove unnecessary transaction entries from" +
          "materializations invalidation cache."),
    MATERIALIZATIONS_INVALIDATION_CACHE_EXPIRY_DURATION("metastore.materializations.invalidation.max.duration",
         "hive.metastore.materializations.invalidation.max.duration",
         86400, TimeUnit.SECONDS, "Maximum duration for query producing a materialization. After this time, transaction" +
         "entries that are not relevant for materializations can be removed from invalidation cache."),

    RUNTIME_STATS_CLEAN_FREQUENCY("metastore.runtime.stats.clean.frequency", "hive.metastore.runtime.stats.clean.frequency", 3600,
        TimeUnit.SECONDS, "Frequency at which timer task runs to remove outdated runtime stat entries."),
    RUNTIME_STATS_MAX_AGE("metastore.runtime.stats.max.age", "hive.metastore.runtime.stats.max.age", 86400 * 3, TimeUnit.SECONDS,
        "Stat entries which are older than this are removed."),

    SCHEDULED_QUERIES_ENABLED("metastore.scheduled.queries.enabled", "hive.metastore.scheduled.queries.enabled", true,
        "Wheter scheduled query metastore requests be processed"),
    SCHEDULED_QUERIES_EXECUTION_PROGRESS_TIMEOUT("metastore.scheduled.queries.execution.timeout",
        "hive.metastore.scheduled.queries.progress.timeout", 120, TimeUnit.SECONDS,
        "If a scheduled query is not making progress for this amount of time it will be considered TIMED_OUT"),
    SCHEDULED_QUERIES_EXECUTION_MAINT_TASK_FREQUENCY("metastore.scheduled.queries.execution.maint.task.frequency",
        "hive.metastore.scheduled.queries.execution.clean.frequency", 60, TimeUnit.SECONDS,
        "Interval of scheduled query maintenance task. Which removes executions above max age;"
            + "and marks executions as timed out if the condition is met"),
    SCHEDULED_QUERIES_EXECUTION_MAX_AGE("metastore.scheduled.queries.execution.max.age",
        "hive.metastore.scheduled.queries.execution.max.age", 30 * 86400, TimeUnit.SECONDS,
        "Maximal age of a scheduled query execution entry before it is removed."),

    SCHEDULED_QUERIES_AUTODISABLE_COUNT("metastore.scheduled.queries.autodisable.count",
        "metastore.scheduled.queries.autodisable.count", 0,
        "Scheduled queries will be automatically disabled after this number of consecutive failures."
            + "Setting it to a non-positive number disables the feature."),

    SCHEDULED_QUERIES_SKIP_OPPORTUNITIES_AFTER_FAILURES(
        "metastore.scheduled.queries.skip.opportunities.after.failures",
        "metastore.scheduled.queries.skip.opportunities.after.failures", 0,
        "Causes to skip schedule opportunities after consequitive failures; taking into account the last N executions. For a scheduled query which have failed its last f execution; it's next schedule will be set to skip f-1 schedule opportunitites."
            + "Suppose that a scheduled query is scheduled to run every minute.\n"
            + "Consider this setting to be set to 3 - which means it will only look at the last 3 executions."
            + "In case the query failed at 1:00 then it will skip 0 opportunities; and the next execution will be scheduled to 1:01\n"
            + "If that execution also fails it will skip 1 opportunities; next execution will happen at 1:03\n"
            + "In case that execution fails as well it will skip 2 opportunities; so next execution will be 1:06."
            + "If the query fails it will skip 2 opportunities again ; because it only cares with the last 3 executions based on the set value."),

    // Parameters for exporting metadata on table drop (requires the use of the)
    // org.apache.hadoop.hive.ql.parse.MetaDataExportListener preevent listener
    METADATA_EXPORT_LOCATION("metastore.metadata.export.location", "hive.metadata.export.location",
        "",
        "When used in conjunction with the org.apache.hadoop.hive.ql.parse.MetaDataExportListener pre event listener, \n" +
            "it is the location to which the metadata will be exported. The default is an empty string, which results in the \n" +
            "metadata being exported to the current user's home directory on HDFS."),
    METASTORE_MAX_EVENT_RESPONSE("metastore.max.event.response", "hive.metastore.max.event.response", 1000000,
        "The parameter will decide the maximum number of events that HMS will respond."),
    METASTORE_CLIENT_FIELD_SCHEMA_FOR_PARTITIONS("metastore.client.skip.columns.for.partitions",
            "hive.metastore.client.skip.columns.for.partitions", false,
            "Config to disable field schema for partitions. Currently all the partitions in a \n"
                    + "table carries the field schema that is same as that of table schema. For a table with \n"
                    + "wider partitions fetching duplicated field schema in every partition increases memory footprint\n"
                    + "and thrift communication timeout errors. Set this config to 'true' to ignore column schema in partitions."),
    METASTORE_PARTITIONS_PARAMETERS_EXCLUDE_PATTERN("metastore.partitions.parameters.exclude.pattern",
        "hive.metastore.partitions.parameters.exclude.pattern", "",
        "SQL pattern used to exclude the matched parameters for get-partitions APIs.\n"
            + "Any key-value pair from parameters whose key matches with the pattern will be excluded from the partitions.\n"
            + "This property doesn't work for the temporary table."),
    METASTORE_PARTITIONS_PARAMETERS_INCLUDE_PATTERN("metastore.partitions.parameters.include.pattern",
        "hive.metastore.partitions.parameters.include.pattern", "",
        "SQL pattern used to select the matched parameters for get-partitions APIs.\n"
            + "Any key-value pair from parameters whose key matches with the pattern will be included in the partitions.\n"
            + "This property doesn't work for the temporary table."),
    METASTORE_CLIENT_FILTER_ENABLED("metastore.client.filter.enabled", "hive.metastore.client.filter.enabled", true,
        "Enable filtering the metadata read results at HMS client. Default is true."),
    METASTORE_SERVER_FILTER_ENABLED("metastore.server.filter.enabled", "hive.metastore.server.filter.enabled", false,
        "Enable filtering the metadata read results at HMS server. Default is false."),
    MOVE_EXPORTED_METADATA_TO_TRASH("metastore.metadata.move.exported.metadata.to.trash",
        "hive.metadata.move.exported.metadata.to.trash", true,
        "When used in conjunction with the org.apache.hadoop.hive.ql.parse.MetaDataExportListener pre event listener, \n" +
            "this setting determines if the metadata that is exported will subsequently be moved to the user's trash directory \n" +
            "alongside the dropped table data. This ensures that the metadata will be cleaned up along with the dropped table data."),
    METRICS_ENABLED("metastore.metrics.enabled", "hive.metastore.metrics.enabled", false,
        "Enable metrics on the metastore."),
    METRICS_CLASS("metastore.metrics.class", "hive.service.metrics.class", "org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics",
        new StringSetValidator(
            "org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics",
            "org.apache.hadoop.hive.common.metrics.LegacyMetrics"),
        "Hive metrics subsystem implementation class."),
    METRICS_HADOOP2_COMPONENT_NAME("metastore.metrics.hadoop2.component", "hive.service.metrics.hadoop2.component", "hivemetastore",
                    "Component name to provide to Hadoop2 Metrics system."),
    METRICS_HADOOP2_INTERVAL("metastore.metrics.hadoop2.component", "hive.service.metrics.hadoop2.frequency", 30,
        TimeUnit.SECONDS, "For metric class org.apache.hadoop.hive.common.metrics.metrics2.Metrics2Reporter " +
        " the frequency of updating the HADOOP2 metrics system."),
    METRICS_JSON_FILE_INTERVAL("metastore.metrics.file.frequency",
        "hive.service.metrics.file.frequency", 60000, TimeUnit.MILLISECONDS,
        "For json metric reporter, the frequency of updating JSON metrics file."),
    METRICS_JSON_FILE_LOCATION("metastore.metrics.file.location",
        "hive.service.metrics.file.location", "/tmp/report.json",
        "For metric class json metric reporter, the location of local JSON metrics file.  " +
            "This file will get overwritten at every interval."),
    METRICS_SLF4J_LOG_FREQUENCY_MINS("metastore.metrics.slf4j.frequency",
        "hive.service.metrics.slf4j.frequency", 5, TimeUnit.MINUTES,
        "For SLF4J metric reporter, the frequency of logging metrics events. The default value is 5 mins."),
    METRICS_SLF4J_LOG_LEVEL("metastore.metrics.slf4j.logging.level",
        "hive.service.metrics.slf4j.logging.level", "INFO",
        new StringSetValidator("TRACE", "DEBUG", "INFO", "WARN", "ERROR"),
        "For SLF4J metric reporter, the logging level to be used for metrics event logs. The default level is INFO."),
    METRICS_REPORTERS("metastore.metrics.reporters", "metastore.metrics.reporters", "json,jmx",
        new StringSetValidator("json", "jmx", "console", "hadoop", "slf4j"),
        "A comma separated list of metrics reporters to start"),
    MSCK_PATH_VALIDATION("metastore.msck.path.validation", "hive.msck.path.validation", "throw",
      new StringSetValidator("throw", "skip", "ignore"), "The approach msck should take with HDFS " +
      "directories that are partition-like but contain unsupported characters. 'throw' (an " +
      "exception) is the default; 'skip' will skip the invalid directories and still repair the" +
      " others; 'ignore' will skip the validation (legacy behavior, causes bugs in many cases)"),
    MSCK_REPAIR_BATCH_SIZE("metastore.msck.repair.batch.size",
      "hive.msck.repair.batch.size", 3000,
      "Batch size for the msck repair command. If the value is greater than zero,\n "
        + "it will execute batch wise with the configured batch size. In case of errors while\n"
        + "adding unknown partitions the batch size is automatically reduced by half in the subsequent\n"
        + "retry attempt. The default value is 3000 which means it will execute in the batches of 3000."),
    MSCK_REPAIR_BATCH_MAX_RETRIES("metastore.msck.repair.batch.max.retries", "hive.msck.repair.batch.max.retries", 4,
      "Maximum number of retries for the msck repair command when adding unknown partitions.\n "
        + "If the value is greater than zero it will retry adding unknown partitions until the maximum\n"
        + "number of attempts is reached or batch size is reduced to 0, whichever is earlier.\n"
        + "In each retry attempt it will reduce the batch size by a factor of 2 until it reaches zero.\n"
        + "If the value is set to zero it will retry until the batch size becomes zero as described above."),
    MSCK_REPAIR_ENABLE_PARTITION_RETENTION("metastore.msck.repair.enable.partition.retention",
      "metastore.msck.repair.enable.partition.retention", false,
      "If 'partition.retention.period' table property is set, this flag determines whether MSCK REPAIR\n" +
      "command should handle partition retention. If enabled, and if a specific partition's age exceeded\n" +
      "retention period the partition will be dropped along with data"),

    GETPARTITIONS_BATCH_MAX_RETRIES("metastore.getpartitions.batch.max.retries",
      "hive.getpartitions.batch.max.retries", 5,
      "Maximum number of retries for the Hive.GetAllPartitionsOf() when getting partitions in batches.\n "
      + "If the value is greater than zero it will retry getting partitions until the maximum\n"
      + "number of attempts is reached or batch size is reduced to 0, whichever is earlier.\n"
      + "In each retry attempt it will reduce the batch size by a factor of 2 until it reaches zero.\n"
      + "If the value is set to zero it will retry until the batch size becomes zero as described above."),


    // Partition management task params
    PARTITION_MANAGEMENT_TASK_FREQUENCY("metastore.partition.management.task.frequency",
      "metastore.partition.management.task.frequency",
      6, TimeUnit.HOURS, "Frequency at which timer task runs to do automatic partition management for tables\n" +
      "with table property 'discover.partitions'='true'. Partition management include 2 pieces. One is partition\n" +
      "discovery and other is partition retention period. When 'discover.partitions'='true' is set, partition\n" +
      "management will look for partitions in table location and add partitions objects for it in metastore.\n" +
      "Similarly if partition object exists in metastore and partition location does not exist, partition object\n" +
      "will be dropped. The second piece in partition management is retention period. When 'discover.partition'\n" +
      "is set to true and if 'partition.retention.period' table property is defined, partitions that are older\n" +
      "than the specified retention period will be automatically dropped from metastore along with the data."),
    PARTITION_MANAGEMENT_TABLE_TYPES("metastore.partition.management.table.types",
      "metastore.partition.management.table.types", "MANAGED_TABLE,EXTERNAL_TABLE",
      "Comma separated list of table types to use for partition management"),
    PARTITION_MANAGEMENT_TASK_THREAD_POOL_SIZE("metastore.partition.management.task.thread.pool.size",
      "metastore.partition.management.task.thread.pool.size", 3,
      "Partition management uses thread pool on to which tasks are submitted for discovering and retaining the\n" +
      "partitions. This determines the size of the thread pool. Note: Increasing the thread pool size will cause\n" +
      "threadPoolSize * maxConnectionPoolSize connections to backend db"),
    PARTITION_MANAGEMENT_CATALOG_NAME("metastore.partition.management.catalog.name",
      "metastore.partition.management.catalog.name", "hive",
      "Automatic partition management will look for tables under the specified catalog name"),
    PARTITION_MANAGEMENT_DATABASE_PATTERN("metastore.partition.management.database.pattern",
      "metastore.partition.management.database.pattern", "*",
      "Automatic partition management will look for tables using the specified database pattern"),
    PARTITION_MANAGEMENT_TABLE_PATTERN("metastore.partition.management.table.pattern",
      "metastore.partition.management.table.pattern", "*",
      "Automatic partition management will look for tables using the specified table pattern"),

    METASTORE_METADATA_TRANSFORMER_CLASS("metastore.metadata.transformer.class", "metastore.metadata.transformer.class",
        "org.apache.hadoop.hive.metastore.MetastoreDefaultTransformer",
        "Fully qualified class name for the metastore metadata transformer class \n"
            + "which is used by HMS Server to fetch the extended tables/partitions information \n"
            + "based on the data processor capabilities \n"
            + " This class should implement the IMetaStoreMetadataTransformer interface"),
    METASTORE_METADATA_TRANSFORMER_TRANSLATED_TO_EXTERNAL_FOLLOWS_RENAMES(
        "metastore.metadata.transformer.translated.to.external.follows.renames",
        "metastore.metadata.transformer.translated.to.external.follows.renames", true,
        "Wether TRANSLATED_TO_EXTERNAL tables should follow renames. In case the default directory exists "
            + "the strategy of metastore.metadata.transformer.location.mode is used"),
    METASTORE_METADATA_TRANSFORMER_LOCATION_MODE("metastore.metadata.transformer.location.mode",
        "metastore.metadata.transformer.location.mode", "force",
        new StringSetValidator("seqsuffix", "seqprefix", "prohibit", "force"),
        "Defines the strategy to use in case the default location for a translated table already exists.\n"
            + "  seqsuffix: add a '_N' suffix to the table name to get a unique location (table,table_1,table_2,...)\n"
            + "  seqprefix: adds a 'N_' prefix to the table name to get a unique location (table,1_table,2_table,...)\n"
            + "  prohibit: do not consider alternate locations; throw error if the default is not available\n"
            + "  force: use the default location even in case the directory is already available"),

    MULTITHREADED("javax.jdo.option.Multithreaded", "javax.jdo.option.Multithreaded", true,
        "Set this to true if multiple threads access metastore through JDO concurrently."),
    MAX_OPEN_TXNS("metastore.max.open.txns", "hive.max.open.txns", 100000,
        "Maximum number of open transactions. If \n" +
        "current open transactions reach this limit, future open transaction requests will be \n" +
        "rejected, until this number goes below the limit."),
    NON_TRANSACTIONAL_READ("javax.jdo.option.NonTransactionalRead",
        "javax.jdo.option.NonTransactionalRead", true,
        "Reads outside of transactions"),
    NOTIFICATION_SEQUENCE_LOCK_MAX_RETRIES("metastore.notification.sequence.lock.max.retries",
        "hive.notification.sequence.lock.max.retries", 10,
        "Number of retries required to acquire a lock when getting the next notification sequential ID for entries "
            + "in the NOTIFICATION_LOG table."),
    NOTIFICATION_SEQUENCE_LOCK_RETRY_SLEEP_INTERVAL(
        "metastore.notification.sequence.lock.retry.sleep.interval",
        "hive.notification.sequence.lock.retry.sleep.interval", 10, TimeUnit.SECONDS,
        "Sleep interval between retries to acquire a notification lock as described part of property "
            + NOTIFICATION_SEQUENCE_LOCK_MAX_RETRIES.name()),
    NOTIFICATION_ALTER_PARTITIONS_V2_ENABLED("metastore.alterPartitions.notification.v2.enabled",
        "hive.metastore.alterPartitions.notification.v2.enabled", true,
        "Should send a single notification event on alter partitions. " +
            "This property is for ensuring backward compatibility when it sets to false, " +
            "HMS will send an old ALTER_PARTITION event per partition, so downstream consumers can " +
            "still process the ALTER_PARTITION event without making changes."),
    ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS("metastore.orm.retrieveMapNullsAsEmptyStrings",
        "hive.metastore.orm.retrieveMapNullsAsEmptyStrings",false,
        "Thrift does not support nulls in maps, so any nulls present in maps retrieved from ORM must " +
            "either be pruned or converted to empty strings. Some backing dbs such as Oracle persist empty strings " +
            "as nulls, so we should set this parameter if we wish to reverse that behaviour. For others, " +
            "pruning is the correct behaviour"),
    PARTITION_NAME_WHITELIST_PATTERN("metastore.partition.name.whitelist.pattern",
        "hive.metastore.partition.name.whitelist.pattern", "",
        "Partition names will be checked against this regex pattern and rejected if not matched."),
    PARTITION_ORDER_EXPR("metastore.partition.order.expr",
        "hive.metastore.partition.order.expr", "\"PART_NAME\" asc",
        "The default partition order if the metastore does not return all partitions. \n" +
            "It can be sorted based on any column in the PARTITIONS table (e.g., \"PARTITIONS\".\"CREATE_TIME\" desc, \"PARTITIONS\".\"LAST_ACCESS_TIME\" desc etc)"),
    PART_INHERIT_TBL_PROPS("metastore.partition.inherit.table.properties",
        "hive.metastore.partition.inherit.table.properties", "",
        "List of comma separated keys occurring in table properties which will get inherited to newly created partitions. \n" +
            "* implies all the keys will get inherited."),
    PRE_EVENT_LISTENERS("metastore.pre.event.listeners", "hive.metastore.pre.event.listeners", "",
        "List of comma separated listeners for metastore events."),
    PWD("javax.jdo.option.ConnectionPassword", "javax.jdo.option.ConnectionPassword", "mine",
        "password to use against metastore database"),
    RAW_STORE_IMPL("metastore.rawstore.impl", "hive.metastore.rawstore.impl",
        "org.apache.hadoop.hive.metastore.ObjectStore",
        "Name of the class that implements org.apache.riven.rawstore interface. \n" +
            "This class is used to store and retrieval of raw metadata objects such as table, database"),
    REPLCMDIR("metastore.repl.cmrootdir", "hive.repl.cmrootdir", "/user/${system:user.name}/cmroot/",
        "Root dir for ChangeManager, used for deleted files."),
    REPLCMENCRYPTEDDIR("metastore.repl.cm.encryptionzone.rootdir", "hive.repl.cm.encryptionzone.rootdir", ".cmroot",
            "Root dir for ChangeManager if encryption zones are enabled, used for deleted files."),
    REPLCMFALLBACKNONENCRYPTEDDIR("metastore.repl.cm.nonencryptionzone.rootdir",
            "hive.repl.cm.nonencryptionzone.rootdir", "",
            "Root dir for ChangeManager for non encrypted paths if hive.repl.cmrootdir is encrypted."),
    REPLCMRETIAN("metastore.repl.cm.retain", "hive.repl.cm.retain",  24 * 10, TimeUnit.HOURS,
        "Time to retain removed files in cmrootdir."),
    REPLCMINTERVAL("metastore.repl.cm.interval", "hive.repl.cm.interval", 3600, TimeUnit.SECONDS,
        "Inteval for cmroot cleanup thread."),
    REPLCMENABLED("metastore.repl.cm.enabled", "hive.repl.cm.enabled", false,
        "Turn on ChangeManager, so delete files will go to cmrootdir."),
    REPLDIR("metastore.repl.rootdir", "hive.repl.rootdir", "/user/${system:user.name}/repl/",
        "HDFS root dir for all replication dumps."),
    REPL_COPYFILE_MAXNUMFILES("metastore.repl.copyfile.maxnumfiles",
        "hive.exec.copyfile.maxnumfiles", 1L,
        "Maximum number of files Hive uses to do sequential HDFS copies between directories." +
            "Distributed copies (distcp) will be used instead for larger numbers of files so that copies can be done faster."),
    REPL_COPYFILE_MAXSIZE("metastore.repl.copyfile.maxsize",
        "hive.exec.copyfile.maxsize", 32L * 1024 * 1024 /*32M*/,
        "Maximum file size (in bytes) that Hive uses to do single HDFS copies between directories." +
            "Distributed copies (distcp) will be used instead for bigger files so that copies can be done faster."),
    REPL_EVENT_DB_LISTENER_TTL("metastore.repl.event.db.listener.timetolive",
            "hive.repl.event.db.listener.timetolive", 10, TimeUnit.DAYS,
            "time after which events will be removed from the database listener queue when repl.cm.enabled \n" +
                    "is set to true. When set to false, the conf event.db.listener.timetolive is used instead."),
    REPL_METRICS_CACHE_MAXSIZE("metastore.repl.metrics.cache.maxsize",
      "hive.repl.metrics.cache.maxsize", 10000 /*10000 rows */,
      "Maximum in memory cache size to collect replication metrics. The metrics will be pushed to persistent"
        + " storage at a frequency defined by config hive.repl.metrics.update.frequency. Till metrics are persisted to"
        + " db, it will be stored in this cache. So set this property based on number of concurrent policies running "
        + " and the frequency of persisting the metrics to persistent storage. "
      ),
    REPL_METRICS_UPDATE_FREQUENCY("metastore.repl.metrics.update.frequency",
      "hive.repl.metrics.update.frequency", 1L, TimeUnit.MINUTES /*1 minute */,
      "Frequency at which replication Metrics will be stored in persistent storage. "
    ),
    REPL_METRICS_CLEANUP_FREQUENCY("metastore.repl.metrics.cleanup.frequency",
      "hive.metastore.repl.metrics.cleanup.frequency", 1, TimeUnit.DAYS,
      "Interval of scheduled metrics clean up task which removes metrics above max age; Max age is"
        + " defined by the config metastore.repl.metrics.max.age. The max age should be greater than this frequency"),
    REPL_METRICS_MAX_AGE("metastore.repl.metrics.max.age",
      "hive.metastore.repl.metrics.max.age", 7, TimeUnit.DAYS,
      "Maximal age of a replication metrics entry before it is removed."),
    REPL_TXN_TIMEOUT("metastore.repl.txn.timeout", "hive.repl.txn.timeout", 11, TimeUnit.DAYS,
      "Time after which replication transactions are declared aborted if the client has not sent a " +
              "heartbeat. If this is a target cluster, value must be greater than" +
              "hive.repl.event.db.listener.timetolive on the source cluster (!), ideally by 1 day."),
    SCHEMA_INFO_CLASS("metastore.schema.info.class", "hive.metastore.schema.info.class",
        "org.apache.hadoop.hive.metastore.MetaStoreSchemaInfo",
        "Fully qualified class name for the metastore schema information class \n"
            + "which is used by schematool to fetch the schema information.\n"
            + " This class should implement the IMetaStoreSchemaInfo interface"),
    SCHEMA_VERIFICATION("metastore.schema.verification", "hive.metastore.schema.verification", true,
        "Enforce metastore schema version consistency.\n" +
        "True: Verify that version information stored in is compatible with one from Hive jars.  Also disable automatic\n" +
        "      schema migration attempt. Users are required to manually migrate schema after Hive upgrade which ensures\n" +
        "      proper metastore schema migration. (Default)\n" +
        "False: Warn if the version information stored in metastore doesn't match with one from in Hive jars."),
    SCHEMA_VERIFICATION_RECORD_VERSION("metastore.schema.verification.record.version",
        "hive.metastore.schema.verification.record.version", false,
        "When true the current MS version is recorded in the VERSION table. If this is disabled and verification is\n" +
            " enabled the MS will be unusable."),
    SERDES_USING_METASTORE_FOR_SCHEMA("metastore.serdes.using.metastore.for.schema",
        "hive.serdes.using.metastore.for.schema",
        "org.apache.hadoop.hive.ql.io.orc.OrcSerde," +
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe," +
            "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe," +
            "org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe," +
            "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe," +
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe," +
            "org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe," +
            "org.apache.hadoop.hive.serde2.OpenCSVSerde," +
            "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
        "SerDes retrieving schema from metastore. This is an internal parameter."),
    SERDES_WITHOUT_FROM_DESERIALIZER("metastore.serdes.without.from.deserializer",
        "hive.metastore.serdes.without.from.deserializer",
        "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
        "SerDes which are providing the schema but do not need the 'from deserializer' comment for the columns."),
    SERVER_MAX_MESSAGE_SIZE("metastore.server.max.message.size",
        "hive.metastore.server.max.message.size", 100*1024*1024L,
        "Maximum message size in bytes a HMS will accept."),
    SERVER_MAX_THREADS("metastore.server.max.threads",
        "hive.metastore.server.max.threads", 1000,
        "Maximum number of worker threads in the Thrift server's pool."),
    SERVER_MIN_THREADS("metastore.server.min.threads", "hive.metastore.server.min.threads", 200,
        "Minimum number of worker threads in the Thrift server's pool."),
    SERVER_PORT("metastore.thrift.port", "hive.metastore.port", 9083,
        "Hive metastore listener port"),
    SSL_KEYSTORE_PASSWORD("metastore.keystore.password", "hive.metastore.keystore.password", "",
        "Metastore SSL certificate keystore password."),
    SSL_KEYSTORE_PATH("metastore.keystore.path", "hive.metastore.keystore.path", "",
        "Metastore SSL certificate keystore location."),
    SSL_KEYSTORE_TYPE("metastore.keystore.type", "hive.metastore.keystore.type", "",
            "Metastore SSL certificate keystore type."),
    SSL_KEYMANAGERFACTORY_ALGORITHM("metastore.keymanagerfactory.algorithm", "hive.metastore.keymanagerfactory.algorithm", "",
            "Metastore SSL certificate keystore algorithm."),
    SSL_PROTOCOL_BLACKLIST("metastore.ssl.protocol.blacklist", "hive.ssl.protocol.blacklist",
        "SSLv2,SSLv3", "SSL Versions to disable for all Hive Servers"),
    SSL_TRUSTSTORE_PATH("metastore.truststore.path", "hive.metastore.truststore.path", "",
        "Metastore SSL certificate truststore location."),
    SSL_TRUSTSTORE_PASSWORD("metastore.truststore.password", "hive.metastore.truststore.password", "",
        "Metastore SSL certificate truststore password."),
    SSL_TRUSTSTORE_TYPE("metastore.truststore.type", "hive.metastore.truststore.type", "",
            "Metastore SSL certificate truststore type."),
    SSL_TRUSTMANAGERFACTORY_ALGORITHM("metastore.trustmanagerfactory.algorithm", "hive.metastore.trustmanagerfactory.algorithm", "",
            "Metastore SSL certificate truststore algorithm."),
    STATS_AUTO_GATHER("metastore.stats.autogather", "hive.stats.autogather", true,
        "A flag to gather statistics (only basic) automatically during the INSERT OVERWRITE command."),
    STATS_FETCH_BITVECTOR("metastore.stats.fetch.bitvector", "hive.stats.fetch.bitvector", false,
        "Whether we fetch bitvector when we compute ndv. Users can turn it off if they want to use old schema"),
    STATS_FETCH_KLL("metastore.stats.fetch.kll", "hive.stats.fetch.kll", false,
        "Whether we fetch KLL data sketches to enable histogram statistics"),
    STATS_NDV_TUNER("metastore.stats.ndv.tuner", "hive.metastore.stats.ndv.tuner", 0.0,
        "Provides a tunable parameter between the lower bound and the higher bound of ndv for aggregate ndv across all the partitions. \n" +
            "The lower bound is equal to the maximum of ndv of all the partitions. The higher bound is equal to the sum of ndv of all the partitions.\n" +
            "Its value should be between 0.0 (i.e., choose lower bound) and 1.0 (i.e., choose higher bound)"),
    STATS_NDV_DENSITY_FUNCTION("metastore.stats.ndv.densityfunction",
        "hive.metastore.stats.ndv.densityfunction", false,
        "Whether to use density function to estimate the NDV for the whole table based on the NDV of partitions"),
    STATS_DEFAULT_AGGREGATOR("metastore.stats.default.aggregator", "hive.stats.default.aggregator",
        "",
        "The Java class (implementing the StatsAggregator interface) that is used by default if hive.stats.dbclass is custom type."),
    STATS_DEFAULT_PUBLISHER("metastore.stats.default.publisher", "hive.stats.default.publisher", "",
        "The Java class (implementing the StatsPublisher interface) that is used by default if hive.stats.dbclass is custom type."),
    STATS_AUTO_UPDATE("metastore.stats.auto.analyze", "hive.metastore.stats.auto.analyze", "none",
        new EnumValidator(StatsUpdateMode.values()),
        "Whether to update stats in the background; none - no, all - for all tables, existing - only existing, out of date, stats."),
    STATS_AUTO_UPDATE_NOOP_WAIT("metastore.stats.auto.analyze.noop.wait",
        "hive.metastore.stats.auto.analyze.noop.wait", 5L, TimeUnit.MINUTES,
        new TimeValidator(TimeUnit.MINUTES),
        "How long to sleep if there were no stats needing update during an update iteration.\n" +
        "This is a setting to throttle table/partition checks when nothing is being changed; not\n" +
        "the analyze queries themselves."),
    STATS_AUTO_UPDATE_WORKER_COUNT("metastore.stats.auto.analyze.worker.count",
        "hive.metastore.stats.auto.analyze.worker.count", 1,
        "Number of parallel analyze commands to run for background stats update."),
    STORAGE_SCHEMA_READER_IMPL("metastore.storage.schema.reader.impl", "metastore.storage.schema.reader.impl",
        SERDE_STORAGE_SCHEMA_READER_CLASS,
        "The class to use to read schemas from storage.  It must implement " +
        "org.apache.hadoop.hive.metastore.StorageSchemaReader"),
    STORE_MANAGER_TYPE("datanucleus.storeManagerType", "datanucleus.storeManagerType", "rdbms", "metadata store type"),
    STRICT_MANAGED_TABLES("metastore.strict.managed.tables", "hive.strict.managed.tables", false,
            "Whether strict managed tables mode is enabled. With this mode enabled, " +
            "only transactional tables (both full and insert-only) are allowed to be created as managed tables"),
    SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES("metastore.support.special.characters.tablename",
        "hive.support.special.characters.tablename", true,
        "This flag should be set to true to enable support for special characters in table names.\n"
            + "When it is set to false, only [a-zA-Z_0-9]+ are supported.\n"
            + "The supported special characters are %&'()*+,-./:;<=>?[]_|{}$^!~#@ and space. This flag applies only to"
            + " quoted table names.\nThe default value is true."),
    TASK_THREADS_ALWAYS("metastore.task.threads.always", "metastore.task.threads.always",
        EVENT_CLEANER_TASK_CLASS + "," + RUNTIME_STATS_CLEANER_TASK_CLASS + "," +
            ACID_METRICS_TASK_CLASS + "," + ACID_METRICS_LOGGER_CLASS + "," +
            "org.apache.hadoop.hive.metastore.HiveProtoEventsCleanerTask" + ","
            + "org.apache.hadoop.hive.metastore.ScheduledQueryExecutionsMaintTask" + ","
            + "org.apache.hadoop.hive.metastore.ReplicationMetricsMaintTask",
        "Comma separated list of tasks that will be started in separate threads.  These will " +
            "always be started, regardless of whether the metastore is running in embedded mode " +
            "or in server mode.  They must implement " + METASTORE_TASK_THREAD_CLASS),
    TASK_THREADS_REMOTE_ONLY("metastore.task.threads.remote", "metastore.task.threads.remote",
        ACID_HOUSEKEEPER_SERVICE_CLASS + "," +
                COMPACTION_HOUSEKEEPER_SERVICE_CLASS + "," +
            ACID_TXN_CLEANER_SERVICE_CLASS + "," +
            ACID_OPEN_TXNS_COUNTER_SERVICE_CLASS + "," +
            MATERIALZIATIONS_REBUILD_LOCK_CLEANER_TASK_CLASS + "," +
            PARTITION_MANAGEMENT_TASK_CLASS,
        "Comma-separated list of tasks that will be started in separate threads.  These will be" +
            " started only when the metastore is running as a separate service.  They must " +
            "implement " + METASTORE_TASK_THREAD_CLASS),
    THRIFT_TRANSPORT_MODE("metastore.server.thrift.transport.mode",
        "hive.metastore.server.thrift.transport.mode", "binary",
        "Transport mode for thrift server in Metastore. Can be binary or http"),
    THRIFT_HTTP_PATH("metastore.server.thrift.http.path",
        "hive.metastore.server.thrift.http.path",
        "metastore",
        "Path component of URL endpoint when in HTTP mode"),
    TCP_KEEP_ALIVE("metastore.server.tcp.keepalive",
        "hive.metastore.server.tcp.keepalive", true,
        "Whether to enable TCP keepalive for the metastore server. Keepalive will prevent accumulation of half-open connections."),
    THREAD_POOL_SIZE("metastore.thread.pool.size", "no.such", 15,
        "Number of threads in the thread pool.  These will be used to execute all background " +
            "processes."),
    THRIFT_CONNECTION_RETRIES("metastore.connect.retries", "hive.metastore.connect.retries", 3,
        "Number of retries while opening a connection to metastore"),
    THRIFT_FAILURE_RETRIES("metastore.failure.retries", "hive.metastore.failure.retries", 1,
        "Number of retries upon failure of Thrift metastore calls"),
    THRIFT_BIND_HOST("metastore.thrift.bind.host", "hive.metastore.thrift.bind.host", "",
        "Bind host on which to run the metastore thrift service."),
    THRIFT_URIS("metastore.thrift.uris", "hive.metastore.uris", "",
        "URIs Used by metastore client to connect to remotemetastore\n." +
                "If dynamic service discovery mode is set, the URIs are used to connect to the" +
                " corresponding service discovery servers e.g. a zookeeper. Otherwise they are " +
                "used as URIs for remote metastore."),
    THRIFT_METASTORE_CLIENT_MAX_MESSAGE_SIZE("metastore.thrift.client.max.message.size",
            "hive.thrift.client.max.message.size", "2147483647b",
            new SizeValidator(-1L, true, (long) Integer.MAX_VALUE, true),
            "Thrift client configuration for max message size. 0 or -1 will use the default defined in the Thrift " +
                    "library. The upper limit is 2147483647 bytes"),
    THRIFT_SERVICE_DISCOVERY_MODE("metastore.service.discovery.mode",
            "hive.metastore.service.discovery.mode",
            "",
            "Specifies which dynamic service discovery method to use. Currently we support only " +
                    "\"zookeeper\" to specify ZooKeeper based service discovery."),
    THRIFT_ZOOKEEPER_USE_KERBEROS("metastore.zookeeper.kerberos.enabled",
            "hive.zookeeper.kerberos.enabled", true,
            "If ZooKeeper is configured for Kerberos authentication. This could be useful when cluster\n" +
            "is kerberized, but Zookeeper is not."),
    THRIFT_ZOOKEEPER_CLIENT_PORT("metastore.zookeeper.client.port",
            "hive.zookeeper.client.port", "2181",
            "The port of ZooKeeper servers to talk to.\n" +
                    "If the list of Zookeeper servers specified in hive.metastore.thrift.uris" +
                    " does not contain port numbers, this value is used."),
    THRIFT_ZOOKEEPER_SESSION_TIMEOUT("metastore.zookeeper.session.timeout",
            "hive.zookeeper.session.timeout", 120000L, TimeUnit.MILLISECONDS,
            new TimeValidator(TimeUnit.MILLISECONDS),
            "ZooKeeper client's session timeout (in milliseconds). The client is disconnected\n" +
                    "if a heartbeat is not sent in the timeout."),
    THRIFT_ZOOKEEPER_CONNECTION_TIMEOUT("metastore.zookeeper.connection.timeout",
            "hive.zookeeper.connection.timeout", 15L, TimeUnit.SECONDS,
            new TimeValidator(TimeUnit.SECONDS),
            "ZooKeeper client's connection timeout in seconds. " +
                    "Connection timeout * hive.metastore.zookeeper.connection.max.retries\n" +
                    "with exponential backoff is when curator client deems connection is lost to zookeeper."),
    THRIFT_ZOOKEEPER_NAMESPACE("metastore.zookeeper.namespace",
            "hive.zookeeper.namespace", "hive_metastore",
            "The parent node under which all ZooKeeper nodes for metastores are created."),
    THRIFT_ZOOKEEPER_CONNECTION_MAX_RETRIES("metastore.zookeeper.connection.max.retries",
            "hive.zookeeper.connection.max.retries", 3,
            "Max number of times to retry when connecting to the ZooKeeper server."),
    THRIFT_ZOOKEEPER_CONNECTION_BASESLEEPTIME("metastore.zookeeper.connection.basesleeptime",
            "hive.zookeeper.connection.basesleeptime", 1000L, TimeUnit.MILLISECONDS,
            new TimeValidator(TimeUnit.MILLISECONDS),
            "Initial amount of time (in milliseconds) to wait between retries\n" +
                    "when connecting to the ZooKeeper server when using ExponentialBackoffRetry policy."),
    THRIFT_ZOOKEEPER_SSL_ENABLE("metastore.zookeeper.ssl.client.enable",
        "hive.zookeeper.ssl.client.enable", false,
        "Set client to use TLS when connecting to ZooKeeper.  An explicit value overrides any value set via the " +
            "zookeeper.client.secure system property (note the different name).  Defaults to false if neither is set."),
    THRIFT_ZOOKEEPER_SSL_KEYSTORE_LOCATION("metastore.zookeeper.ssl.keystore.location",
        "hive.zookeeper.ssl.keystore.location", "",
        "Keystore location when using a client-side certificate with TLS connectivity to ZooKeeper. " +
            "Overrides any explicit value set via the zookeeper.ssl.keyStore.location " +
            "system property (note the camelCase)."),
    THRIFT_ZOOKEEPER_SSL_KEYSTORE_PASSWORD("metastore.zookeeper.ssl.keystore.password",
        "hive.zookeeper.ssl.keystore.password", "",
        "Keystore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
            "Overrides any explicit value set via the zookeeper.ssl.keyStore.password" +
            "system property (note the camelCase)."),
    THRIFT_ZOOKEEPER_SSL_KEYSTORE_TYPE("metastore.zookeeper.ssl.keystore.type",
        "hive.zookeeper.ssl.keystore.type", "",
        "Keystore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
            "Overrides any explicit value set via the zookeeper.ssl.keyStore.type" +
            "system property (note the camelCase)."),
    THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION("metastore.zookeeper.ssl.truststore.location",
        "hive.zookeeper.ssl.truststore.location", "",
        "Truststore location when using a client-side certificate with TLS connectivity to ZooKeeper. " +
            "Overrides any explicit value set via the zookeeper.ssl.trustStore.location " +
            "system property (note the camelCase)."),
    THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD("metastore.zookeeper.ssl.truststore.password",
        "hive.zookeeper.ssl.truststore.password", "",
        "Truststore password when using a client-side certificate with TLS connectivity to ZooKeeper." +
            "Overrides any explicit value set via the zookeeper.ssl.trustStore.password " +
            "system property (note the camelCase)."),
    THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_TYPE("metastore.zookeeper.ssl.truststore.type",
        "hive.zookeeper.ssl.truststore.type", "",
        "Truststore type when using a client-side certificate with TLS connectivity to ZooKeeper." +
            "Overrides any explicit value set via the zookeeper.ssl.trustStore.type" +
            "system property (note the camelCase)."),
    THRIFT_URI_SELECTION("metastore.thrift.uri.selection", "hive.metastore.uri.selection", "RANDOM",
        new StringSetValidator("RANDOM", "SEQUENTIAL"),
        "Determines the selection mechanism used by metastore client to connect to remote " +
        "metastore.  SEQUENTIAL implies that the first valid metastore from the URIs specified " +
        "through hive.metastore.uris will be picked.  RANDOM implies that the metastore " +
        "will be picked randomly"),
    TOKEN_SIGNATURE("metastore.token.signature", "hive.metastore.token.signature", "",
        "The delegation token service name to match when selecting a token from the current user's tokens."),
    METASTORE_CACHE_CAN_USE_EVENT("metastore.cache.can.use.event", "hive.metastore.cache.can.use.event", false,
            "Can notification events from notification log table be used for updating the metastore cache."),
    TRANSACTIONAL_EVENT_LISTENERS("metastore.transactional.event.listeners",
        "hive.metastore.transactional.event.listeners", "",
        "A comma separated list of Java classes that implement the org.apache.riven.MetaStoreEventListener" +
            " interface. Both the metastore event and corresponding listener method will be invoked in the same JDO transaction." +
            " If org.apache.hive.hcatalog.listener.DbNotificationListener is configured along with other transactional event" +
            " listener implementation classes, make sure org.apache.hive.hcatalog.listener.DbNotificationListener is placed at" +
            " the end of the list."),
    TRY_DIRECT_SQL("metastore.try.direct.sql", "hive.metastore.try.direct.sql", true,
        "Whether the metastore should try to use direct SQL queries instead of the\n" +
            "DataNucleus for certain read paths. This can improve metastore performance when\n" +
            "fetching many partitions or column statistics by orders of magnitude; however, it\n" +
            "is not guaranteed to work on all RDBMS-es and all versions. In case of SQL failures,\n" +
            "the metastore will fall back to the DataNucleus, so it's safe even if SQL doesn't\n" +
            "work for all queries on your datastore. If all SQL queries fail (for example, your\n" +
            "metastore is backed by MongoDB), you might want to disable this to save the\n" +
            "try-and-fall-back cost."),
    TRY_DIRECT_SQL_DDL("metastore.try.direct.sql.ddl", "hive.metastore.try.direct.sql.ddl", true,
        "Same as hive.metastore.try.direct.sql, for read statements within a transaction that\n" +
            "modifies metastore data. Due to non-standard behavior in Postgres, if a direct SQL\n" +
            "select query has incorrect syntax or something similar inside a transaction, the\n" +
            "entire transaction will fail and fall-back to DataNucleus will not be possible. You\n" +
            "should disable the usage of direct SQL inside transactions if that happens in your case."),
    TXN_MAX_OPEN_BATCH("metastore.txn.max.open.batch", "hive.txn.max.open.batch", 1000,
        "Maximum number of transactions that can be fetched in one call to open_txns().\n" +
            "This controls how many transactions streaming agents such as Flume or Storm open\n" +
            "simultaneously. The streaming agent then writes that number of entries into a single\n" +
            "file (per Flume agent or Storm bolt). Thus increasing this value decreases the number\n" +
            "of delta files created by streaming agents. But it also increases the number of open\n" +
            "transactions that Hive has to track at any given time, which may negatively affect\n" +
            "read performance."),
    TXN_RETRYABLE_SQLEX_REGEX("metastore.txn.retryable.sqlex.regex",
        "hive.txn.retryable.sqlex.regex", "", "Comma separated list\n" +
        "of regular expression patterns for SQL state, error code, and error message of\n" +
        "retryable SQLExceptions, that's suitable for the metastore DB.\n" +
        "For example: Can't serialize.*,40001$,^Deadlock,.*ORA-08176.*\n" +
        "The string that the regex will be matched against is of the following form, where ex is a SQLException:\n" +
        "ex.getMessage() + \" (SQLState=\" + ex.getSQLState() + \", ErrorCode=\" + ex.getErrorCode() + \")\""),
    TXN_STORE_IMPL("metastore.txn.store.impl", "hive.metastore.txn.store.impl",
        "org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler",
        "Name of class that implements org.apache.riven.txn.TxnStore.  This " +
            "class is used to store and retrieve transactions and locks"),
    TXN_TIMEOUT("metastore.txn.timeout", "hive.txn.timeout", 300, TimeUnit.SECONDS,
        "time after which transactions are declared aborted if the client has not sent a heartbeat."),
    TXN_OPENTXN_TIMEOUT("metastore.txn.opentxn.timeout", "hive.txn.opentxn.timeout", 1000, TimeUnit.MILLISECONDS,
        "Time before an open transaction operation should persist, otherwise it is considered invalid and rolled back"),
    TXN_USE_MIN_HISTORY_LEVEL("metastore.txn.use.minhistorylevel", "hive.txn.use.minhistorylevel", true,
        "Set this to false, for the TxnHandler and Cleaner to not use MIN_HISTORY_LEVEL table and take advantage of openTxn optimisation.\n"
            + "If the table is dropped HMS will switch this flag to false, any other value changes need a restart to take effect."),
    TXN_USE_MIN_HISTORY_WRITE_ID("metastore.txn.use.minhistorywriteid", "hive.txn.use.minhistorywriteid", false,
      "Set this to true, to avoid global minOpenTxn check in Cleaner.\n"
            + "If the table is dropped HMS will switch this flag to false."),
    LOCK_NUMRETRIES("metastore.lock.numretries", "hive.lock.numretries", 100,
        "The number of times you want to try to get all the locks"),
    LOCK_SLEEP_BETWEEN_RETRIES("metastore.lock.sleep.between.retries", "hive.lock.sleep.between.retries", 60, TimeUnit.SECONDS,
        new TimeValidator(TimeUnit.SECONDS, 0L, false, Long.MAX_VALUE, false),
        "The maximum sleep time between various retries"),
    URI_RESOLVER("metastore.uri.resolver", "hive.metastore.uri.resolver", "",
            "If set, fully qualified class name of resolver for hive metastore uri's"),
    USERS_IN_ADMIN_ROLE("metastore.users.in.admin.role", "hive.users.in.admin.role", "", false,
        "Comma separated list of users who are in admin role for bootstrapping.\n" +
            "More users can be added in ADMIN role later."),
    // TODO: Should we have a separate config for the metastoreclient or THRIFT_TRANSPORT_MODE
    // would suffice ?
    METASTORE_CLIENT_THRIFT_TRANSPORT_MODE("metastore.client.thrift.transport.mode",
        "hive.metastore.client.thrift.transport.mode", "binary",
        "Transport mode to be used by the metastore client. It should be the same as " + THRIFT_TRANSPORT_MODE),
    METASTORE_CLIENT_THRIFT_HTTP_PATH("metastore.client.thrift.http.path",
        "hive.metastore.client.thrift.http.path",
        "metastore",
        "Path component of URL endpoint when in HTTP mode"),
    METASTORE_THRIFT_HTTP_REQUEST_HEADER_SIZE("metastore.server.thrift.http.request.header.size",
        "hive.metastore.server.thrift.http.request.header.size", 6*1024,
        "Request header size in bytes when using HTTP transport mode for metastore thrift server."
            + " Defaults to jetty's defaults"),
    METASTORE_THRIFT_HTTP_RESPONSE_HEADER_SIZE("metastore.server.thrift.http.response.header.size",
        "metastore.server.thrift.http.response.header.size", 6*1024,
        "Response header size in bytes when using HTTP transport mode for metastore thrift server."
            + " Defaults to jetty's defaults"),
    METASTORE_THRIFT_HTTP_MAX_IDLE_TIME("metastore.thrift.http.max.idle.time", "hive.metastore.thrift.http.max.idle.time", 
        1800, TimeUnit.SECONDS,
        "Maximum idle time for a connection on the server when in HTTP mode."),
    USE_SSL("metastore.use.SSL", "hive.metastore.use.SSL", false,
        "Set this to true for using SSL encryption in HMS server."),
    // We should somehow unify next two options.
    USE_THRIFT_SASL("metastore.sasl.enabled", "hive.metastore.sasl.enabled", false,
        "If true, the metastore Thrift interface will be secured with SASL. Clients must authenticate with Kerberos."),
    METASTORE_CLIENT_AUTH_MODE("metastore.client.auth.mode",
            "hive.metastore.client.auth.mode", "NOSASL",
            new StringSetValidator("NOSASL", "PLAIN", "KERBEROS", "JWT"),
            "If PLAIN, clients will authenticate using plain authentication, by providing username" +
                    " and password. Any other value is ignored right now but may be used later."
                + "If JWT- Supported only in HTTP transport mode. If set, HMS Client will pick the value of JWT from "
                + "environment variable HMS_JWT and set it in Authorization header in http request"),
    METASTORE_CLIENT_ADDITIONAL_HEADERS("metastore.client.http.additional.headers",
        "hive.metastore.client.http.additional.headers", "",
        "Comma separated list of headers which are passed to the metastore service in the http headers"),
    METASTORE_CLIENT_PLAIN_USERNAME("metastore.client.plain.username",
            "hive.metastore.client.plain.username",  "",
        "The username used by the metastore client when " +
                METASTORE_CLIENT_AUTH_MODE + " is true. The password is obtained from " +
                    CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH + " using username as the " +
                    "alias."),
    THRIFT_AUTH_CONFIG_USERNAME("metastore.authentication.config.username",
            "hive.metastore.authentication.config.username", "",
            "If " + THRIFT_METASTORE_AUTHENTICATION + " is set to CONFIG, username provided by " +
                    "client is matched against this value."),
    THRIFT_AUTH_CONFIG_PASSWORD("metastore.authentication.config.password",
             "hive.metastore.authentication.config.password", "",
            "If " + THRIFT_METASTORE_AUTHENTICATION + " is set to CONFIG, password provided by " +
                    "the client is matched against this value."),
    USE_THRIFT_FRAMED_TRANSPORT("metastore.thrift.framed.transport.enabled",
        "hive.metastore.thrift.framed.transport.enabled", false,
        "If true, the metastore Thrift interface will use TFramedTransport. When false (default) a standard TTransport is used."),
    USE_THRIFT_COMPACT_PROTOCOL("metastore.thrift.compact.protocol.enabled",
        "hive.metastore.thrift.compact.protocol.enabled", false,
        "If true, the metastore Thrift interface will use TCompactProtocol. When false (default) TBinaryProtocol will be used.\n" +
            "Setting it to true will break compatibility with older clients running TBinaryProtocol."),
    VALIDATE_COLUMNS("datanucleus.schema.validateColumns", "datanucleus.schema.validateColumns", false,
        "validates existing schema against code. turn this on if you want to verify existing schema"),
    VALIDATE_CONSTRAINTS("datanucleus.schema.validateConstraints",
        "datanucleus.schema.validateConstraints", false,
        "validates existing schema against code. turn this on if you want to verify existing schema"),
    VALIDATE_TABLES("datanucleus.schema.validateTables",
        "datanucleus.schema.validateTables", false,
        "validates existing schema against code. turn this on if you want to verify existing schema"),
    WAREHOUSE("metastore.warehouse.dir", "hive.metastore.warehouse.dir", "/user/hive/warehouse",
        "location of default database for the warehouse"),
    WAREHOUSE_EXTERNAL("metastore.warehouse.external.dir",
        "hive.metastore.warehouse.external.dir", "",
        "Default location for external tables created in the warehouse. " +
        "If not set or null, then the normal warehouse location will be used as the default location."),
    WM_DEFAULT_POOL_SIZE("metastore.wm.default.pool.size",
        "hive.metastore.wm.default.pool.size", 4,
        "The size of a default pool to create when creating an empty resource plan;\n" +
        "If not positive, no default pool will be created."),
    RAWSTORE_PARTITION_BATCH_SIZE("metastore.rawstore.batch.size",
        "metastore.rawstore.batch.size", -1,
        "Batch size for partition and other object retrieval from the underlying DB in JDO.\n" +
        "The JDO implementation such as DataNucleus may run into issues when the generated queries are\n" +
        "too large. Use this parameter to break the query into multiple batches. -1 means no batching."),
    /**
     * @deprecated Deprecated due to HIVE-26443
     */
    @Deprecated
    HIVE_METASTORE_RUNWORKER_IN("hive.metastore.runworker.in",
        "hive.metastore.runworker.in", "hs2", new StringSetValidator("metastore", "hs2"),
        "Deprecated. HMS side compaction workers doesn't support pooling. With the concept of compaction " +
            "pools (HIVE-26443), running workers on HMS side is still supported but not suggested anymore. " +
            "This config value will be removed in the future.\n" +
            "Chooses where the compactor worker threads should run, Only possible values are \"metastore\" and \"hs2\""),
    // Hive values we have copied and use as is
    // These two are used to indicate that we are running tests
    HIVE_IN_TEST("hive.in.test", "hive.in.test", false, "internal usage only, true in test mode"),
    HIVE_IN_TEZ_TEST("hive.in.tez.test", "hive.in.tez.test", false,
        "internal use only, true when in testing tez"),
    // We need to track this as some listeners pass it through our config and we need to honor
    // the system properties.
    HIVE_AUTHORIZATION_MANAGER("hive.security.authorization.manager",
        "hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory",
        "The Hive client authorization manager class name. The user defined authorization class should implement \n" +
            "interface org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider."),
    HIVE_METASTORE_AUTHENTICATOR_MANAGER("hive.security.metastore.authenticator.manager",
        "hive.security.metastore.authenticator.manager",
        "org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator",
        "authenticator manager class name to be used in the metastore for authentication. \n" +
            "The user defined authenticator should implement interface org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider."),
    HIVE_METASTORE_AUTHORIZATION_AUTH_READS("hive.security.metastore.authorization.auth.reads",
        "hive.security.metastore.authorization.auth.reads", true,
        "If this is true, metastore authorizer authorizes read actions on database, table"),
    // The metastore shouldn't care what txn manager Hive is running, but in various tests it
    // needs to set these values.  We should do the work to detangle this.
    HIVE_TXN_MANAGER("hive.txn.manager", "hive.txn.manager",
        "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager",
        "Set to org.apache.hadoop.hive.ql.lockmgr.DbTxnManager as part of turning on Hive\n" +
            "transactions, which also requires appropriate settings for hive.compactor.initiator.on,hive.compactor.cleaner.on,\n" +
            "hive.compactor.worker.threads, hive.support.concurrency (true),\n" +
            "and hive.exec.dynamic.partition.mode (nonstrict).\n" +
            "The default DummyTxnManager replicates pre-Hive-0.13 behavior and provides\n" +
            "no transactions."),
    // Metastore always support concurrency, but certain ACID tests depend on this being set.  We
    // need to do the work to detangle this
    HIVE_SUPPORT_CONCURRENCY("hive.support.concurrency", "hive.support.concurrency", false,
        "Whether Hive supports concurrency control or not. \n" +
            "A ZooKeeper instance must be up and running when using zookeeper Hive lock manager "),
    HIVE_TXN_STATS_ENABLED("hive.txn.stats.enabled", "hive.txn.stats.enabled", true,
        "Whether Hive supports transactional stats (accurate stats for transactional tables)"),

    // External RDBMS support
    USE_CUSTOM_RDBMS("metastore.use.custom.database.product",
        "hive.metastore.use.custom.database.product", false,
        "Use an external RDBMS which is not in the list of natively supported databases (Derby,\n"
            + "Mysql, Oracle, Postgres, MSSQL), as defined by hive.metastore.db.type. If this configuration\n"
            + "is true, the metastore.custom.database.product.classname must be set to a valid class name"),
    CUSTOM_RDBMS_CLASSNAME("metastore.custom.database.product.classname",
        "hive.metastore.custom.database.product.classname", "none",
          "Hook for external RDBMS. This class will be instantiated only when " +
          "metastore.use.custom.database.product is set to true."),
    HIVE_BLOBSTORE_SUPPORTED_SCHEMES("hive.blobstore.supported.schemes", "hive.blobstore.supported.schemes", "s3,s3a,s3n",
            "Comma-separated list of supported blobstore schemes."),

    // Property-maps
    PROPERTIES_CACHE_CAPACITY("hive.metastore.properties.cache.capacity",
        "hive.metastore.properties.cache.maxsize", 64,
        "Maximum number of property-maps (collection of properties for one entity) held in cache per store."
    ),
    PROPERTIES_CACHE_LOADFACTOR("hive.metastore.properties.cache.loadfactor",
        "hive.metastore.properties.cache.maxsize", 0.75d,
        "Property-maps cache map initial fill factor (> 0.0, < 1.0)."
    ),
    PROPERTIES_SERVLET_PATH("hive.metastore.properties.servlet.path",
        "hive.metastore.properties.servlet.path", "hmscli",
        "Property-maps servlet path component of URL endpoint."
    ),
    PROPERTIES_SERVLET_PORT("hive.metastore.properties.servlet.port",
        "hive.metastore.properties.servlet.port", -1,
        "Property-maps servlet server port. Negative value disables the servlet," +
            " 0 will let the system determine the servlet server port," +
            " positive value will be used as-is."
    ),
    PROPERTIES_SERVLET_AUTH("hive.metastore.properties.servlet.auth",
        "hive.metastore.properties.servlet.auth", "jwt",
        "Property-maps servlet authentication method (simple or jwt)."
    ),

    // Deprecated Hive values that we are keeping for backwards compatibility.
    @Deprecated
    HIVE_CODAHALE_METRICS_REPORTER_CLASSES("hive.service.metrics.codahale.reporter.classes",
        "hive.service.metrics.codahale.reporter.classes",
        "org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter, " +
        "org.apache.hadoop.hive.common.metrics.metrics2.JmxMetricsReporter",
        "Use METRICS_REPORTERS instead.  Comma separated list of reporter implementation classes " +
            "for metric class org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics. Overrides "
            + "HIVE_METRICS_REPORTER conf if present.  This will be overridden by " +
            "METRICS_REPORTERS if it is present"),
    @Deprecated
    HIVE_METRICS_REPORTER("hive.service.metrics.reporter", "hive.service.metrics.reporter", "",
        "Reporter implementations for metric class "
            + "org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;" +
            "Deprecated, use METRICS_REPORTERS instead. This configuraiton will be"
            + " overridden by HIVE_CODAHALE_METRICS_REPORTER_CLASSES and METRICS_REPORTERS if " +
            "present. Comma separated list of JMX, CONSOLE, JSON_FILE, HADOOP2"),
    // Planned to be removed in HIVE-21024
    @Deprecated
    DBACCESS_SSL_PROPS("metastore.dbaccess.ssl.properties", "hive.metastore.dbaccess.ssl.properties", "",
        "Deprecated. Use the metastore.dbaccess.ssl.* properties instead. Comma-separated SSL properties for " +
            "metastore to access database when JDO connection URL enables SSL access. \n"
            + "e.g. javax.net.ssl.trustStore=/tmp/truststore,javax.net.ssl.trustStorePassword=pwd.\n " +
            "If both this and the metastore.dbaccess.ssl.* properties are set, then the latter properties \n" +
            "will overwrite what was set in the deprecated property."),
    METASTORE_NUM_STRIPED_TABLE_LOCKS("metastore.num.striped.table.locks", "hive.metastore.num.striped.table.locks", 32,
        "Number of striped locks available to provide exclusive operation support for critical table operations like add_partitions."),
    COLSTATS_RETAIN_ON_COLUMN_REMOVAL("metastore.colstats.retain.on.column.removal",
        "hive.metastore.colstats.retain.on.column.removal", true,
        "Whether to retain column statistics during column removals in partitioned tables - disabling this "
            + "purges all column statistics data "
            + "for all partition to retain working consistency"),

    // These are all values that we put here just for testing
    STR_TEST_ENTRY("test.str", "hive.test.str", "defaultval", "comment"),
    STR_SET_ENTRY("test.str.set", "hive.test.str.set", "a", new StringSetValidator("a", "b", "c"), ""),
    STR_LIST_ENTRY("test.str.list", "hive.test.str.list", "a,b,c",
        "no comment"),
    LONG_TEST_ENTRY("test.long", "hive.test.long", 42, "comment"),
    DOUBLE_TEST_ENTRY("test.double", "hive.test.double", Math.PI, "comment"),
    TIME_TEST_ENTRY("test.time", "hive.test.time", 1, TimeUnit.SECONDS, "comment"),
    DEPRECATED_TEST_ENTRY("test.deprecated", "hive.test.deprecated", 0, new RangeValidator(0, 3), "comment",
        "this.is.the.metastore.deprecated.name", "this.is.the.hive.deprecated.name"),
    TIME_VALIDATOR_ENTRY_INCLUSIVE("test.time.validator.inclusive", "hive.test.time.validator.inclusive", 1,
        TimeUnit.SECONDS,
        new TimeValidator(TimeUnit.MILLISECONDS, 500L, true, 1500L, true), "comment"),
    TIME_VALIDATOR_ENTRY_EXCLUSIVE("test.time.validator.exclusive", "hive.test.time.validator.exclusive", 1,
        TimeUnit.SECONDS,
        new TimeValidator(TimeUnit.MILLISECONDS, 500L, false, 1500L, false), "comment"),
    BOOLEAN_TEST_ENTRY("test.bool", "hive.test.bool", true, "comment"),
    CLASS_TEST_ENTRY("test.class", "hive.test.class", "", "comment");

    private final String varname;
    private final String hiveName;
    private final Object defaultVal;
    private final Validator validator;
    private final boolean caseSensitive;
    private final String description;
    private String deprecatedName = null;
    private String hiveDeprecatedName = null;

    ConfVars(String varname, String hiveName, String defaultVal, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      validator = null;
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, String defaultVal, Validator validator,
             String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      this.validator = validator;
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, String defaultVal, boolean caseSensitive,
             String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      validator = null;
      this.caseSensitive = caseSensitive;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, long defaultVal, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      validator = null;
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, long defaultVal, Validator validator,
             String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      this.validator = validator;
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, long defaultVal, Validator validator,
        String description, String deprecatedName, String hiveDeprecatedName) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      this.validator = validator;
      caseSensitive = false;
      this.description = description;
      this.deprecatedName = deprecatedName;
      this.hiveDeprecatedName = hiveDeprecatedName;
    }

    ConfVars(String varname, String hiveName, boolean defaultVal, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      validator = null;
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, double defaultVal, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      validator = null;
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, long defaultVal, TimeUnit unit, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = new TimeValue(defaultVal, unit);
      validator = new TimeValidator(unit);
      caseSensitive = false;
      this.description = description;
    }

    ConfVars(String varname, String hiveName, long defaultVal, TimeUnit unit,
             Validator validator, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = new TimeValue(defaultVal, unit);
      this.validator = validator;
      caseSensitive = false;
      this.description = description;
    }

    public void validate(String value) throws IllegalArgumentException {
      if (validator != null) {
        validator.validate(value);
      }
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    /**
     * If you are calling this, you're probably doing it wrong.  You shouldn't need to use the
     * underlying variable name.  Use one of the getVar methods instead.  Only use this if you
     * are 100% sure you know you're doing.  The reason for this is that MetastoreConf goes to a
     * lot of trouble to make sure it checks both Hive and Metastore values for config keys.  If
     * you call {@link Configuration#get(String)} you are undermining that.
     * @return variable name
     */
    public String getVarname() {
      return varname;
    }

    /**
     * Use this method if you need to set a system property and are going to instantiate the
     * configuration file via HiveConf.  This is because HiveConf only looks for values it knows,
     * so it will miss all of the metastore.* ones.  Do not use this to explicitly set or get the
     * underlying config value unless you are 100% sure you know what you're doing.
     * The reason for this is that MetastoreConf goes to a
     * lot of trouble to make sure it checks both Hive and Metastore values for config keys.  If
     * you call {@link Configuration#get(String)} you are undermining that.
     * @return hive.* configuration key
     */
    public String getHiveName() {
      return hiveName;
    }

    public Object getDefaultVal() {
      return defaultVal;
    }

    public String getDescription() {
      return description;
    }

    /**
     * This is useful if you need the variable name for a LOG message or
     * {@link System#setProperty(String, String)}, beware however that you should only use this
     * with setProperty if you're going to create a configuration via
     * {@link MetastoreConf#newMetastoreConf()}.  If you are going to create it with HiveConf,
     * then use {@link #getHiveName()}.
     * @return metastore.* configuration key
     */
    @Override
    public String toString() {
      return varname;
    }
  }

  public static final ConfVars[] dataNucleusAndJdoConfs = {
      ConfVars.AUTO_CREATE_ALL,
      ConfVars.CONNECTION_DRIVER,
      ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS,
      ConfVars.CONNECTION_POOLING_TYPE,
      ConfVars.CONNECT_URL_KEY,
      ConfVars.CONNECTION_USER_NAME,
      ConfVars.DATANUCLEUS_AUTOSTART,
      ConfVars.DATANUCLEUS_CACHE_LEVEL2,
      ConfVars.DATANUCLEUS_CACHE_LEVEL2_TYPE,
      ConfVars.DATANUCLEUS_INIT_COL_INFO,
      ConfVars.DATANUCLEUS_PLUGIN_REGISTRY_BUNDLE_CHECK,
      ConfVars.DATANUCLEUS_TRANSACTION_ISOLATION,
      ConfVars.DATANUCLEUS_USE_LEGACY_VALUE_STRATEGY,
      ConfVars.DATANUCLEUS_QUERY_SQL_ALLOWALL,
      ConfVars.DETACH_ALL_ON_COMMIT,
      ConfVars.IDENTIFIER_FACTORY,
      ConfVars.MANAGER_FACTORY_CLASS,
      ConfVars.MULTITHREADED,
      ConfVars.NON_TRANSACTIONAL_READ,
      ConfVars.PWD,
      ConfVars.STORE_MANAGER_TYPE,
      ConfVars.VALIDATE_COLUMNS,
      ConfVars.VALIDATE_CONSTRAINTS,
      ConfVars.VALIDATE_TABLES
  };

  // Make sure no one calls this
  private MetastoreConf() {
    throw new RuntimeException("You should never be creating one of these!");
  }

  public static void setHiveSiteLocation(URL location) {
    hiveSiteURL = location;
  }

  public static Configuration newMetastoreConf() {
    return newMetastoreConf(new Configuration());
  }

  public static Configuration newMetastoreConf(Configuration conf) {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = MetastoreConf.class.getClassLoader();
    }
    // We don't add this to the resources because we don't want to read config values from it.
    // But we do find it because we want to remember where it is for later in case anyone calls
    // getHiveDefaultLocation().
    hiveDefaultURL = classLoader.getResource("hive-default.xml");

    // Add in hive-site.xml.  We add this first so that it gets overridden by the new metastore
    // specific files if they exist.
    if(hiveSiteURL == null) {
      /*
       * this 'if' is pretty lame - QTestUtil.QTestUtil() uses hiveSiteURL to load a specific
       * hive-site.xml from data/conf/<subdir> so this makes it follow the same logic - otherwise
       * HiveConf and MetastoreConf may load different hive-site.xml  ( For example,
       * HiveConf uses data/conf/tez/hive-site.xml and MetastoreConf data/conf/hive-site.xml)
       */
      hiveSiteURL = findConfigFile(classLoader, "hive-site.xml");
    }
    if (hiveSiteURL != null) {
      conf.addResource(hiveSiteURL);
    }

    // Now add hivemetastore-site.xml.  Again we add this before our own config files so that the
    // newer overrides the older.
    hiveMetastoreSiteURL = findConfigFile(classLoader, "hivemetastore-site.xml");
    if (hiveMetastoreSiteURL != null) {
      conf.addResource(hiveMetastoreSiteURL);
    }

    // Add in our conf file
    metastoreSiteURL = findConfigFile(classLoader, "metastore-site.xml");
    if (metastoreSiteURL !=  null) {
      conf.addResource(metastoreSiteURL);
    }

    // If a system property that matches one of our conf value names is set then use the value
    // it's set to to set our own conf value.
    for (ConfVars var : ConfVars.values()) {
      if (System.getProperty(var.varname) != null) {
        LOG.debug("Setting conf value " + var.varname + " using value " +
            System.getProperty(var.varname));
        conf.set(var.varname, System.getProperty(var.varname));
      }
    }

    // Pick up any system properties that start with "hive." and set them in our config.  This
    // way we can properly pull any Hive values from the environment without needing to know all
    // of the Hive config values.
    System.getProperties().stringPropertyNames().stream()
        .filter(s -> s.startsWith("hive."))
        .forEach(s -> {
          String v = System.getProperty(s);
          LOG.debug("Picking up system property " + s + " with value " + v);
          conf.set(s, v);
        });

    // If we are going to validate the schema, make sure we don't create it
    if (getBoolVar(conf, ConfVars.SCHEMA_VERIFICATION)) {
      setBoolVar(conf, ConfVars.AUTO_CREATE_ALL, false);
    }

    if (!beenDumped.getAndSet(true) && getBoolVar(conf, ConfVars.DUMP_CONFIG_ON_CREATION) &&
        LOG.isDebugEnabled()) {
      LOG.debug(dumpConfig(conf));
    }

    /*
    Add deprecated config names to configuration.
    The parameters for Configuration.addDeprecation are (oldKey, newKey) and it is assumed that the config is set via
    newKey and the value is retrieved via oldKey.
    However in this case we assume the value is set with the deprecated key (oldKey) in some config file and we
    retrieve it in the code via the new key. So the parameter order we use here is: (newKey, deprecatedKey).
    We do this with the HiveConf configs as well.
     */
    for (ConfVars var : ConfVars.values()) {
      if (var.deprecatedName != null) {
        Configuration.addDeprecation(var.getVarname(), var.deprecatedName);
      }
      if (var.hiveDeprecatedName != null) {
        Configuration.addDeprecation(var.getHiveName(), var.hiveDeprecatedName);
      }
    }

    return conf;
  }

  private static URL findConfigFile(ClassLoader classLoader, String name) {
    // First, look in the classpath
    URL result = classLoader.getResource(name);
    if (result == null) {
      // Nope, so look to see if our conf dir has been explicitly set
      result = seeIfConfAtThisLocation("METASTORE_CONF_DIR", name, false);
    }
    if (result == null) {
      // Nope, so look to see if our home dir has been explicitly set
      result = seeIfConfAtThisLocation("METASTORE_HOME", name, true);
    }
    if (result == null) {
      // Nope, so look to see if Hive's conf dir has been explicitly set
      result = seeIfConfAtThisLocation("HIVE_CONF_DIR", name, false);
    }
    if (result == null) {
      // Nope, so look to see if Hive's home dir has been explicitly set
      result = seeIfConfAtThisLocation("HIVE_HOME", name, true);
    }
    if (result == null) {
      // Nope, so look to see if we can find a conf file by finding our jar, going up one
      // directory, and looking for a conf directory.
      URI jarUri = null;
      try {
        jarUri = MetastoreConf.class.getProtectionDomain().getCodeSource().getLocation().toURI();
      } catch (Throwable e) {
        LOG.warn("Cannot get jar URI", e);
      }
      if (jarUri != null) {
        result = seeIfConfAtThisLocation(new File(jarUri).getParent(), name, true);
      }
    }

    if (result == null) {
      LOG.info("Unable to find config file: " + name);
    } else {
      LOG.info("Found configuration file: " + result);
    }

    return result;
  }

  private static URL seeIfConfAtThisLocation(String envVar, String name, boolean inConfDir) {
    String path = System.getenv(envVar);
    if (path == null) {
      // Workaround for testing since tests can't set the env vars.
      path = System.getProperty(TEST_ENV_WORKAROUND + envVar);
    }
    if (path != null) {
      String suffix = inConfDir ? "conf" + File.separatorChar + name : name;
      return checkConfigFile(new File(path, suffix));
    }
    return null;
  }

  private static URL checkConfigFile(File f) {
    try {
      return (f.exists() && f.isFile()) ? f.toURI().toURL() : null;
    } catch (Throwable e) {
      LOG.warn("Error looking for config " + f, e);
      return null;
    }
  }

  // In all of the getters, we try the metastore value name first.  If it is not set we try the
  // Hive value name.

  /**
   * Get the variable as a string
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @return value, or default value if value not in config file
   */
  public static String getVar(Configuration conf, ConfVars var) {
    assert var.defaultVal.getClass() == String.class;
    String val = conf.get(var.varname);
    return val == null ? conf.get(var.hiveName, (String)var.defaultVal) : val;
  }

  /**
   * Get the variable as a string
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @param defaultVal default to return if the variable is unset
   * @return value, or default value passed in if the value is not in the config file
   */
  public static String getVar(Configuration conf, ConfVars var, String defaultVal) {
    assert var.defaultVal.getClass() == String.class;
    String val = conf.get(var.varname);
    return val == null ? conf.get(var.hiveName, defaultVal) : val;
  }

  /**
   * Treat a configuration value as a comma separated list.
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @return collection of strings.  If the value is unset it will return an empty collection.
   */
  public static Collection<String> getStringCollection(Configuration conf, ConfVars var) {
    assert var.defaultVal.getClass() == String.class;
    String val = conf.get(var.varname);
    if (val == null) {
      val = conf.get(var.hiveName, (String)var.defaultVal);
    }
    if (val == null) {
      return Collections.emptySet();
    }
    return StringUtils.asSet(val.split(","));
  }

  /**
   * Set the variable as a string
   * @param conf configuration file to set it in
   * @param var variable to set
   * @param val value to set it to
   */
  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert var.defaultVal.getClass() == String.class;
    conf.set(var.varname, val);
  }

  /**
   * Get the variable as a int.  Note that all integer valued variables are stored as longs, thus
   * this downcasts from a long to an in.
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @return value, or default value if value not in config file
   */
  public static int getIntVar(Configuration conf, ConfVars var) {
    long val = getLongVar(conf, var);
    assert val <= Integer.MAX_VALUE;
    return (int)val;
  }

  /**
   * Get the variable as a long
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @return value, or default value if value not in config file
   */
  public static long getLongVar(Configuration conf, ConfVars var) {
    assert var.defaultVal.getClass() == Long.class;
    String val = conf.get(var.varname);
    return val == null ? conf.getLong(var.hiveName, (Long)var.defaultVal) : Long.parseLong(val);
  }

  /**
   * Set the variable as a long
   * @param conf configuration file to set it in
   * @param var variable to set
   * @param val value to set it to
   */
  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    assert var.defaultVal.getClass() == Long.class;
    conf.setLong(var.varname, val);
  }

  /**
   * Get the variable as a boolean
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @return value, or default value if value not in config file
   */
  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert var.defaultVal.getClass() == Boolean.class;
    String val = conf.get(var.varname);
    return val == null ? conf.getBoolean(var.hiveName, (Boolean)var.defaultVal) : Boolean.valueOf(val);
  }

  /**
   * Get values from comma-separated config, to an array after extracting individual values.
   * @param conf Configuration to retrieve it from
   * @param var variable to retrieve
   * @return Array of String, containing each value from the comma-separated config,
   *  or default value if value not in config file
   */
  public static String[] getTrimmedStringsVar(Configuration conf, ConfVars var) {
    assert var.defaultVal.getClass() == String.class;
    String[] result = conf.getTrimmedStrings(var.varname, (String[]) null);
    if (result != null) {
      return result;
    }
    if (var.hiveName != null) {
      result = conf.getTrimmedStrings(var.hiveName, (String[]) null);
      if (result != null) {
        return result;
      }
    }
    return org.apache.hadoop.util.StringUtils.getTrimmedStrings((String) var.getDefaultVal());
  }

  /**
   * Set the variable as a boolean
   * @param conf configuration file to set it in
   * @param var variable to set
   * @param val value to set it to
   */
  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    assert var.defaultVal.getClass() == Boolean.class;
    conf.setBoolean(var.varname, val);
  }

  /**
   * Get the variable as a double
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @return value, or default value if value not in config file
   */
  public static double getDoubleVar(Configuration conf, ConfVars var) {
    assert var.defaultVal.getClass() == Double.class;
    String val = conf.get(var.varname);
    return val == null ? conf.getDouble(var.hiveName, (Double)var.defaultVal) : Double.valueOf(val);
  }

  /**
   * Set the variable as a double
   * @param conf configuration file to set it in
   * @param var variable to set
   * @param val value to set it to
   */
  public static void setDoubleVar(Configuration conf, ConfVars var, double val) {
    assert var.defaultVal.getClass() == Double.class;
    conf.setDouble(var.varname, val);
  }

  public static long getSizeVar(Configuration conf, ConfVars var) {
    return SizeValidator.toSizeBytes(getVar(conf, var));
  }

  /**
   * Get a class instance based on a configuration value
   * @param conf configuration file to retrieve it from
   * @param var variable to retrieve
   * @param defaultValue default class to return if the value isn't set
   * @param xface interface that class must implement
   * @param <I> interface that class implements
   * @return instance of the class
   */
  public static <I> Class<? extends I> getClass(Configuration conf, ConfVars var,
                                                Class<? extends I> defaultValue,
                                                Class<I> xface) {
    assert var.defaultVal.getClass() == String.class;
    String val = conf.get(var.varname);
    return val == null ? conf.getClass(var.hiveName, defaultValue, xface) :
        conf.getClass(var.varname, defaultValue, xface);
  }

  /**
   * Set the class name in the configuration file
   * @param conf configuration file to set it in
   * @param var variable to set
   * @param theClass the class to set it to
   * @param xface interface that the class implements.  I don't know why this is required, but
   *              the underlying {@link Configuration#setClass(String, Class, Class)} requires it.
   * @param <I> interface the class implements.
   */
  public static <I> void setClass(Configuration conf, ConfVars var, Class<? extends I> theClass,
                                  Class<I> xface) {
    assert var.defaultVal.getClass() == String.class;
    conf.setClass(var.varname, theClass, xface);
  }



  /**
   * Get the variable as a long indicating a period of time
   * @param conf configuration to retrieve it from
   * @param var variable to retrieve
   * @param outUnit Timeout to return value in
   * @return value, or default value if value not in config file
   */
  public static long getTimeVar(Configuration conf, ConfVars var, TimeUnit outUnit) {
    assert var.defaultVal.getClass() == TimeValue.class;
    String val = conf.get(var.varname);

    if (val == null) {
      // Look for it under the old Hive name
      val = conf.get(var.hiveName);
    }

    if (val != null) {
      return convertTimeStr(val, ((TimeValue)var.defaultVal).unit, outUnit);
    } else {
      return outUnit.convert(((TimeValue)var.defaultVal).val, ((TimeValue)var.defaultVal).unit);
    }
  }

  /**
   * Set the variable as a string
   * @param conf configuration file to set it in
   * @param var variable to set
   * @param duration value to set it to
   * @param unit time unit that duration is expressed in
   */
  public static void setTimeVar(Configuration conf, ConfVars var, long duration, TimeUnit unit) {
    assert var.defaultVal.getClass() == TimeValue.class;
    conf.setTimeDuration(var.varname, duration, unit);
  }

  public static long convertTimeStr(String val, TimeUnit defaultUnit, TimeUnit outUnit) {
    if (val.charAt(val.length() - 1) >= 'A') {
      // It ends in a character, this means they appended a time indicator (e.g. 600s)
      Matcher m = TIME_UNIT_SUFFIX.matcher(val);
      if (m.matches()) {
        long duration = Long.parseLong(m.group(1));
        String unit = m.group(2).toLowerCase();

        // If/else chain arranged in likely order of frequency for performance
        if (unit.equals("s") || unit.startsWith("sec")) {
          return outUnit.convert(duration, TimeUnit.SECONDS);
        } else if (unit.equals("ms") || unit.startsWith("msec")) {
          return outUnit.convert(duration, TimeUnit.MILLISECONDS);
        } else if (unit.equals("m") || unit.startsWith("min")) {
          return outUnit.convert(duration, TimeUnit.MINUTES);
        } else if (unit.equals("us") || unit.startsWith("usec")) {
          return outUnit.convert(duration, TimeUnit.MICROSECONDS);
        } else if (unit.equals("ns") || unit.startsWith("nsec")) {
          return outUnit.convert(duration, TimeUnit.NANOSECONDS);
        } else if (unit.equals("h") || unit.startsWith("hour")) {
          return outUnit.convert(duration, TimeUnit.HOURS);
        } else if (unit.equals("d") || unit.startsWith("day")) {
          return outUnit.convert(duration, TimeUnit.DAYS);
        } else {
          throw new IllegalArgumentException("Invalid time unit " + unit);
        }
      } else {
        throw new IllegalArgumentException("Invalid time unit " + val);
      }
    }

    // If they gave a value but not a time unit assume the default time unit.
    return outUnit.convert(Long.parseLong(val), defaultUnit);
  }

  static String timeAbbreviationFor(TimeUnit timeunit) {
    switch (timeunit) {
    case DAYS: return "d";
    case HOURS: return "h";
    case MINUTES: return "m";
    case SECONDS: return "s";
    case MILLISECONDS: return "ms";
    case MICROSECONDS: return "us";
    case NANOSECONDS: return "ns";
    }
    throw new IllegalArgumentException("Invalid timeunit " + timeunit);
  }

  /**
   * Get a password from the configuration file.  This uses Hadoop's
   * {@link Configuration#getPassword(String)} to handle getting secure passwords.
   * @param conf configuration file to read from
   * @param var configuration value to read
   * @return the password as a string, or the default value.
   * @throws IOException if thrown by Configuration.getPassword
   */
  public static String getPassword(Configuration conf, ConfVars var) throws IOException {
    assert var.defaultVal.getClass() == String.class;
    char[] pw = conf.getPassword(var.varname);
    if (pw == null) {
      // Might be under the hive name
      pw = conf.getPassword(var.hiveName);
    }
    return pw == null ? var.defaultVal.toString() : new String(pw);
  }

  /**
   * Get the configuration value based on a string rather than a ConfVar.  This will do the
   * mapping between metastore keys and Hive keys.  That is, if there's a ConfVar with a
   * metastore key of "metastore.a" and a hive key of "hive.a", the value for that variable will
   * be returned if either of those keys is present in the config.  If neither are present than
   * the default value will be returned.
   * @param conf configuration to read.
   * @param key metastore or hive key to read.
   * @return the value set
   */
  public static String get(Configuration conf, String key) {
    ConfVars var = keyToVars.get(key);
    if (var == null) {
      // Ok, this isn't one we track.  Just return whatever matches the string
      return conf.get(key);
    }
    // Check if the metastore key is set first
    String val = conf.get(var.varname);
    return val == null ? conf.get(var.hiveName, var.defaultVal.toString()) : val;
  }

  public static boolean isPrintable(String key) {
    return !unprintables.contains(key);
  }

  /**
   * Return the configuration value as a String.  For time based values it will be returned in
   * the default time unit appended with an appropriate abbreviation (eg s for seconds, ...)
   * @param conf configuration to read
   * @param var variable to read
   * @return value as a String
   */
  public static String getAsString(Configuration conf, ConfVars var) {
    if (var.defaultVal.getClass() == String.class) {
      return getVar(conf, var);
    } else if (var.defaultVal.getClass() == Boolean.class) {
      return Boolean.toString(getBoolVar(conf, var));
    } else if (var.defaultVal.getClass() == Long.class) {
      return Long.toString(getLongVar(conf, var));
    } else if (var.defaultVal.getClass() == Double.class) {
      return Double.toString(getDoubleVar(conf, var));
    } else if (var.defaultVal.getClass() == TimeValue.class) {
      TimeUnit timeUnit = ((TimeValue)var.defaultVal).unit;
      return getTimeVar(conf, var, timeUnit) + timeAbbreviationFor(timeUnit);
    } else {
      throw new RuntimeException("Unknown type for getObject " + var.defaultVal.getClass().getName());
    }
  }

  public static URL getHiveDefaultLocation() {
    return hiveDefaultURL;
  }

  public static URL getHiveSiteLocation() {
    return hiveSiteURL;
  }

  public static URL getHiveMetastoreSiteURL() {
    return hiveMetastoreSiteURL;
  }

  public static URL getMetastoreSiteURL() {
    return metastoreSiteURL;
  }

  public List<URL> getResourceFileLocations() {
    return Arrays.asList(hiveSiteURL, hiveMetastoreSiteURL, metastoreSiteURL);
  }

  /**
   * Check if metastore is being used in embedded mode.
   * This utility function exists so that the logic for determining the mode is same
   * in HiveConf and HiveMetaStoreClient
   * @param msUri - metastore server uri
   * @return true if the metastore is embedded
   */
  public static boolean isEmbeddedMetaStore(String msUri) {
    return (msUri == null) || msUri.trim().isEmpty();
  }

  public static ZooKeeperHiveHelper getZKConfig(Configuration conf) {
    String keyStorePassword = "";
    String trustStorePassword = "";
    if (MetastoreConf.getBoolVar(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_ENABLE)) {
      try {
        keyStorePassword = MetastoreConf.getPassword(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_KEYSTORE_PASSWORD);
        trustStorePassword = MetastoreConf.getPassword(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read zookeeper configuration passwords", e);
      }
    }
    return ZooKeeperHiveHelper.builder()
        .quorum(MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS))
        .clientPort(MetastoreConf.getVar(conf, ConfVars.THRIFT_ZOOKEEPER_CLIENT_PORT))
        .serverRegistryNameSpace(MetastoreConf.getVar(conf, ConfVars.THRIFT_ZOOKEEPER_NAMESPACE))
        .connectionTimeout((int) getTimeVar(conf, ConfVars.THRIFT_ZOOKEEPER_CONNECTION_TIMEOUT,
            TimeUnit.MILLISECONDS))
        .sessionTimeout((int) MetastoreConf.getTimeVar(conf, ConfVars.THRIFT_ZOOKEEPER_SESSION_TIMEOUT,
            TimeUnit.MILLISECONDS))
        .baseSleepTime((int) MetastoreConf.getTimeVar(conf, ConfVars.THRIFT_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
            TimeUnit.MILLISECONDS))
        .maxRetries(MetastoreConf.getIntVar(conf, ConfVars.THRIFT_ZOOKEEPER_CONNECTION_MAX_RETRIES))
        .sslEnabled(MetastoreConf.getBoolVar(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_ENABLE))
        .keyStoreLocation(MetastoreConf.getVar(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_KEYSTORE_LOCATION))
        .keyStorePassword(keyStorePassword)
        .keyStoreType(MetastoreConf.getVar(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_KEYSTORE_TYPE))
        .trustStoreLocation(MetastoreConf.getVar(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION))
        .trustStorePassword(trustStorePassword)
        .trustStoreType(MetastoreConf.getVar(conf, ConfVars.THRIFT_ZOOKEEPER_SSL_TRUSTSTORE_TYPE)).build();
  }

  /**
   * Dump the configuration file to the log.  It will be dumped at an INFO level.  This can
   * potentially produce a lot of logs, so you might want to be careful when and where you do it.
   * It takes care not to dump hidden keys.
   * @param conf Configuration file to dump
   * @return String containing dumped config file.
   */
  static String dumpConfig(Configuration conf) {
    StringBuilder buf = new StringBuilder("MetastoreConf object:\n");
    if (hiveSiteURL != null) {
      buf.append("Used hive-site file: ")
          .append(hiveSiteURL)
          .append('\n');
    }
    if (hiveMetastoreSiteURL != null) {
      buf.append("Used hivemetastore-site file: ")
          .append(hiveMetastoreSiteURL)
          .append('\n');
    }
    if (metastoreSiteURL != null) {
      buf.append("Used metastore-site file: ")
          .append(metastoreSiteURL)
          .append('\n');
    }
    for (ConfVars var : ConfVars.values()) {
      if (!unprintables.contains(var.varname)) {
        buf.append("Key: <")
            .append(var.varname)
            .append("> old hive key: <")
            .append(var.hiveName)
            .append(">  value: <")
            .append(getAsString(conf, var))
            .append(">\n");
      }
    }
    buf.append("Finished MetastoreConf object.\n");
    return buf.toString();
  }

  public static char[] getValueFromKeystore(String keystorePath, String key) throws IOException {
    char[] valueCharArray = null;
    if (keystorePath != null && key != null) {
      Configuration conf = new Configuration();
      conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, keystorePath);
      valueCharArray = conf.getPassword(key);
    }
    return valueCharArray;
  }
}
