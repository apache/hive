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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DefaultStorageSchemaReader;
import org.apache.hadoop.hive.metastore.HiveAlterHandler;
import org.apache.hadoop.hive.metastore.MaterializationsCacheCleanerTask;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.txn.AcidCompactionHistoryService;
import org.apache.hadoop.hive.metastore.txn.AcidHouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.AcidOpenTxnsCounterService;
import org.apache.hadoop.hive.metastore.txn.AcidWriteSetService;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  private static final Map<String, ConfVars> metaConfs = new HashMap<>();
  private static final String NO_SUCH_KEY = "no.such.key"; // Used in config definitions when
                                                           // there is no matching Hive or
                                                           // metastore key for a value
  private static URL hiveDefaultURL = null;
  private static URL hiveSiteURL = null;
  private static URL hiveMetastoreSiteURL = null;
  private static URL metastoreSiteURL = null;
  private static AtomicBoolean beenDumped = new AtomicBoolean();

  private static Map<String, ConfVars> keyToVars;

  @VisibleForTesting
  static final String TEST_ENV_WORKAROUND = "metastore.testing.env.workaround.dont.ever.set.this.";

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
   */
  public static final MetastoreConf.ConfVars[] metaVars = {
      ConfVars.WAREHOUSE,
      ConfVars.REPLDIR,
      ConfVars.THRIFT_URIS,
      ConfVars.SERVER_PORT,
      ConfVars.THRIFT_CONNECTION_RETRIES,
      ConfVars.THRIFT_FAILURE_RETRIES,
      ConfVars.CLIENT_CONNECT_RETRY_DELAY,
      ConfVars.CLIENT_SOCKET_TIMEOUT,
      ConfVars.CLIENT_SOCKET_LIFETIME,
      ConfVars.PWD,
      ConfVars.CONNECTURLHOOK,
      ConfVars.CONNECTURLKEY,
      ConfVars.SERVER_MIN_THREADS,
      ConfVars.SERVER_MAX_THREADS,
      ConfVars.TCP_KEEP_ALIVE,
      ConfVars.KERBEROS_KEYTAB_FILE,
      ConfVars.KERBEROS_PRINCIPAL,
      ConfVars.USE_THRIFT_SASL,
      ConfVars.TOKEN_SIGNATURE,
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
      ConfVars.HMSHANDLERATTEMPTS,
      ConfVars.HMSHANDLERINTERVAL,
      ConfVars.HMSHANDLERFORCERELOADCONF,
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
      ConfVars.FILE_METADATA_THREADS
  };

  /**
   * User configurable Metastore vars
   */
  private static final MetastoreConf.ConfVars[] metaConfVars = {
      ConfVars.TRY_DIRECT_SQL,
      ConfVars.TRY_DIRECT_SQL_DDL,
      ConfVars.CLIENT_SOCKET_TIMEOUT,
      ConfVars.PARTITION_NAME_WHITELIST_PATTERN,
      ConfVars.CAPABILITY_CHECK,
      ConfVars.DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES
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
      ConfVars.SSL_TRUSTSTORE_PASSWORD.hiveName
  );

  public static ConfVars getMetaConf(String name) {
    return metaConfs.get(name);
  }

  public enum ConfVars {
    // alpha order, PLEASE!
    ADDED_JARS("metastore.added.jars.path", "hive.added.jars.path", "",
        "This an internal parameter."),
    AGGREGATE_STATS_CACHE_CLEAN_UNTIL("metastore.aggregate.stats.cache.clean.until",
        "hive.metastore.aggregate.stats.cache.clean.until", 0.8,
        "The cleaner thread cleans until cache reaches this % full size."),
    AGGREGATE_STATS_CACHE_ENABLED("metastore.aggregate.stats.cache.enabled",
        "hive.metastore.aggregate.stats.cache.enabled", true,
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
    ALTER_HANDLER("metastore.alter.handler", "hive.metastore.alter.impl",
        HiveAlterHandler.class.getName(),
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
        "Maximum number of objects (tables/partitions) can be retrieved from metastore in one batch. \n" +
            "The higher the number, the less the number of round trips is needed to the Hive metastore server, \n" +
            "but it may also cause higher memory requirement at the client side."),
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
    CAPABILITY_CHECK("metastore.client.capability.check",
        "hive.metastore.client.capability.check", true,
        "Whether to check client capabilities for potentially breaking API usage."),
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
    COMPACTOR_HISTORY_REAPER_INTERVAL("metastore.compactor.history.reaper.interval",
        "hive.compactor.history.reaper.interval", 2, TimeUnit.MINUTES,
        "Determines how often compaction history reaper runs"),
    COMPACTOR_HISTORY_RETENTION_ATTEMPTED("metastore.compactor.history.retention.attempted",
        "hive.compactor.history.retention.attempted", 2,
        new Validator.RangeValidator(0, 100), "Determines how many attempted compaction records will be " +
        "retained in compaction history for a given table/partition."),
    COMPACTOR_HISTORY_RETENTION_FAILED("metastore.compactor.history.retention.failed",
        "hive.compactor.history.retention.failed", 3,
        new Validator.RangeValidator(0, 100), "Determines how many failed compaction records will be " +
        "retained in compaction history for a given table/partition."),
    COMPACTOR_HISTORY_RETENTION_SUCCEEDED("metastore.compactor.history.retention.succeeded",
        "hive.compactor.history.retention.succeeded", 3,
        new Validator.RangeValidator(0, 100), "Determines how many successful compaction records will be " +
        "retained in compaction history for a given table/partition."),
    COMPACTOR_INITIATOR_FAILED_THRESHOLD("metastore.compactor.initiator.failed.compacts.threshold",
        "hive.compactor.initiator.failed.compacts.threshold", 2,
        new Validator.RangeValidator(1, 20), "Number of consecutive compaction failures (per table/partition) " +
        "after which automatic compactions will not be scheduled any more.  Note that this must be less " +
        "than hive.compactor.history.retention.failed."),
    COMPACTOR_INITIATOR_ON("metastore.compactor.initiator.on", "hive.compactor.initiator.on", false,
        "Whether to run the initiator and cleaner threads on this metastore instance or not.\n" +
            "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
            "on Hive transactions. For a complete list of parameters required for turning on\n" +
            "transactions, see hive.txn.manager."),
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
    CONNECTURLHOOK("metastore.ds.connection.url.hook",
        "hive.metastore.ds.connection.url.hook", "",
        "Name of the hook to use for retrieving the JDO connection URL. If empty, the value in javax.jdo.option.ConnectionURL is used"),
    CONNECTURLKEY("javax.jdo.option.ConnectionURL",
        "javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=metastore_db;create=true",
        "JDBC connect string for a JDBC metastore.\n" +
            "To use SSL to encrypt/authenticate the connection, provide database-specific SSL flag in the connection URL.\n" +
            "For example, jdbc:postgresql://myhost/db?ssl=true for postgres database."),
    CONNECTION_POOLING_TYPE("datanucleus.connectionPoolingType",
        "datanucleus.connectionPoolingType", "HikariCP", new Validator.StringSet("BONECP", "DBCP",
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
        "datanucleus.autoStartMechanismMode", "ignored", new Validator.StringSet("ignored"),
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
    DBACCESS_SSL_PROPS("metastore.dbaccess.ssl.properties", "hive.metastore.dbaccess.ssl.properties", "",
        "Comma-separated SSL properties for metastore to access database when JDO connection URL\n" +
            "enables SSL access. e.g. javax.net.ssl.trustStore=/tmp/truststore,javax.net.ssl.trustStorePassword=pwd."),
    DEFAULTPARTITIONNAME("metastore.default.partition.name",
        "hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__",
        "The default partition name in case the dynamic partition column value is null/empty string or any other values that cannot be escaped. \n" +
            "This value must not contain any special character used in HDFS URI (e.g., ':', '%', '/' etc). \n" +
            "The user has to be aware that the dynamic partition value should not contain this value to avoid confusions."),
    DELEGATION_KEY_UPDATE_INTERVAL("metastore.cluster.delegation.key.update-interval",
        "hive.cluster.delegation.key.update-interval", 1, TimeUnit.DAYS, ""),
    DELEGATION_TOKEN_GC_INTERVAL("metastore.cluster.delegation.token.gc-interval",
        "hive.cluster.delegation.token.gc-interval", 1, TimeUnit.HOURS, ""),
    DELEGATION_TOKEN_MAX_LIFETIME("metastore.cluster.delegation.token.max-lifetime",
        "hive.cluster.delegation.token.max-lifetime", 7, TimeUnit.DAYS, ""),
    DELEGATION_TOKEN_RENEW_INTERVAL("metastore.cluster.delegation.token.renew-interval",
      "hive.cluster.delegation.token.renew-interval", 1, TimeUnit.DAYS, ""),
    DELEGATION_TOKEN_STORE_CLS("metastore.cluster.delegation.token.store.class",
        "hive.cluster.delegation.token.store.class", MetastoreDelegationTokenManager.class.getName(),
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
    DUMP_CONFIG_ON_CREATION("metastore.dump.config.on.creation", NO_SUCH_KEY, true,
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
        "org.apache.hadoop.hive.metastore.messaging.json.JSONMessageFactory",
        "Factory class for making encoding and decoding messages in the events generated."),
    EVENT_DB_LISTENER_TTL("metastore.event.db.listener.timetolive",
        "hive.metastore.event.db.listener.timetolive", 86400, TimeUnit.SECONDS,
        "time after which events will be removed from the database listener queue"),
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
    HMSHANDLERATTEMPTS("metastore.hmshandler.retry.attempts", "hive.hmshandler.retry.attempts", 10,
        "The number of times to retry a HMSHandler call if there were a connection error."),
    HMSHANDLERFORCERELOADCONF("metastore.hmshandler.force.reload.conf",
        "hive.hmshandler.force.reload.conf", false,
        "Whether to force reloading of the HMSHandler configuration (including\n" +
            "the connection URL, before the next metastore query that accesses the\n" +
            "datastore. Once reloaded, this value is reset to false. Used for\n" +
            "testing only."),
    HMSHANDLERINTERVAL("metastore.hmshandler.retry.interval", "hive.hmshandler.retry.interval",
        2000, TimeUnit.MILLISECONDS, "The time between HMSHandler retry attempts on failure."),
    IDENTIFIER_FACTORY("datanucleus.identifierFactory",
        "datanucleus.identifierFactory", "datanucleus1",
        "Name of the identifier factory to use when generating table/column names etc. \n" +
            "'datanucleus1' is used for backward compatibility with DataNucleus v1"),
    INIT_HOOKS("metastore.init.hooks", "hive.metastore.init.hooks", "",
        "A comma separated list of hooks to be invoked at the beginning of HMSHandler initialization. \n" +
            "An init hook is specified as the name of Java class which extends org.apache.riven.MetaStoreInitListener."),
    INIT_METADATA_COUNT_ENABLED("metastore.initial.metadata.count.enabled",
        "hive.metastore.initial.metadata.count.enabled", true,
        "Enable a metadata count at metastore startup for metrics."),
    INTEGER_JDO_PUSHDOWN("metastore.integral.jdo.pushdown",
        "hive.metastore.integral.jdo.pushdown", false,
        "Allow JDO query pushdown for integral partition columns in metastore. Off by default. This\n" +
            "improves metastore perf for integral columns, especially if there's a large number of partitions.\n" +
            "However, it doesn't work correctly with integral values that are not normalized (e.g. have\n" +
            "leading zeroes, like 0012). If metastore direct SQL is enabled and works, this optimization\n" +
            "is also irrelevant."),
    KERBEROS_KEYTAB_FILE("metastore.kerberos.keytab.file",
        "hive.metastore.kerberos.keytab.file", "",
        "The path to the Kerberos Keytab file containing the metastore Thrift server's service principal."),
    KERBEROS_PRINCIPAL("metastore.kerberos.principal", "hive.metastore.kerberos.principal",
        "hive-metastore/_HOST@EXAMPLE.COM",
        "The service principal for the metastore Thrift server. \n" +
            "The special string _HOST will be replaced automatically with the correct host name."),
    LIMIT_PARTITION_REQUEST("metastore.limit.partition.request",
        "hive.metastore.limit.partition.request", -1,
        "This limits the number of partitions (whole partition objects) that can be requested " +
        "from the metastore for a give table. MetaStore API methods using this are: \n" +
                "get_partitions, \n" +
                "get_partitions_with_auth, \n" +
                "get_partitions_by_filter, \n" +
                "get_partitions_by_expr.\n" +
            "The default value \"-1\" means no limit."),
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
        new Validator.StringSet("DEFAULT", "DISABLE"),
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
    // Parameters for exporting metadata on table drop (requires the use of the)
    // org.apache.hadoop.hive.ql.parse.MetaDataExportListener preevent listener
    METADATA_EXPORT_LOCATION("metastore.metadata.export.location", "hive.metadata.export.location",
        "",
        "When used in conjunction with the org.apache.hadoop.hive.ql.parse.MetaDataExportListener pre event listener, \n" +
            "it is the location to which the metadata will be exported. The default is an empty string, which results in the \n" +
            "metadata being exported to the current user's home directory on HDFS."),
    MOVE_EXPORTED_METADATA_TO_TRASH("metastore.metadata.move.exported.metadata.to.trash",
        "hive.metadata.move.exported.metadata.to.trash", true,
        "When used in conjunction with the org.apache.hadoop.hive.ql.parse.MetaDataExportListener pre event listener, \n" +
            "this setting determines if the metadata that is exported will subsequently be moved to the user's trash directory \n" +
            "alongside the dropped table data. This ensures that the metadata will be cleaned up along with the dropped table data."),
    METRICS_ENABLED("metastore.metrics.enabled", "hive.metastore.metrics.enabled", false,
        "Enable metrics on the metastore."),
    METRICS_JSON_FILE_INTERVAL("metastore.metrics.file.frequency",
        "hive.service.metrics.file.frequency", 1, TimeUnit.MINUTES,
        "For json metric reporter, the frequency of updating JSON metrics file."),
    METRICS_JSON_FILE_LOCATION("metastore.metrics.file.location",
        "hive.service.metrics.file.location", "/tmp/report.json",
        "For metric class json metric reporter, the location of local JSON metrics file.  " +
            "This file will get overwritten at every interval."),
    METRICS_REPORTERS("metastore.metrics.reporters", NO_SUCH_KEY, "json,jmx",
        new Validator.StringSet("json", "jmx", "console", "hadoop"),
        "A comma separated list of metrics reporters to start"),
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
        "hive.notification.sequence.lock.max.retries", 5,
        "Number of retries required to acquire a lock when getting the next notification sequential ID for entries "
            + "in the NOTIFICATION_LOG table."),
    NOTIFICATION_SEQUENCE_LOCK_RETRY_SLEEP_INTERVAL(
        "metastore.notification.sequence.lock.retry.sleep.interval",
        "hive.notification.sequence.lock.retry.sleep.interval", 500, TimeUnit.MILLISECONDS,
        "Sleep interval between retries to acquire a notification lock as described part of property "
            + NOTIFICATION_SEQUENCE_LOCK_MAX_RETRIES.name()),
    ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS("metastore.orm.retrieveMapNullsAsEmptyStrings",
        "hive.metastore.orm.retrieveMapNullsAsEmptyStrings",false,
        "Thrift does not support nulls in maps, so any nulls present in maps retrieved from ORM must " +
            "either be pruned or converted to empty strings. Some backing dbs such as Oracle persist empty strings " +
            "as nulls, so we should set this parameter if we wish to reverse that behaviour. For others, " +
            "pruning is the correct behaviour"),
    PARTITION_NAME_WHITELIST_PATTERN("metastore.partition.name.whitelist.pattern",
        "hive.metastore.partition.name.whitelist.pattern", "",
        "Partition names will be checked against this regex pattern and rejected if not matched."),
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
    REPLCMDIR("metastore.repl.cmrootdir", "hive.repl.cmrootdir", "/user/hive/cmroot/",
        "Root dir for ChangeManager, used for deleted files."),
    REPLCMRETIAN("metastore.repl.cm.retain", "hive.repl.cm.retain",  24, TimeUnit.HOURS,
        "Time to retain removed files in cmrootdir."),
    REPLCMINTERVAL("metastore.repl.cm.interval", "hive.repl.cm.interval", 3600, TimeUnit.SECONDS,
        "Inteval for cmroot cleanup thread."),
    REPLCMENABLED("metastore.repl.cm.enabled", "hive.repl.cm.enabled", false,
        "Turn on ChangeManager, so delete files will go to cmrootdir."),
    REPLDIR("metastore.repl.rootdir", "hive.repl.rootdir", "/user/hive/repl/",
        "HDFS root dir for all replication dumps."),
    REPL_COPYFILE_MAXNUMFILES("metastore.repl.copyfile.maxnumfiles",
        "hive.exec.copyfile.maxnumfiles", 1L,
        "Maximum number of files Hive uses to do sequential HDFS copies between directories." +
            "Distributed copies (distcp) will be used instead for larger numbers of files so that copies can be done faster."),
    REPL_COPYFILE_MAXSIZE("metastore.repl.copyfile.maxsize",
        "hive.exec.copyfile.maxsize", 32L * 1024 * 1024 /*32M*/,
        "Maximum file size (in bytes) that Hive uses to do single HDFS copies between directories." +
            "Distributed copies (distcp) will be used instead for bigger files so that copies can be done faster."),
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
            "org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe," +
            "org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe," +
            "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe," +
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe," +
            "org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe",
        "SerDes retrieving schema from metastore. This is an internal parameter."),
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
    SSL_PROTOCOL_BLACKLIST("metastore.ssl.protocol.blacklist", "hive.ssl.protocol.blacklist",
        "SSLv2,SSLv3", "SSL Versions to disable for all Hive Servers"),
    SSL_TRUSTSTORE_PATH("metastore.truststore.path", "hive.metastore.truststore.path", "",
        "Metastore SSL certificate truststore location."),
    SSL_TRUSTSTORE_PASSWORD("metastore.truststore.password", "hive.metastore.truststore.password", "",
        "Metastore SSL certificate truststore password."),
    STATS_AUTO_GATHER("metastore.stats.autogather", "hive.stats.autogather", true,
        "A flag to gather statistics (only basic) automatically during the INSERT OVERWRITE command."),
    STATS_FETCH_BITVECTOR("metastore.stats.fetch.bitvector", "hive.stats.fetch.bitvector", false,
        "Whether we fetch bitvector when we compute ndv. Users can turn it off if they want to use old schema"),
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
    STORAGE_SCHEMA_READER_IMPL("metastore.storage.schema.reader.impl", NO_SUCH_KEY,
        DefaultStorageSchemaReader.class.getName(),
        "The class to use to read schemas from storage.  It must implement " +
        "org.apache.hadoop.hive.metastore.StorageSchemaReader"),
    STORE_MANAGER_TYPE("datanucleus.storeManagerType", "datanucleus.storeManagerType", "rdbms", "metadata store type"),
    SUPPORT_SPECICAL_CHARACTERS_IN_TABLE_NAMES("metastore.support.special.characters.tablename",
        "hive.support.special.characters.tablename", true,
        "This flag should be set to true to enable support for special characters in table names.\n"
            + "When it is set to false, only [a-zA-Z_0-9]+ are supported.\n"
            + "The only supported special character right now is '/'. This flag applies only to quoted table names.\n"
            + "The default value is true."),
    TASK_THREADS_ALWAYS("metastore.task.threads.always", "metastore.task.threads.always",
        EventCleanerTask.class.getName() + "," +
        "org.apache.hadoop.hive.metastore.repl.DumpDirCleanerTask" + "," +
        MaterializationsCacheCleanerTask.class.getName(),
        "Comma separated list of tasks that will be started in separate threads.  These will " +
            "always be started, regardless of whether the metastore is running in embedded mode " +
            "or in server mode.  They must implement " + MetastoreTaskThread.class.getName()),
    TASK_THREADS_REMOTE_ONLY("metastore.task.threads.remote", "metastore.task.threads.remote",
        AcidHouseKeeperService.class.getName() + "," +
            AcidOpenTxnsCounterService.class.getName() + "," +
            AcidCompactionHistoryService.class.getName() + "," +
            AcidWriteSetService.class.getName(),
        "Command separated list of tasks that will be started in separate threads.  These will be" +
            " started only when the metastore is running as a separate service.  They must " +
            "implement " + MetastoreTaskThread.class.getName()),
    TCP_KEEP_ALIVE("metastore.server.tcp.keepalive",
        "hive.metastore.server.tcp.keepalive", true,
        "Whether to enable TCP keepalive for the metastore server. Keepalive will prevent accumulation of half-open connections."),
    THREAD_POOL_SIZE("metastore.thread.pool.size", "no.such", 10,
        "Number of threads in the thread pool.  These will be used to execute all background " +
            "processes."),
    THRIFT_CONNECTION_RETRIES("metastore.connect.retries", "hive.metastore.connect.retries", 3,
        "Number of retries while opening a connection to metastore"),
    THRIFT_FAILURE_RETRIES("metastore.failure.retries", "hive.metastore.failure.retries", 1,
        "Number of retries upon failure of Thrift metastore calls"),
    THRIFT_URIS("metastore.thrift.uris", "hive.metastore.uris", "",
        "Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore."),
    THRIFT_URI_SELECTION("metastore.thrift.uri.selection", "hive.metastore.uri.selection", "RANDOM",
        new Validator.StringSet("RANDOM", "SEQUENTIAL"),
        "Determines the selection mechanism used by metastore client to connect to remote " +
        "metastore.  SEQUENTIAL implies that the first valid metastore from the URIs specified " +
        "as part of hive.metastore.uris will be picked.  RANDOM implies that the metastore " +
        "will be picked randomly"),
    TIMEDOUT_TXN_REAPER_START("metastore.timedout.txn.reaper.start",
        "hive.timedout.txn.reaper.start", 100, TimeUnit.SECONDS,
        "Time delay of 1st reaper run after metastore start"),
    TIMEDOUT_TXN_REAPER_INTERVAL("metastore.timedout.txn.reaper.interval",
        "hive.timedout.txn.reaper.interval", 180, TimeUnit.SECONDS,
        "Time interval describing how often the reaper runs"),
    TOKEN_SIGNATURE("metastore.token.signature", "hive.metastore.token.signature", "",
        "The delegation token service name to match when selecting a token from the current user's tokens."),
    TRANSACTIONAL_EVENT_LISTENERS("metastore.transactional.event.listeners",
        "hive.metastore.transactional.event.listeners", "",
        "A comma separated list of Java classes that implement the org.apache.riven.MetaStoreEventListener" +
            " interface. Both the metastore event and corresponding listener method will be invoked in the same JDO transaction."),
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
    URI_RESOLVER("metastore.uri.resolver", "hive.metastore.uri.resolver", "",
            "If set, fully qualified class name of resolver for hive metastore uri's"),
    USERS_IN_ADMIN_ROLE("metastore.users.in.admin.role", "hive.users.in.admin.role", "", false,
        "Comma separated list of users who are in admin role for bootstrapping.\n" +
            "More users can be added in ADMIN role later."),
    USE_SSL("metastore.use.SSL", "hive.metastore.use.SSL", false,
        "Set this to true for using SSL encryption in HMS server."),
    USE_THRIFT_SASL("metastore.sasl.enabled", "hive.metastore.sasl.enabled", false,
        "If true, the metastore Thrift interface will be secured with SASL. Clients must authenticate with Kerberos."),
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
    WRITE_SET_REAPER_INTERVAL("metastore.writeset.reaper.interval",
        "hive.writeset.reaper.interval", 60, TimeUnit.SECONDS,
        "Frequency of WriteSet reaper runs"),
    WM_DEFAULT_POOL_SIZE("metastore.wm.default.pool.size",
        "hive.metastore.wm.default.pool.size", 4,
        "The size of a default pool to create when creating an empty resource plan;\n" +
        "If not positive, no default pool will be created."),

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
    HIVE_METASTORE_AUTHORIZATION_MANAGER(NO_SUCH_KEY,
        "hive.security.metastore.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveMetastoreAuthorizationProvider",
        "Names of authorization manager classes (comma separated) to be used in the metastore\n" +
            "for authorization. The user defined authorization class should implement interface\n" +
            "org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider.\n" +
            "All authorization manager classes have to successfully authorize the metastore API\n" +
            "call for the command execution to be allowed."),
    // The metastore shouldn't care what txn manager Hive is running, but in various tests it
    // needs to set these values.  We should do the work to detangle this.
    HIVE_TXN_MANAGER("hive.txn.manager", "hive.txn.manager",
        "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager",
        "Set to org.apache.hadoop.hive.ql.lockmgr.DbTxnManager as part of turning on Hive\n" +
            "transactions, which also requires appropriate settings for hive.compactor.initiator.on,\n" +
            "hive.compactor.worker.threads, hive.support.concurrency (true),\n" +
            "and hive.exec.dynamic.partition.mode (nonstrict).\n" +
            "The default DummyTxnManager replicates pre-Hive-0.13 behavior and provides\n" +
            "no transactions."),
    // Metastore always support concurrency, but certain ACID tests depend on this being set.  We
    // need to do the work to detangle this
    HIVE_SUPPORT_CONCURRENCY("hive.support.concurrency", "hive.support.concurrency", false,
        "Whether Hive supports concurrency control or not. \n" +
            "A ZooKeeper instance must be up and running when using zookeeper Hive lock manager "),

    // Deprecated Hive values that we are keeping for backwards compatibility.
    @Deprecated
    HIVE_CODAHALE_METRICS_REPORTER_CLASSES(NO_SUCH_KEY,
        "hive.service.metrics.codahale.reporter.classes", "",
        "Use METRICS_REPORTERS instead.  Comma separated list of reporter implementation classes " +
            "for metric class org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics. Overrides "
            + "HIVE_METRICS_REPORTER conf if present.  This will be overridden by " +
            "METRICS_REPORTERS if it is present"),
    @Deprecated
    HIVE_METRICS_REPORTER(NO_SUCH_KEY, "hive.service.metrics.reporter", "",
        "Reporter implementations for metric class "
            + "org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;" +
            "Deprecated, use METRICS_REPORTERS instead. This configuraiton will be"
            + " overridden by HIVE_CODAHALE_METRICS_REPORTER_CLASSES and METRICS_REPORTERS if " +
            "present. Comma separated list of JMX, CONSOLE, JSON_FILE, HADOOP2"),

    // These are all values that we put here just for testing
    STR_TEST_ENTRY("test.str", "hive.test.str", "defaultval", "comment"),
    STR_SET_ENTRY("test.str.set", NO_SUCH_KEY, "a", new Validator.StringSet("a", "b", "c"), ""),
    STR_LIST_ENTRY("test.str.list", "hive.test.str.list", "a,b,c",
        "no comment"),
    LONG_TEST_ENTRY("test.long", "hive.test.long", 42, "comment"),
    DOUBLE_TEST_ENTRY("test.double", "hive.test.double", 3.141592654, "comment"),
    TIME_TEST_ENTRY("test.time", "hive.test.time", 1, TimeUnit.SECONDS, "comment"),
    TIME_VALIDATOR_ENTRY_INCLUSIVE("test.time.validator.inclusive", NO_SUCH_KEY, 1,
        TimeUnit.SECONDS,
        new Validator.TimeValidator(TimeUnit.MILLISECONDS, 500L, true, 1500L, true), "comment"),
    TIME_VALIDATOR_ENTRY_EXCLUSIVE("test.time.validator.exclusive", NO_SUCH_KEY, 1,
        TimeUnit.SECONDS,
        new Validator.TimeValidator(TimeUnit.MILLISECONDS, 500L, false, 1500L, false), "comment"),
    BOOLEAN_TEST_ENTRY("test.bool", "hive.test.bool", true, "comment"),
    CLASS_TEST_ENTRY("test.class", "hive.test.class", "", "comment");

    private final String varname;
    private final String hiveName;
    private final Object defaultVal;
    private final Validator validator;
    private final boolean caseSensitive;
    private final String description;

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
      validator = new Validator.TimeValidator(unit);
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
     * you call conf.get(varname) you are undermining that.
     * @return variable name
     */
    public String getVarname() {
      return varname;
    }

    /**
     * If you are calling this, you're probably doing it wrong.  You shouldn't need to use the
     * underlying variable name.  Use one of the getVar methods instead.  Only use this if you
     * are 100% sure you know you're doing.  The reason for this is that MetastoreConf goes to a
     * lot of trouble to make sure it checks both Hive and Metastore values for config keys.  If
     * you call conf.get(hivename) you are undermining that.
     * @return variable hive name
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
      ConfVars.CONNECTURLKEY,
      ConfVars.CONNECTION_USER_NAME,
      ConfVars.DATANUCLEUS_AUTOSTART,
      ConfVars.DATANUCLEUS_CACHE_LEVEL2,
      ConfVars.DATANUCLEUS_CACHE_LEVEL2_TYPE,
      ConfVars.DATANUCLEUS_INIT_COL_INFO,
      ConfVars.DATANUCLEUS_PLUGIN_REGISTRY_BUNDLE_CHECK,
      ConfVars.DATANUCLEUS_TRANSACTION_ISOLATION,
      ConfVars.DATANUCLEUS_USE_LEGACY_VALUE_STRATEGY,
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

    Configuration conf = new Configuration();

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
       * HiveConf uses data/conf/spark/hive-site.xml and MetastoreConf data/conf/hive-site.xml)
       */
      hiveSiteURL = findConfigFile(classLoader, "hive-site.xml");
    }
    if (hiveSiteURL != null) conf.addResource(hiveSiteURL);

    // Now add hivemetastore-site.xml.  Again we add this before our own config files so that the
    // newer overrides the older.
    hiveMetastoreSiteURL = findConfigFile(classLoader, "hivemetastore-site.xml");
    if (hiveMetastoreSiteURL != null) conf.addResource(hiveMetastoreSiteURL);

    // Add in our conf file
    metastoreSiteURL = findConfigFile(classLoader, "metastore-site.xml");
    if (metastoreSiteURL !=  null) conf.addResource(metastoreSiteURL);

    // If a system property that matches one of our conf value names is set then use the value
    // it's set to to set our own conf value.
    for (ConfVars var : ConfVars.values()) {
      if (System.getProperty(var.varname) != null) {
        LOG.debug("Setting conf value " + var.varname + " using value " +
            System.getProperty(var.varname));
        conf.set(var.varname, System.getProperty(var.varname));
      } else if (System.getProperty(var.hiveName) != null) {
        conf.set(var.hiveName, System.getProperty(var.hiveName));
      }
    }

    // If we are going to validate the schema, make sure we don't create it
    if (getBoolVar(conf, ConfVars.SCHEMA_VERIFICATION)) {
      setBoolVar(conf, ConfVars.AUTO_CREATE_ALL, false);
    }

    if (!beenDumped.getAndSet(true) && getBoolVar(conf, ConfVars.DUMP_CONFIG_ON_CREATION) &&
        LOG.isInfoEnabled()) {
      LOG.info(dumpConfig(conf));
    }
    return conf;
  }

  private static URL findConfigFile(ClassLoader classLoader, String name) {
    // First, look in the classpath
    URL result = classLoader.getResource(name);
    if (result == null) {
      // Nope, so look to see if our conf dir has been explicitly set
      result = seeIfConfAtThisLocation("METASTORE_CONF_DIR", name, false);
      if (result == null) {
        // Nope, so look to see if our home dir has been explicitly set
        result = seeIfConfAtThisLocation("METASTORE_HOME", name, true);
        if (result == null) {
          // Nope, so look to see if Hive's conf dir has been explicitly set
          result = seeIfConfAtThisLocation("HIVE_CONF_DIR", name, false);
          if (result == null) {
            // Nope, so look to see if Hive's home dir has been explicitly set
            result = seeIfConfAtThisLocation("HIVE_HOME", name, true);
            if (result == null) {
              // Nope, so look to see if we can find a conf file by finding our jar, going up one
              // directory, and looking for a conf directory.
              URI jarUri = null;
              try {
                jarUri = MetastoreConf.class.getProtectionDomain().getCodeSource().getLocation().toURI();
              } catch (Throwable e) {
                LOG.warn("Cannot get jar URI", e);
              }
              result = seeIfConfAtThisLocation(new File(jarUri).getParent(), name, true);
              // At this point if we haven't found it, screw it, we don't know where it is
              if (result == null) {
                LOG.info("Unable to find config file " + name);
              }
            }
          }
        }
      }
    }
    LOG.info("Found configuration file " + result);
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
    if (val == null) val = conf.get(var.hiveName, (String)var.defaultVal);
    if (val == null) return Collections.emptySet();
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
    return val == null ? conf.getLong(var.hiveName, (Long)var.defaultVal) : Long.valueOf(val);
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

  private static String timeAbbreviationFor(TimeUnit timeunit) {
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
    // Map this key back to the ConfVars it is associated with.
    if (keyToVars == null) {
      synchronized (MetastoreConf.class) {
        if (keyToVars == null) {
          keyToVars = new HashMap<>(ConfVars.values().length * 2);
          for (ConfVars var : ConfVars.values()) {
            keyToVars.put(var.varname, var);
            keyToVars.put(var.hiveName, var);
          }
        }
      }
    }
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
   * @return value as an object
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
      TimeUnit timeUnit = (var.defaultVal.getClass() == TimeValue.class) ?
          ((TimeValue)var.defaultVal).unit : null;
      return Long.toString(getTimeVar(conf, var, timeUnit)) + timeAbbreviationFor(timeUnit);
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

  /**
   * Dump the configuration file to the log.  It will be dumped at an INFO level.  This can
   * potentially produce a lot of logs, so you might want to be careful when and where you do it.
   * It takes care not to dump hidden keys.
   * @param conf Configuration file to dump
   * @return String containing dumped config file.
   */
  public static String dumpConfig(Configuration conf) {
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

  /**
   * validate value for a ConfVar, return non-null string for fail message
   */
  public interface Validator {

    /**
     * Validate if the given value is acceptable.
     * @param value value to test
     * @throws IllegalArgumentException if the value is invalid
     */
    void validate(String value) throws IllegalArgumentException;

    class StringSet implements Validator {

      private final boolean caseSensitive;
      private final Set<String> expected = new LinkedHashSet<String>();

      public StringSet(String... values) {
        this(false, values);
      }

      public StringSet(boolean caseSensitive, String... values) {
        this.caseSensitive = caseSensitive;
        for (String value : values) {
          expected.add(caseSensitive ? value : value.toLowerCase());
        }
      }

      public Set<String> getExpected() {
        return new HashSet<String>(expected);
      }

      @Override
      public void validate(String value) {
        if (value == null || !expected.contains(caseSensitive ? value : value.toLowerCase())) {
          throw new IllegalArgumentException("Invalid value.. expects one of " + expected);
        }
      }

    }

    enum TYPE {
      INT {
        @Override
        protected boolean inRange(String value, Object lower, Object upper) {
          int ivalue = Integer.parseInt(value);
          if (lower != null && ivalue < (Integer)lower) {
            return false;
          }
          if (upper != null && ivalue > (Integer)upper) {
            return false;
          }
          return true;
        }
      },
      LONG {
        @Override
        protected boolean inRange(String value, Object lower, Object upper) {
          long lvalue = Long.parseLong(value);
          if (lower != null && lvalue < (Long)lower) {
            return false;
          }
          if (upper != null && lvalue > (Long)upper) {
            return false;
          }
          return true;
        }
      },
      FLOAT {
        @Override
        protected boolean inRange(String value, Object lower, Object upper) {
          float fvalue = Float.parseFloat(value);
          if (lower != null && fvalue < (Float)lower) {
            return false;
          }
          if (upper != null && fvalue > (Float)upper) {
            return false;
          }
          return true;
        }
      };

      public static TYPE valueOf(Object lower, Object upper) {
        if (lower instanceof Integer || upper instanceof Integer) {
          return INT;
        } else if (lower instanceof Long || upper instanceof Long) {
          return LONG;
        } else if (lower instanceof Float || upper instanceof Float) {
          return FLOAT;
        }
        throw new IllegalArgumentException("invalid range from " + lower + " to " + upper);
      }

      protected abstract boolean inRange(String value, Object lower, Object upper);
    }

    class RangeValidator implements Validator {

      private final TYPE type;
      private final Object lower, upper;

      public RangeValidator(Object lower, Object upper) {
        this.lower = lower;
        this.upper = upper;
        this.type = TYPE.valueOf(lower, upper);
      }

      @Override
      public void validate(String value) {
        if (value == null || !type.inRange(value.trim(), lower, upper)) {
          throw new IllegalArgumentException("Invalid value  " + value +
              ", which should be in between " + lower + " and " + upper);
        }
      }
    }

    class TimeValidator implements Validator {

      private final TimeUnit unit;
      private final Long min;
      private final boolean minInclusive;

      private final Long max;
      private final boolean maxInclusive;

      public TimeValidator(TimeUnit unit) {
        this(unit, null, false, null, false);
      }

      public TimeValidator(TimeUnit unit, Long min, boolean minInclusive, Long max,
                           boolean maxInclusive) {
        this.unit = unit;
        this.min = min;
        this.minInclusive = minInclusive;
        this.max = max;
        this.maxInclusive = maxInclusive;
      }

      @Override
      public void validate(String value) {
        // First just check that this translates
        TimeUnit defaultUnit = unit;
        long time = convertTimeStr(value, defaultUnit, defaultUnit);
        if (min != null) {
          if (minInclusive ? time < min : time <= min) {
            throw new IllegalArgumentException(value + " is smaller than minimum " + min +
                timeAbbreviationFor(defaultUnit));
          }
        }

        if (max != null) {
          if (maxInclusive ? time > max : time >= max) {
            throw new IllegalArgumentException(value + " is larger than maximum " + max +
                timeAbbreviationFor(defaultUnit));
          }
        }
      }

      private String timeString(long time, TimeUnit timeUnit) {
        return time + " " + timeAbbreviationFor(timeUnit);
      }
    }
  }

}
