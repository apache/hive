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

package org.apache.hadoop.hive.conf;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;

/**
 * Hive Configuration.
 */
public class HiveConf extends Configuration {

  protected String hiveJar;
  protected Properties origProp;
  protected String auxJars;
  private static final Log l4j = LogFactory.getLog(HiveConf.class);
  private static URL hiveDefaultURL = null;
  private static URL hiveSiteURL = null;
  private static byte[] confVarByteArray = null;

  private static final Map<String, ConfVars> vars = new HashMap<String, ConfVars>();
  private final List<String> restrictList = new ArrayList<String>();

  static {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = HiveConf.class.getClassLoader();
    }

    hiveDefaultURL = classLoader.getResource("hive-default.xml");

    // Look for hive-site.xml on the CLASSPATH and log its location if found.
    hiveSiteURL = classLoader.getResource("hive-site.xml");
    for (ConfVars confVar : ConfVars.values()) {
      vars.put(confVar.varname, confVar);
    }
  }

  /**
   * Metastore related options that the db is initialized against. When a conf
   * var in this is list is changed, the metastore instance for the CLI will
   * be recreated so that the change will take effect.
   */
  public static final HiveConf.ConfVars[] metaVars = {
      HiveConf.ConfVars.METASTOREDIRECTORY,
      HiveConf.ConfVars.METASTOREWAREHOUSE,
      HiveConf.ConfVars.METASTOREURIS,
      HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES,
      HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES,
      HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY,
      HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,
      HiveConf.ConfVars.METASTOREPWD,
      HiveConf.ConfVars.METASTORECONNECTURLHOOK,
      HiveConf.ConfVars.METASTORECONNECTURLKEY,
      HiveConf.ConfVars.METASTOREATTEMPTS,
      HiveConf.ConfVars.METASTOREINTERVAL,
      HiveConf.ConfVars.METASTOREFORCERELOADCONF,
      HiveConf.ConfVars.METASTORESERVERMINTHREADS,
      HiveConf.ConfVars.METASTORESERVERMAXTHREADS,
      HiveConf.ConfVars.METASTORE_TCP_KEEP_ALIVE,
      HiveConf.ConfVars.METASTORE_INT_ORIGINAL,
      HiveConf.ConfVars.METASTORE_INT_ARCHIVED,
      HiveConf.ConfVars.METASTORE_INT_EXTRACTED,
      HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE,
      HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL,
      HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
      HiveConf.ConfVars.METASTORE_CACHE_PINOBJTYPES,
      HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE,
      HiveConf.ConfVars.METASTORE_VALIDATE_TABLES,
      HiveConf.ConfVars.METASTORE_VALIDATE_COLUMNS,
      HiveConf.ConfVars.METASTORE_VALIDATE_CONSTRAINTS,
      HiveConf.ConfVars.METASTORE_STORE_MANAGER_TYPE,
      HiveConf.ConfVars.METASTORE_AUTO_CREATE_SCHEMA,
      HiveConf.ConfVars.METASTORE_AUTO_START_MECHANISM_MODE,
      HiveConf.ConfVars.METASTORE_TRANSACTION_ISOLATION,
      HiveConf.ConfVars.METASTORE_CACHE_LEVEL2,
      HiveConf.ConfVars.METASTORE_CACHE_LEVEL2_TYPE,
      HiveConf.ConfVars.METASTORE_IDENTIFIER_FACTORY,
      HiveConf.ConfVars.METASTORE_PLUGIN_REGISTRY_BUNDLE_CHECK,
      HiveConf.ConfVars.METASTORE_AUTHORIZATION_STORAGE_AUTH_CHECKS,
      HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX,
      HiveConf.ConfVars.METASTORE_EVENT_LISTENERS,
      HiveConf.ConfVars.METASTORE_EVENT_CLEAN_FREQ,
      HiveConf.ConfVars.METASTORE_EVENT_EXPIRY_DURATION,
      HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
      HiveConf.ConfVars.METASTORE_END_FUNCTION_LISTENERS,
      HiveConf.ConfVars.METASTORE_PART_INHERIT_TBL_PROPS,
      HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_TABLE_PARTITION_MAX,
      HiveConf.ConfVars.METASTORE_INIT_HOOKS,
      HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS,
      HiveConf.ConfVars.HMSHANDLERATTEMPTS,
      HiveConf.ConfVars.HMSHANDLERINTERVAL,
      HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF,
      HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN,
      HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES
      };

  /**
   * dbVars are the parameters can be set per database. If these
   * parameters are set as a database property, when switching to that
   * database, the HiveConf variable will be changed. The change of these
   * parameters will effectively change the DFS and MapReduce clusters
   * for different databases.
   */
  public static final HiveConf.ConfVars[] dbVars = {
    HiveConf.ConfVars.HADOOPBIN,
    HiveConf.ConfVars.HADOOPJT,
    HiveConf.ConfVars.METASTOREWAREHOUSE,
    HiveConf.ConfVars.SCRATCHDIR
  };

  /**
   * ConfVars.
   *
   * These are the default configuration properties for Hive. Each HiveConf
   * object is initialized as follows:
   *
   * 1) Hadoop configuration properties are applied.
   * 2) ConfVar properties with non-null values are overlayed.
   * 3) hive-site.xml properties are overlayed.
   *
   * WARNING: think twice before adding any Hadoop configuration properties
   * with non-null values to this list as they will override any values defined
   * in the underlying Hadoop configuration.
   */
  public static enum ConfVars {
    // QL execution stuff
    SCRIPTWRAPPER("hive.exec.script.wrapper", null),
    PLAN("hive.exec.plan", ""),
    PLAN_SERIALIZATION("hive.plan.serialization.format","kryo"),
    SCRATCHDIR("hive.exec.scratchdir", "/tmp/hive-" + System.getProperty("user.name")),
    LOCALSCRATCHDIR("hive.exec.local.scratchdir", System.getProperty("java.io.tmpdir") + File.separator + System.getProperty("user.name")),
    SCRATCHDIRPERMISSION("hive.scratch.dir.permission", "700"),
    SUBMITVIACHILD("hive.exec.submitviachild", false),
    SCRIPTERRORLIMIT("hive.exec.script.maxerrsize", 100000),
    ALLOWPARTIALCONSUMP("hive.exec.script.allow.partial.consumption", false),
    STREAMREPORTERPERFIX("stream.stderr.reporter.prefix", "reporter:"),
    STREAMREPORTERENABLED("stream.stderr.reporter.enabled", true),
    COMPRESSRESULT("hive.exec.compress.output", false),
    COMPRESSINTERMEDIATE("hive.exec.compress.intermediate", false),
    COMPRESSINTERMEDIATECODEC("hive.intermediate.compression.codec", ""),
    COMPRESSINTERMEDIATETYPE("hive.intermediate.compression.type", ""),
    BYTESPERREDUCER("hive.exec.reducers.bytes.per.reducer", (long) (1000 * 1000 * 1000)),
    MAXREDUCERS("hive.exec.reducers.max", 999),
    PREEXECHOOKS("hive.exec.pre.hooks", ""),
    POSTEXECHOOKS("hive.exec.post.hooks", ""),
    ONFAILUREHOOKS("hive.exec.failure.hooks", ""),
    CLIENTSTATSPUBLISHERS("hive.client.stats.publishers", ""),
    EXECPARALLEL("hive.exec.parallel", false), // parallel query launching
    EXECPARALLETHREADNUMBER("hive.exec.parallel.thread.number", 8),
    HIVESPECULATIVEEXECREDUCERS("hive.mapred.reduce.tasks.speculative.execution", true),
    HIVECOUNTERSPULLINTERVAL("hive.exec.counters.pull.interval", 1000L),
    DYNAMICPARTITIONING("hive.exec.dynamic.partition", true),
    DYNAMICPARTITIONINGMODE("hive.exec.dynamic.partition.mode", "strict"),
    DYNAMICPARTITIONMAXPARTS("hive.exec.max.dynamic.partitions", 1000),
    DYNAMICPARTITIONMAXPARTSPERNODE("hive.exec.max.dynamic.partitions.pernode", 100),
    MAXCREATEDFILES("hive.exec.max.created.files", 100000L),
    DOWNLOADED_RESOURCES_DIR("hive.downloaded.resources.dir",
        System.getProperty("java.io.tmpdir") + File.separator  + "${hive.session.id}_resources"),
    DEFAULTPARTITIONNAME("hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__"),
    DEFAULT_ZOOKEEPER_PARTITION_NAME("hive.lockmgr.zookeeper.default.partition.name", "__HIVE_DEFAULT_ZOOKEEPER_PARTITION__"),
    // Whether to show a link to the most failed task + debugging tips
    SHOW_JOB_FAIL_DEBUG_INFO("hive.exec.show.job.failure.debug.info", true),
    JOB_DEBUG_CAPTURE_STACKTRACES("hive.exec.job.debug.capture.stacktraces", true),
    JOB_DEBUG_TIMEOUT("hive.exec.job.debug.timeout", 30000),
    TASKLOG_DEBUG_TIMEOUT("hive.exec.tasklog.debug.timeout", 20000),
    OUTPUT_FILE_EXTENSION("hive.output.file.extension", null),

    // should hive determine whether to run in local mode automatically ?
    LOCALMODEAUTO("hive.exec.mode.local.auto", false),
    // if yes:
    // run in local mode only if input bytes is less than this. 128MB by default
    LOCALMODEMAXBYTES("hive.exec.mode.local.auto.inputbytes.max", 134217728L),
    // run in local mode only if number of tasks (for map and reduce each) is
    // less than this
    LOCALMODEMAXINPUTFILES("hive.exec.mode.local.auto.input.files.max", 4),
    // if true, DROP TABLE/VIEW does not fail if table/view doesn't exist and IF EXISTS is
    // not specified
    DROPIGNORESNONEXISTENT("hive.exec.drop.ignorenonexistent", true),

    // ignore the mapjoin hint
    HIVEIGNOREMAPJOINHINT("hive.ignore.mapjoin.hint", true),

    // Max number of lines of footer user can set for a table file.
    HIVE_FILE_MAX_FOOTER("hive.file.max.footer", 100),

    // Hadoop Configuration Properties
    // Properties with null values are ignored and exist only for the purpose of giving us
    // a symbolic name to reference in the Hive source code. Properties with non-null
    // values will override any values set in the underlying Hadoop configuration.
    HADOOPBIN("hadoop.bin.path", findHadoopBinary()),
    HADOOPFS("fs.default.name", null),
    HIVE_FS_HAR_IMPL("fs.har.impl", "org.apache.hadoop.hive.shims.HiveHarFileSystem"),
    HADOOPMAPFILENAME("map.input.file", null),
    HADOOPMAPREDINPUTDIR("mapred.input.dir", null),
    HADOOPMAPREDINPUTDIRRECURSIVE("mapred.input.dir.recursive", false),
    HADOOPJT("mapred.job.tracker", null),
    MAPREDMAXSPLITSIZE("mapred.max.split.size", 256000000L),
    MAPREDMINSPLITSIZE("mapred.min.split.size", 1L),
    MAPREDMINSPLITSIZEPERNODE("mapred.min.split.size.per.rack", 1L),
    MAPREDMINSPLITSIZEPERRACK("mapred.min.split.size.per.node", 1L),
    // The number of reduce tasks per job. Hadoop sets this value to 1 by default
    // By setting this property to -1, Hive will automatically determine the correct
    // number of reducers.
    HADOOPNUMREDUCERS("mapred.reduce.tasks", -1),
    HADOOPJOBNAME("mapred.job.name", null),
    HADOOPSPECULATIVEEXECREDUCERS("mapred.reduce.tasks.speculative.execution", true),

    // Metastore stuff. Be sure to update HiveConf.metaVars when you add
    // something here!
    METASTOREDIRECTORY("hive.metastore.metadb.dir", ""),
    METASTOREWAREHOUSE("hive.metastore.warehouse.dir", "/user/hive/warehouse"),
    METASTOREURIS("hive.metastore.uris", ""),
    // Number of times to retry a connection to a Thrift metastore server
    METASTORETHRIFTCONNECTIONRETRIES("hive.metastore.connect.retries", 3),
    // Number of times to retry a Thrift metastore call upon failure
    METASTORETHRIFTFAILURERETRIES("hive.metastore.failure.retries", 1),

    // Number of seconds the client should wait between connection attempts
    METASTORE_CLIENT_CONNECT_RETRY_DELAY("hive.metastore.client.connect.retry.delay", 1),
    // Socket timeout for the client connection (in seconds)
    METASTORE_CLIENT_SOCKET_TIMEOUT("hive.metastore.client.socket.timeout", 20),
    METASTOREPWD("javax.jdo.option.ConnectionPassword", "mine"),
    // Class name of JDO connection url hook
    METASTORECONNECTURLHOOK("hive.metastore.ds.connection.url.hook", ""),
    METASTOREMULTITHREADED("javax.jdo.option.Multithreaded", true),
    // Name of the connection url in the configuration
    METASTORECONNECTURLKEY("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=metastore_db;create=true"),
    // Number of attempts to retry connecting after there is a JDO datastore err
    METASTOREATTEMPTS("hive.metastore.ds.retry.attempts", 1),
    // Number of miliseconds to wait between attepting
    METASTOREINTERVAL("hive.metastore.ds.retry.interval", 1000),
    // Whether to force reloading of the metastore configuration (including
    // the connection URL, before the next metastore query that accesses the
    // datastore. Once reloaded, this value is reset to false. Used for
    // testing only.
    METASTOREFORCERELOADCONF("hive.metastore.force.reload.conf", false),
    // Number of attempts to retry connecting after there is a JDO datastore err
    HMSHANDLERATTEMPTS("hive.hmshandler.retry.attempts", 1),
    // Number of miliseconds to wait between attepting
    HMSHANDLERINTERVAL("hive.hmshandler.retry.interval", 1000),
    // Whether to force reloading of the HMSHandler configuration (including
    // the connection URL, before the next metastore query that accesses the
    // datastore. Once reloaded, this value is reset to false. Used for
    // testing only.
    HMSHANDLERFORCERELOADCONF("hive.hmshandler.force.reload.conf", false),
    METASTORESERVERMINTHREADS("hive.metastore.server.min.threads", 200),
    METASTORESERVERMAXTHREADS("hive.metastore.server.max.threads", 100000),
    METASTORE_TCP_KEEP_ALIVE("hive.metastore.server.tcp.keepalive", true),
    // Intermediate dir suffixes used for archiving. Not important what they
    // are, as long as collisions are avoided
    METASTORE_INT_ORIGINAL("hive.metastore.archive.intermediate.original",
        "_INTERMEDIATE_ORIGINAL"),
    METASTORE_INT_ARCHIVED("hive.metastore.archive.intermediate.archived",
        "_INTERMEDIATE_ARCHIVED"),
    METASTORE_INT_EXTRACTED("hive.metastore.archive.intermediate.extracted",
        "_INTERMEDIATE_EXTRACTED"),
    METASTORE_KERBEROS_KEYTAB_FILE("hive.metastore.kerberos.keytab.file", ""),
    METASTORE_KERBEROS_PRINCIPAL("hive.metastore.kerberos.principal",
        "hive-metastore/_HOST@EXAMPLE.COM"),
    METASTORE_USE_THRIFT_SASL("hive.metastore.sasl.enabled", false),
    METASTORE_USE_THRIFT_FRAMED_TRANSPORT("hive.metastore.thrift.framed.transport.enabled", false),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS(
        "hive.cluster.delegation.token.store.class",
        "org.apache.hadoop.hive.thrift.MemoryTokenStore"),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_ZK_CONNECTSTR(
        "hive.cluster.delegation.token.store.zookeeper.connectString", ""),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_ZK_ZNODE(
        "hive.cluster.delegation.token.store.zookeeper.znode", "/hive/cluster/delegation"),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_ZK_ACL(
        "hive.cluster.delegation.token.store.zookeeper.acl", ""),
    METASTORE_CACHE_PINOBJTYPES("hive.metastore.cache.pinobjtypes", "Table,StorageDescriptor,SerDeInfo,Partition,Database,Type,FieldSchema,Order"),
    METASTORE_CONNECTION_POOLING_TYPE("datanucleus.connectionPoolingType", "BONECP"),
    METASTORE_VALIDATE_TABLES("datanucleus.validateTables", false),
    METASTORE_VALIDATE_COLUMNS("datanucleus.validateColumns", false),
    METASTORE_VALIDATE_CONSTRAINTS("datanucleus.validateConstraints", false),
    METASTORE_STORE_MANAGER_TYPE("datanucleus.storeManagerType", "rdbms"),
    METASTORE_AUTO_CREATE_SCHEMA("datanucleus.autoCreateSchema", true),
    METASTORE_FIXED_DATASTORE("datanucleus.fixedDatastore", false),
    METASTORE_SCHEMA_VERIFICATION("hive.metastore.schema.verification", false),
    METASTORE_AUTO_START_MECHANISM_MODE("datanucleus.autoStartMechanismMode", "checked"),
    METASTORE_TRANSACTION_ISOLATION("datanucleus.transactionIsolation", "read-committed"),
    METASTORE_CACHE_LEVEL2("datanucleus.cache.level2", false),
    METASTORE_CACHE_LEVEL2_TYPE("datanucleus.cache.level2.type", "none"),
    METASTORE_IDENTIFIER_FACTORY("datanucleus.identifierFactory", "datanucleus1"),
    METASTORE_USE_LEGACY_VALUE_STRATEGY("datanucleus.rdbms.useLegacyNativeValueStrategy", true),
    METASTORE_PLUGIN_REGISTRY_BUNDLE_CHECK("datanucleus.plugin.pluginRegistryBundleCheck", "LOG"),
    METASTORE_BATCH_RETRIEVE_MAX("hive.metastore.batch.retrieve.max", 300),
    METASTORE_BATCH_RETRIEVE_TABLE_PARTITION_MAX(
      "hive.metastore.batch.retrieve.table.partition.max", 1000),
    // A comma separated list of hooks which implement MetaStoreInitListener and will be run at
    // the beginning of HMSHandler initialization
    METASTORE_INIT_HOOKS("hive.metastore.init.hooks", ""),
    METASTORE_PRE_EVENT_LISTENERS("hive.metastore.pre.event.listeners", ""),
    METASTORE_EVENT_LISTENERS("hive.metastore.event.listeners", ""),
    // should we do checks against the storage (usually hdfs) for operations like drop_partition
    METASTORE_AUTHORIZATION_STORAGE_AUTH_CHECKS("hive.metastore.authorization.storage.checks", false),
    METASTORE_EVENT_CLEAN_FREQ("hive.metastore.event.clean.freq",0L),
    METASTORE_EVENT_EXPIRY_DURATION("hive.metastore.event.expiry.duration",0L),
    METASTORE_EXECUTE_SET_UGI("hive.metastore.execute.setugi", false),
    METASTORE_PARTITION_NAME_WHITELIST_PATTERN(
        "hive.metastore.partition.name.whitelist.pattern", ""),
    // Whether to enable integral JDO pushdown. For partition columns storing integers
    // in non-canonical form, (e.g. '012'), it may not work, so it's off by default.
    METASTORE_INTEGER_JDO_PUSHDOWN("hive.metastore.integral.jdo.pushdown", false),
    METASTORE_TRY_DIRECT_SQL("hive.metastore.try.direct.sql", true),
    METASTORE_TRY_DIRECT_SQL_DDL("hive.metastore.try.direct.sql.ddl", true),
    METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES(
        "hive.metastore.disallow.incompatible.col.type.changes", false),

    // Default parameters for creating tables
    NEWTABLEDEFAULTPARA("hive.table.parameters.default", ""),
    // Parameters to copy over when creating a table with Create Table Like.
    DDL_CTL_PARAMETERS_WHITELIST("hive.ddl.createtablelike.properties.whitelist", ""),
    METASTORE_RAW_STORE_IMPL("hive.metastore.rawstore.impl",
        "org.apache.hadoop.hive.metastore.ObjectStore"),
    METASTORE_CONNECTION_DRIVER("javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver"),
    METASTORE_MANAGER_FACTORY_CLASS("javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory"),
    METASTORE_EXPRESSION_PROXY_CLASS("hive.metastore.expression.proxy",
        "org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore"),
    METASTORE_DETACH_ALL_ON_COMMIT("javax.jdo.option.DetachAllOnCommit", true),
    METASTORE_NON_TRANSACTIONAL_READ("javax.jdo.option.NonTransactionalRead", true),
    METASTORE_CONNECTION_USER_NAME("javax.jdo.option.ConnectionUserName", "APP"),
    METASTORE_END_FUNCTION_LISTENERS("hive.metastore.end.function.listeners", ""),
    METASTORE_PART_INHERIT_TBL_PROPS("hive.metastore.partition.inherit.table.properties",""),

    // Parameters for exporting metadata on table drop (requires the use of the)
    // org.apache.hadoop.hive.ql.parse.MetaDataExportListener preevent listener
    METADATA_EXPORT_LOCATION("hive.metadata.export.location", ""),
    MOVE_EXPORTED_METADATA_TO_TRASH("hive.metadata.move.exported.metadata.to.trash", true),

    // CLI
    CLIIGNOREERRORS("hive.cli.errors.ignore", false),
    CLIPRINTCURRENTDB("hive.cli.print.current.db", false),
    CLIPROMPT("hive.cli.prompt", "hive"),
    CLIPRETTYOUTPUTNUMCOLS("hive.cli.pretty.output.num.cols", -1),

    HIVE_METASTORE_FS_HANDLER_CLS("hive.metastore.fs.handler.class", "org.apache.hadoop.hive.metastore.HiveMetaStoreFsImpl"),

    // Things we log in the jobconf

    // session identifier
    HIVESESSIONID("hive.session.id", ""),
    // whether session is running in silent mode or not
    HIVESESSIONSILENT("hive.session.silent", false),

    // Whether to enable history for this session
    HIVE_SESSION_HISTORY_ENABLED("hive.session.history.enabled", false),

    // query being executed (multiple per session)
    HIVEQUERYSTRING("hive.query.string", ""),

    // id of query being executed (multiple per session)
    HIVEQUERYID("hive.query.id", ""),

    // id of the mapred plan being executed (multiple per query)
    HIVEPLANID("hive.query.planid", ""),
        // max jobname length
    HIVEJOBNAMELENGTH("hive.jobname.length", 50),

    // hive jar
    HIVEJAR("hive.jar.path", ""),
    HIVEAUXJARS("hive.aux.jars.path", ""),

    // hive added files and jars
    HIVEADDEDFILES("hive.added.files.path", ""),
    HIVEADDEDJARS("hive.added.jars.path", ""),
    HIVEADDEDARCHIVES("hive.added.archives.path", ""),

    // for hive script operator
    HIVES_AUTO_PROGRESS_TIMEOUT("hive.auto.progress.timeout", 0),
    HIVETABLENAME("hive.table.name", ""),
    HIVEPARTITIONNAME("hive.partition.name", ""),
    HIVESCRIPTAUTOPROGRESS("hive.script.auto.progress", false),
    HIVESCRIPTIDENVVAR("hive.script.operator.id.env.var", "HIVE_SCRIPT_OPERATOR_ID"),
    HIVESCRIPTTRUNCATEENV("hive.script.operator.truncate.env", false),
    HIVEMAPREDMODE("hive.mapred.mode", "nonstrict"),
    HIVEALIAS("hive.alias", ""),
    HIVEMAPSIDEAGGREGATE("hive.map.aggr", true),
    HIVEGROUPBYSKEW("hive.groupby.skewindata", false),
    HIVE_OPTIMIZE_MULTI_GROUPBY_COMMON_DISTINCTS("hive.optimize.multigroupby.common.distincts",
        true),
    HIVEJOINEMITINTERVAL("hive.join.emit.interval", 1000),
    HIVEJOINCACHESIZE("hive.join.cache.size", 25000),

    // hive.mapjoin.bucket.cache.size has been replaced by hive.smbjoin.cache.row,
    // need to remove by hive .13. Also, do not change default (see SMB operator)
    HIVEMAPJOINBUCKETCACHESIZE("hive.mapjoin.bucket.cache.size", 100),

    HIVESMBJOINCACHEROWS("hive.smbjoin.cache.rows", 10000),
    HIVEGROUPBYMAPINTERVAL("hive.groupby.mapaggr.checkinterval", 100000),
    HIVEMAPAGGRHASHMEMORY("hive.map.aggr.hash.percentmemory", (float) 0.5),
    HIVEMAPJOINFOLLOWEDBYMAPAGGRHASHMEMORY("hive.mapjoin.followby.map.aggr.hash.percentmemory", (float) 0.3),
    HIVEMAPAGGRMEMORYTHRESHOLD("hive.map.aggr.hash.force.flush.memory.threshold", (float) 0.9),
    HIVEMAPAGGRHASHMINREDUCTION("hive.map.aggr.hash.min.reduction", (float) 0.5),
    HIVEMULTIGROUPBYSINGLEREDUCER("hive.multigroupby.singlereducer", true),
    HIVE_MAP_GROUPBY_SORT("hive.map.groupby.sorted", false),
    HIVE_MAP_GROUPBY_SORT_TESTMODE("hive.map.groupby.sorted.testmode", false),
    HIVE_GROUPBY_ORDERBY_POSITION_ALIAS("hive.groupby.orderby.position.alias", false),
    HIVE_NEW_JOB_GROUPING_SET_CARDINALITY("hive.new.job.grouping.set.cardinality", 30),

    // for hive udtf operator
    HIVEUDTFAUTOPROGRESS("hive.udtf.auto.progress", false),

    // Default file format for CREATE TABLE statement
    // Options: TextFile, SequenceFile
    HIVEDEFAULTFILEFORMAT("hive.default.fileformat", "TextFile",
        new StringsValidator("TextFile", "SequenceFile", "RCfile", "ORC")),
    HIVEQUERYRESULTFILEFORMAT("hive.query.result.fileformat", "TextFile",
        new StringsValidator("TextFile", "SequenceFile", "RCfile")),
    HIVECHECKFILEFORMAT("hive.fileformat.check", true),

    // default serde for rcfile
    HIVEDEFAULTRCFILESERDE("hive.default.rcfile.serde",
                           "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"),

    //Location of Hive run time structured log file
    HIVEHISTORYFILELOC("hive.querylog.location", System.getProperty("java.io.tmpdir") + File.separator + System.getProperty("user.name")),

    // Whether to log the plan's progress every time a job's progress is checked
    HIVE_LOG_INCREMENTAL_PLAN_PROGRESS("hive.querylog.enable.plan.progress", true),

    // The interval between logging the plan's progress in milliseconds
    HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL("hive.querylog.plan.progress.interval", 60000L),

    // Default serde and record reader for user scripts
    HIVESCRIPTSERDE("hive.script.serde", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    HIVESCRIPTRECORDREADER("hive.script.recordreader",
        "org.apache.hadoop.hive.ql.exec.TextRecordReader"),
    HIVESCRIPTRECORDWRITER("hive.script.recordwriter",
        "org.apache.hadoop.hive.ql.exec.TextRecordWriter"),
    HIVESCRIPTESCAPE("hive.transform.escape.input", false),
    HIVEBINARYRECORDMAX("hive.binary.record.max.length", 1000 ),

    // HWI
    HIVEHWILISTENHOST("hive.hwi.listen.host", "0.0.0.0"),
    HIVEHWILISTENPORT("hive.hwi.listen.port", "9999"),
    HIVEHWIWARFILE("hive.hwi.war.file", System.getenv("HWI_WAR_FILE")),

    // mapper/reducer memory in local mode
    HIVEHADOOPMAXMEM("hive.mapred.local.mem", 0),

    //small table file size
    HIVESMALLTABLESFILESIZE("hive.mapjoin.smalltable.filesize",25000000L), //25M

    // random number for split sampling
    HIVESAMPLERANDOMNUM("hive.sample.seednumber", 0),

    // test mode in hive mode
    HIVETESTMODE("hive.test.mode", false),
    HIVETESTMODEPREFIX("hive.test.mode.prefix", "test_"),
    HIVETESTMODESAMPLEFREQ("hive.test.mode.samplefreq", 32),
    HIVETESTMODENOSAMPLE("hive.test.mode.nosamplelist", ""),

    HIVEMERGEMAPFILES("hive.merge.mapfiles", true),
    HIVEMERGEMAPREDFILES("hive.merge.mapredfiles", false),
    HIVEMERGEMAPFILESSIZE("hive.merge.size.per.task", (long) (256 * 1000 * 1000)),
    HIVEMERGEMAPFILESAVGSIZE("hive.merge.smallfiles.avgsize", (long) (16 * 1000 * 1000)),
    HIVEMERGERCFILEBLOCKLEVEL("hive.merge.rcfile.block.level", true),
    HIVEMERGEINPUTFORMATBLOCKLEVEL("hive.merge.input.format.block.level",
        "org.apache.hadoop.hive.ql.io.rcfile.merge.RCFileBlockMergeInputFormat"),
    HIVEMERGECURRENTJOBHASDYNAMICPARTITIONS(
        "hive.merge.current.job.has.dynamic.partitions", false),

    HIVEUSEEXPLICITRCFILEHEADER("hive.exec.rcfile.use.explicit.header", true),
    HIVEUSERCFILESYNCCACHE("hive.exec.rcfile.use.sync.cache", true),

    // Maximum fraction of heap that can be used by ORC file writers
    HIVE_ORC_FILE_MEMORY_POOL("hive.exec.orc.memory.pool", 0.5f), // 50%
    // Define the version of the file to write
    HIVE_ORC_WRITE_FORMAT("hive.exec.orc.write.format", null),
    // Define the default ORC stripe size
    HIVE_ORC_DEFAULT_STRIPE_SIZE("hive.exec.orc.default.stripe.size",
        256L * 1024 * 1024),

    HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD("hive.exec.orc.dictionary.key.size.threshold", 0.8f),

    HIVE_ORC_INCLUDE_FILE_FOOTER_IN_SPLITS("hive.orc.splits.include.file.footer", false),
    HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE("hive.orc.cache.stripe.details.size", 10000),
    HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS("hive.orc.compute.splits.num.threads", 10),

    HIVESKEWJOIN("hive.optimize.skewjoin", false),
    HIVECONVERTJOIN("hive.auto.convert.join", true),
    HIVECONVERTJOINNOCONDITIONALTASK("hive.auto.convert.join.noconditionaltask", true),
    HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD("hive.auto.convert.join.noconditionaltask.size",
        10000000L),
    HIVESKEWJOINKEY("hive.skewjoin.key", 100000),
    HIVESKEWJOINMAPJOINNUMMAPTASK("hive.skewjoin.mapjoin.map.tasks", 10000),
    HIVESKEWJOINMAPJOINMINSPLIT("hive.skewjoin.mapjoin.min.split", 33554432L), //32M

    HIVESENDHEARTBEAT("hive.heartbeat.interval", 1000),
    HIVELIMITMAXROWSIZE("hive.limit.row.max.size", 100000L),
    HIVELIMITOPTLIMITFILE("hive.limit.optimize.limit.file", 10),
    HIVELIMITOPTENABLE("hive.limit.optimize.enable", false),
    HIVELIMITOPTMAXFETCH("hive.limit.optimize.fetch.max", 50000),
    HIVELIMITPUSHDOWNMEMORYUSAGE("hive.limit.pushdown.memory.usage", -1f),

    HIVEHASHTABLETHRESHOLD("hive.hashtable.initialCapacity", 100000),
    HIVEHASHTABLELOADFACTOR("hive.hashtable.loadfactor", (float) 0.75),
    HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE("hive.mapjoin.followby.gby.localtask.max.memory.usage", (float) 0.55),
    HIVEHASHTABLEMAXMEMORYUSAGE("hive.mapjoin.localtask.max.memory.usage", (float) 0.90),
    HIVEHASHTABLESCALE("hive.mapjoin.check.memory.rows", (long)100000),

    HIVEDEBUGLOCALTASK("hive.debug.localtask",false),

    HIVEINPUTFORMAT("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat"),

    HIVEENFORCEBUCKETING("hive.enforce.bucketing", false),
    HIVEENFORCESORTING("hive.enforce.sorting", false),
    HIVEOPTIMIZEBUCKETINGSORTING("hive.optimize.bucketingsorting", true),
    HIVEPARTITIONER("hive.mapred.partitioner", "org.apache.hadoop.hive.ql.io.DefaultHivePartitioner"),
    HIVEENFORCESORTMERGEBUCKETMAPJOIN("hive.enforce.sortmergebucketmapjoin", false),
    HIVEENFORCEBUCKETMAPJOIN("hive.enforce.bucketmapjoin", false),

    HIVE_AUTO_SORTMERGE_JOIN("hive.auto.convert.sortmerge.join", false),
    HIVE_AUTO_SORTMERGE_JOIN_BIGTABLE_SELECTOR(
        "hive.auto.convert.sortmerge.join.bigtable.selection.policy",
        "org.apache.hadoop.hive.ql.optimizer.AvgPartitionSizeBasedBigTableSelectorForAutoSMJ"),
    HIVE_AUTO_SORTMERGE_JOIN_TOMAPJOIN(
        "hive.auto.convert.sortmerge.join.to.mapjoin", false),

    HIVESCRIPTOPERATORTRUST("hive.exec.script.trust", false),
    HIVEROWOFFSET("hive.exec.rowoffset", false),

    HIVE_COMBINE_INPUT_FORMAT_SUPPORTS_SPLITTABLE("hive.hadoop.supports.splittable.combineinputformat", false),

    // Optimizer
    HIVEOPTINDEXFILTER("hive.optimize.index.filter", false), // automatically use indexes
    HIVEINDEXAUTOUPDATE("hive.optimize.index.autoupdate", false), //automatically update stale indexes
    HIVEOPTPPD("hive.optimize.ppd", true), // predicate pushdown
    HIVEPPDRECOGNIZETRANSITIVITY("hive.ppd.recognizetransivity", true), // predicate pushdown
    HIVEPPDREMOVEDUPLICATEFILTERS("hive.ppd.remove.duplicatefilters", true),
    HIVEMETADATAONLYQUERIES("hive.optimize.metadataonly", true),
    // push predicates down to storage handlers
    HIVEOPTPPD_STORAGE("hive.optimize.ppd.storage", true),
    HIVEOPTGROUPBY("hive.optimize.groupby", true), // optimize group by
    HIVEOPTBUCKETMAPJOIN("hive.optimize.bucketmapjoin", false), // optimize bucket map join
    HIVEOPTSORTMERGEBUCKETMAPJOIN("hive.optimize.bucketmapjoin.sortedmerge", false), // try to use sorted merge bucket map join
    HIVEOPTREDUCEDEDUPLICATION("hive.optimize.reducededuplication", true),
    HIVEOPTREDUCEDEDUPLICATIONMINREDUCER("hive.optimize.reducededuplication.min.reducer", 4),

    HIVESAMPLINGFORORDERBY("hive.optimize.sampling.orderby", false),
    HIVESAMPLINGNUMBERFORORDERBY("hive.optimize.sampling.orderby.number", 1000),
    HIVESAMPLINGPERCENTFORORDERBY("hive.optimize.sampling.orderby.percent", 0.1f),

    // whether to optimize union followed by select followed by filesink
    // It creates sub-directories in the final output, so should not be turned on in systems
    // where MAPREDUCE-1501 is not present
    HIVE_OPTIMIZE_UNION_REMOVE("hive.optimize.union.remove", false),
    HIVEOPTCORRELATION("hive.optimize.correlation", false), // exploit intra-query correlations

    // whether hadoop map-reduce supports sub-directories. It was added by MAPREDUCE-1501.
    // Some optimizations can only be performed if the version of hadoop being used supports
    // sub-directories
    HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES("hive.mapred.supports.subdirectories", false),

    // optimize skewed join by changing the query plan at compile time
    HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME("hive.optimize.skewjoin.compiletime", false),

    // Indexes
    HIVEOPTINDEXFILTER_COMPACT_MINSIZE("hive.optimize.index.filter.compact.minsize", (long) 5 * 1024 * 1024 * 1024), // 5G
    HIVEOPTINDEXFILTER_COMPACT_MAXSIZE("hive.optimize.index.filter.compact.maxsize", (long) -1), // infinity
    HIVE_INDEX_COMPACT_QUERY_MAX_ENTRIES("hive.index.compact.query.max.entries", (long) 10000000), // 10M
    HIVE_INDEX_COMPACT_QUERY_MAX_SIZE("hive.index.compact.query.max.size", (long) 10 * 1024 * 1024 * 1024), // 10G
    HIVE_INDEX_COMPACT_BINARY_SEARCH("hive.index.compact.binary.search", true),

    // Statistics
    HIVESTATSAUTOGATHER("hive.stats.autogather", true),
    HIVESTATSDBCLASS("hive.stats.dbclass", "counter",
        new PatternValidator("jdbc(:.*)", "hbase", "counter", "custom")), // StatsSetupConst.StatDB
    HIVESTATSJDBCDRIVER("hive.stats.jdbcdriver",
        "org.apache.derby.jdbc.EmbeddedDriver"), // JDBC driver specific to the dbclass
    HIVESTATSDBCONNECTIONSTRING("hive.stats.dbconnectionstring",
        "jdbc:derby:;databaseName=TempStatsStore;create=true"), // automatically create database
    HIVE_STATS_DEFAULT_PUBLISHER("hive.stats.default.publisher",
        ""), // default stats publisher if none of JDBC/HBase is specified
    HIVE_STATS_DEFAULT_AGGREGATOR("hive.stats.default.aggregator",
        ""), // default stats aggregator if none of JDBC/HBase is specified
    HIVE_STATS_JDBC_TIMEOUT("hive.stats.jdbc.timeout",
        30), // default timeout in sec for JDBC connection & SQL statements
    HIVE_STATS_ATOMIC("hive.stats.atomic",
        false), // whether to update metastore stats only if all stats are available
    HIVE_STATS_RETRIES_MAX("hive.stats.retries.max",
        0),     // maximum # of retries to insert/select/delete the stats DB
    HIVE_STATS_RETRIES_WAIT("hive.stats.retries.wait",
        3000),  // # milliseconds to wait before the next retry
    HIVE_STATS_COLLECT_RAWDATASIZE("hive.stats.collect.rawdatasize", true),
    // should the raw data size be collected when analyzing tables
    CLIENT_STATS_COUNTERS("hive.client.stats.counters", ""),
    //Subset of counters that should be of interest for hive.client.stats.publishers (when one wants to limit their publishing). Non-display names should be used".
    HIVE_STATS_RELIABLE("hive.stats.reliable", false),
    // Collect table access keys information for operators that can benefit from bucketing
    HIVE_STATS_COLLECT_TABLEKEYS("hive.stats.collect.tablekeys", false),
    // Collect column access information
    HIVE_STATS_COLLECT_SCANCOLS("hive.stats.collect.scancols", false),
    // standard error allowed for ndv estimates. A lower value indicates higher accuracy and a
    // higher compute cost.
    HIVE_STATS_NDV_ERROR("hive.stats.ndv.error", (float)20.0),
    HIVE_STATS_KEY_PREFIX_MAX_LENGTH("hive.stats.key.prefix.max.length", 150),
    HIVE_STATS_KEY_PREFIX("hive.stats.key.prefix", ""), // internal usage only
    // if length of variable length data type cannot be determined this length will be used.
    HIVE_STATS_MAX_VARIABLE_LENGTH("hive.stats.max.variable.length", 100),
    // if number of elements in list cannot be determined, this value will be used
    HIVE_STATS_LIST_NUM_ENTRIES("hive.stats.list.num.entries", 10),
    // if number of elements in map cannot be determined, this value will be used
    HIVE_STATS_MAP_NUM_ENTRIES("hive.stats.map.num.entries", 10),
    // to accurately compute statistics for GROUPBY map side parallelism needs to be known
    HIVE_STATS_MAP_SIDE_PARALLELISM("hive.stats.map.parallelism", 1),
    // statistics annotation fetches column statistics for all required columns and for all
    // required partitions which can be very expensive sometimes
    HIVE_STATS_FETCH_COLUMN_STATS("hive.stats.fetch.column.stats", false),
    // in the absence of table/partition stats, average row size will be used to
    // estimate the number of rows/data size
    HIVE_STATS_AVG_ROW_SIZE("hive.stats.avg.row.size", 10000),
    // in the absence of column statistics, the estimated number of rows/data size that will
    // emitted from join operator will depend on t factor
    HIVE_STATS_JOIN_FACTOR("hive.stats.join.factor", (float) 1.1),
    // in the absence of uncompressed/raw data size, total file size will be used for statistics
    // annotation. But the file may be compressed, encoded and serialized which may be lesser in size
    // than the actual uncompressed/raw data size. This factor will be multiplied to file size to estimate
    // the raw data size.
    HIVE_STATS_DESERIALIZATION_FACTOR("hive.stats.deserialization.factor", (float) 1.0),

    // Concurrency
    HIVE_SUPPORT_CONCURRENCY("hive.support.concurrency", false),
    HIVE_LOCK_MANAGER("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager"),
    HIVE_LOCK_NUMRETRIES("hive.lock.numretries", 100),
    HIVE_UNLOCK_NUMRETRIES("hive.unlock.numretries", 10),
    HIVE_LOCK_SLEEP_BETWEEN_RETRIES("hive.lock.sleep.between.retries", 60),
    HIVE_LOCK_MAPRED_ONLY("hive.lock.mapred.only.operation", false),

    HIVE_ZOOKEEPER_QUORUM("hive.zookeeper.quorum", ""),
    HIVE_ZOOKEEPER_CLIENT_PORT("hive.zookeeper.client.port", "2181"),
    HIVE_ZOOKEEPER_SESSION_TIMEOUT("hive.zookeeper.session.timeout", 600*1000),
    HIVE_ZOOKEEPER_NAMESPACE("hive.zookeeper.namespace", "hive_zookeeper_namespace"),
    HIVE_ZOOKEEPER_CLEAN_EXTRA_NODES("hive.zookeeper.clean.extra.nodes", false),

    // For HBase storage handler
    HIVE_HBASE_WAL_ENABLED("hive.hbase.wal.enabled", true),

    // For har files
    HIVEARCHIVEENABLED("hive.archive.enabled", false),

    //Enable/Disable gbToIdx rewrite rule
    HIVEOPTGBYUSINGINDEX("hive.optimize.index.groupby", false),

    HIVEOUTERJOINSUPPORTSFILTERS("hive.outerjoin.supports.filters", true),

    // 'minimal', 'more' (and 'all' later)
    HIVEFETCHTASKCONVERSION("hive.fetch.task.conversion", "minimal",
        new StringsValidator("minimal", "more")),
    HIVEFETCHTASKCONVERSIONTHRESHOLD("hive.fetch.task.conversion.threshold", -1l),

    HIVEFETCHTASKAGGR("hive.fetch.task.aggr", false),

    HIVEOPTIMIZEMETADATAQUERIES("hive.compute.query.using.stats", false),

    // Serde for FetchTask
    HIVEFETCHOUTPUTSERDE("hive.fetch.output.serde", "org.apache.hadoop.hive.serde2.DelimitedJSONSerDe"),

    HIVEEXPREVALUATIONCACHE("hive.cache.expr.evaluation", true),

    // Hive Variables
    HIVEVARIABLESUBSTITUTE("hive.variable.substitute", true),
    HIVEVARIABLESUBSTITUTEDEPTH("hive.variable.substitute.depth", 40),

    HIVECONFVALIDATION("hive.conf.validation", true),

    SEMANTIC_ANALYZER_HOOK("hive.semantic.analyzer.hook", ""),
    HIVE_AUTHORIZATION_ENABLED("hive.security.authorization.enabled", false),
    HIVE_AUTHORIZATION_MANAGER("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider"),
    HIVE_AUTHENTICATOR_MANAGER("hive.security.authenticator.manager",
        "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator"),
    HIVE_METASTORE_AUTHORIZATION_MANAGER("hive.security.metastore.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization."
        + "DefaultHiveMetastoreAuthorizationProvider"),
    HIVE_METASTORE_AUTHENTICATOR_MANAGER("hive.security.metastore.authenticator.manager",
        "org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator"),
    HIVE_AUTHORIZATION_TABLE_USER_GRANTS("hive.security.authorization.createtable.user.grants", ""),
    HIVE_AUTHORIZATION_TABLE_GROUP_GRANTS("hive.security.authorization.createtable.group.grants",
        ""),
    HIVE_AUTHORIZATION_TABLE_ROLE_GRANTS("hive.security.authorization.createtable.role.grants", ""),
    HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS("hive.security.authorization.createtable.owner.grants",
        ""),

    // Print column names in output
    HIVE_CLI_PRINT_HEADER("hive.cli.print.header", false),

    HIVE_ERROR_ON_EMPTY_PARTITION("hive.error.on.empty.partition", false),

    HIVE_INDEX_IGNORE_HDFS_LOC("hive.index.compact.file.ignore.hdfs", false),

    HIVE_EXIM_URI_SCHEME_WL("hive.exim.uri.scheme.whitelist", "hdfs,pfile"),
    // temporary variable for testing. This is added just to turn off this feature in case of a bug in
    // deployment. It has not been documented in hive-default.xml intentionally, this should be removed
    // once the feature is stable
    HIVE_MAPPER_CANNOT_SPAN_MULTIPLE_PARTITIONS("hive.mapper.cannot.span.multiple.partitions", false),
    HIVE_REWORK_MAPREDWORK("hive.rework.mapredwork", false),
    HIVE_CONCATENATE_CHECK_INDEX ("hive.exec.concatenate.check.index", true),
    HIVE_IO_EXCEPTION_HANDLERS("hive.io.exception.handlers", ""),

    // logging configuration
    HIVE_LOG4J_FILE("hive.log4j.file", ""),
    HIVE_EXEC_LOG4J_FILE("hive.exec.log4j.file", ""),

    // prefix used to auto generated column aliases (this should be started with '_')
    HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL("hive.autogen.columnalias.prefix.label", "_c"),
    HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME(
                               "hive.autogen.columnalias.prefix.includefuncname", false),

    // The class responsible for logging client side performance metrics
    // Must be a subclass of org.apache.hadoop.hive.ql.log.PerfLogger
    HIVE_PERF_LOGGER("hive.exec.perf.logger", "org.apache.hadoop.hive.ql.log.PerfLogger"),
    // Whether to delete the scratchdir while startup
    HIVE_START_CLEANUP_SCRATCHDIR("hive.start.cleanup.scratchdir", false),
    HIVE_INSERT_INTO_MULTILEVEL_DIRS("hive.insert.into.multilevel.dirs", false),
    HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS("hive.warehouse.subdir.inherit.perms", false),
    // whether insert into external tables is allowed
    HIVE_INSERT_INTO_EXTERNAL_TABLES("hive.insert.into.external.tables", true),

    // A comma separated list of hooks which implement HiveDriverRunHook and will be run at the
    // beginning and end of Driver.run, these will be run in the order specified
    HIVE_DRIVER_RUN_HOOKS("hive.exec.driver.run.hooks", ""),
    HIVE_DDL_OUTPUT_FORMAT("hive.ddl.output.format", null),
    HIVE_ENTITY_SEPARATOR("hive.entity.separator", "@"),

    HIVE_SERVER2_MAX_START_ATTEMPTS("hive.server2.max.start.attempts", 30L,
        new LongRangeValidator(0L, Long.MAX_VALUE)),

    // binary or http
    HIVE_SERVER2_TRANSPORT_MODE("hive.server2.transport.mode", "binary",
        new StringsValidator("binary", "http")),

    // http (over thrift) transport settings
    HIVE_SERVER2_THRIFT_HTTP_PORT("hive.server2.thrift.http.port", 10001),
    HIVE_SERVER2_THRIFT_HTTP_PATH("hive.server2.thrift.http.path", "cliservice"),
    HIVE_SERVER2_THRIFT_HTTP_MIN_WORKER_THREADS("hive.server2.thrift.http.min.worker.threads", 5),
    HIVE_SERVER2_THRIFT_HTTP_MAX_WORKER_THREADS("hive.server2.thrift.http.max.worker.threads", 500),

    // binary transport settings
    HIVE_SERVER2_THRIFT_PORT("hive.server2.thrift.port", 10000),
    HIVE_SERVER2_THRIFT_BIND_HOST("hive.server2.thrift.bind.host", ""),
    HIVE_SERVER2_THRIFT_SASL_QOP("hive.server2.thrift.sasl.qop", "auth",
        new StringsValidator("auth", "auth-int", "auth-conf")),
    HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS("hive.server2.thrift.min.worker.threads", 5),
    HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS("hive.server2.thrift.max.worker.threads", 500),

    // Configuration for async thread pool in SessionManager
    // Number of async threads
    HIVE_SERVER2_ASYNC_EXEC_THREADS("hive.server2.async.exec.threads", 100),
    // Number of seconds HiveServer2 shutdown will wait for async threads to terminate
    HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT("hive.server2.async.exec.shutdown.timeout", 10),
    // Size of the wait queue for async thread pool in HiveServer2.
    // After hitting this limit, the async thread pool will reject new requests.
    HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE("hive.server2.async.exec.wait.queue.size", 100),
    // Number of seconds that an idle HiveServer2 async thread (from the thread pool)
    // will wait for a new task to arrive before terminating
    HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME("hive.server2.async.exec.keepalive.time", 10),


    // HiveServer2 auth configuration
    HIVE_SERVER2_AUTHENTICATION("hive.server2.authentication", "NONE",
        new StringsValidator("NOSASL", "NONE", "LDAP", "KERBEROS", "CUSTOM")),
    HIVE_SERVER2_KERBEROS_KEYTAB("hive.server2.authentication.kerberos.keytab", ""),
    HIVE_SERVER2_KERBEROS_PRINCIPAL("hive.server2.authentication.kerberos.principal", ""),
    HIVE_SERVER2_PLAIN_LDAP_URL("hive.server2.authentication.ldap.url", null),
    HIVE_SERVER2_PLAIN_LDAP_BASEDN("hive.server2.authentication.ldap.baseDN", null),
    HIVE_SERVER2_PLAIN_LDAP_DOMAIN("hive.server2.authentication.ldap.Domain", null),
    HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS("hive.server2.custom.authentication.class", null),
    HIVE_SERVER2_ENABLE_DOAS("hive.server2.enable.doAs", true),
    HIVE_SERVER2_TABLE_TYPE_MAPPING("hive.server2.table.type.mapping", "CLASSIC",
        new StringsValidator("CLASSIC", "HIVE")),
    HIVE_SERVER2_SESSION_HOOK("hive.server2.session.hook", ""),
    HIVE_SERVER2_USE_SSL("hive.server2.use.SSL", false),
    HIVE_SERVER2_SSL_KEYSTORE_PATH("hive.server2.keystore.path", ""),
    HIVE_SERVER2_SSL_KEYSTORE_PASSWORD("hive.server2.keystore.password", ""),

    HIVE_SECURITY_COMMAND_WHITELIST("hive.security.command.whitelist", "set,reset,dfs,add,delete,compile"),

    HIVE_CONF_RESTRICTED_LIST("hive.conf.restricted.list", ""),

    // If this is set all move tasks at the end of a multi-insert query will only begin once all
    // outputs are ready
    HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES(
        "hive.multi.insert.move.tasks.share.dependencies", false),

    // If this is set, when writing partitions, the metadata will include the bucketing/sorting
    // properties with which the data was written if any (this will not overwrite the metadata
    // inherited from the table if the table is bucketed/sorted)
    HIVE_INFER_BUCKET_SORT("hive.exec.infer.bucket.sort", false),
    // If this is set, when setting the number of reducers for the map reduce task which writes the
    // final output files, it will choose a number which is a power of two.  The number of reducers
    // may be set to a power of two, only to be followed by a merge task meaning preventing
    // anything from being inferred.
    HIVE_INFER_BUCKET_SORT_NUM_BUCKETS_POWER_TWO(
        "hive.exec.infer.bucket.sort.num.buckets.power.two", false),

    /* The following section contains all configurations used for list bucketing feature.*/
    /* This is not for clients. but only for block merge task. */
    /* This is used by BlockMergeTask to send out flag to RCFileMergeMapper */
    /* about alter table...concatenate and list bucketing case. */
    HIVEMERGECURRENTJOBCONCATENATELISTBUCKETING(
        "hive.merge.current.job.concatenate.list.bucketing", true),
    /* This is not for clients. but only for block merge task. */
    /* This is used by BlockMergeTask to send out flag to RCFileMergeMapper */
    /* about depth of list bucketing. */
    HIVEMERGECURRENTJOBCONCATENATELISTBUCKETINGDEPTH(
            "hive.merge.current.job.concatenate.list.bucketing.depth", 0),
    // Enable list bucketing optimizer. Default value is false so that we disable it by default.
    HIVEOPTLISTBUCKETING("hive.optimize.listbucketing", false),

    // Allow TCP Keep alive socket option for for HiveServer or a maximum timeout for the socket.

    SERVER_READ_SOCKET_TIMEOUT("hive.server.read.socket.timeout", 10),
    SERVER_TCP_KEEP_ALIVE("hive.server.tcp.keepalive", true),

    // Whether to show the unquoted partition names in query results.
    HIVE_DECODE_PARTITION_NAME("hive.decode.partition.name", false),

    HIVE_EXECUTION_ENGINE("hive.execution.engine", "mr",
        new StringsValidator("mr", "tez")),
    HIVE_JAR_DIRECTORY("hive.jar.directory", "hdfs:///user/hive/"),
    HIVE_USER_INSTALL_DIR("hive.user.install.directory", "hdfs:///user/"),

    // Vectorization enabled
    HIVE_VECTORIZATION_ENABLED("hive.vectorized.execution.enabled", false),
    HIVE_VECTORIZATION_GROUPBY_CHECKINTERVAL("hive.vectorized.groupby.checkinterval", 100000),
    HIVE_VECTORIZATION_GROUPBY_MAXENTRIES("hive.vectorized.groupby.maxentries", 1000000),
    HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT("hive.vectorized.groupby.flush.percent", (float) 0.1),
    

    HIVE_TYPE_CHECK_ON_INSERT("hive.typecheck.on.insert", true),

    // Whether to send the query plan via local resource or RPC
    HIVE_RPC_QUERY_PLAN("hive.rpc.query.plan", false),

    // Whether to generate the splits locally or in the AM (tez only)
    HIVE_AM_SPLIT_GENERATION("hive.compute.splits.in.am", true),

    // none, idonly, traverse, execution
    HIVESTAGEIDREARRANGE("hive.stageid.rearrange", "none"),
    HIVEEXPLAINDEPENDENCYAPPENDTASKTYPES("hive.explain.dependency.append.tasktype", false),

    HIVECOUNTERGROUP("hive.counters.group.name", "HIVE"),
    
    // none, column
    // none is the default(past) behavior. Implies only alphaNumeric and underscore are valid characters in identifiers.
    // column: implies column names can contain any character.
    HIVE_QUOTEDID_SUPPORT("hive.support.quoted.identifiers", "column",
        new PatternValidator("none", "column"))
    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;

    private final VarType type;

    private final Validator validator;

    ConfVars(String varname, String defaultVal) {
      this(varname, defaultVal, null);
    }

    ConfVars(String varname, String defaultVal, Validator validator) {
      this.varname = varname;
      this.valClass = String.class;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
      this.type = VarType.STRING;
      this.validator = validator;
    }

    ConfVars(String varname, int defaultVal) {
      this(varname, defaultVal, null);
    }

    ConfVars(String varname, int defaultIntVal, Validator validator) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultVal = Integer.toString(defaultIntVal);
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
      this.type = VarType.INT;
      this.validator = validator;
    }

    ConfVars(String varname, long defaultVal) {
      this(varname, defaultVal, null);
    }

    ConfVars(String varname, long defaultLongVal, Validator validator) {
      this.varname = varname;
      this.valClass = Long.class;
      this.defaultVal = Long.toString(defaultLongVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
      this.type = VarType.LONG;
      this.validator = validator;
    }

    ConfVars(String varname, float defaultVal) {
      this(varname, defaultVal, null);
    }

    ConfVars(String varname, float defaultFloatVal, Validator validator) {
      this.varname = varname;
      this.valClass = Float.class;
      this.defaultVal = Float.toString(defaultFloatVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
      this.type = VarType.FLOAT;
      this.validator = validator;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultVal = Boolean.toString(defaultBoolVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
      this.type = VarType.BOOLEAN;
      this.validator = null;
    }

    public boolean isType(String value) {
      return type.isType(value);
    }

    public String validate(String value) {
      return validator == null ? null : validator.validate(value);
    }

    public String typeString() {
      return type.typeString();
    }

    @Override
    public String toString() {
      return varname;
    }

    private static String findHadoopBinary() {
      String val = System.getenv("HADOOP_HOME");
      // In Hadoop 1.X and Hadoop 2.X HADOOP_HOME is gone and replaced with HADOOP_PREFIX
      if (val == null) {
        val = System.getenv("HADOOP_PREFIX");
      }
      // and if all else fails we can at least try /usr/bin/hadoop
      val = (val == null ? File.separator + "usr" : val)
        + File.separator + "bin" + File.separator + "hadoop";
      // Launch hadoop command file on windows.
      return val + (Shell.WINDOWS ? ".cmd" : "");
    }

    enum VarType {
      STRING { @Override
      void checkType(String value) throws Exception { } },
      INT { @Override
      void checkType(String value) throws Exception { Integer.valueOf(value); } },
      LONG { @Override
      void checkType(String value) throws Exception { Long.valueOf(value); } },
      FLOAT { @Override
      void checkType(String value) throws Exception { Float.valueOf(value); } },
      BOOLEAN { @Override
      void checkType(String value) throws Exception { Boolean.valueOf(value); } };

      boolean isType(String value) {
        try { checkType(value); } catch (Exception e) { return false; }
        return true;
      }
      String typeString() { return name().toUpperCase();}
      abstract void checkType(String value) throws Exception;
    }
  }

  /**
   * Writes the default ConfVars out to a byte array and returns an input
   * stream wrapping that byte array.
   *
   * We need this in order to initialize the ConfVar properties
   * in the underling Configuration object using the addResource(InputStream)
   * method.
   *
   * It is important to use a LoopingByteArrayInputStream because it turns out
   * addResource(InputStream) is broken since Configuration tries to read the
   * entire contents of the same InputStream repeatedly without resetting it.
   * LoopingByteArrayInputStream has special logic to handle this.
   */
  private static synchronized InputStream getConfVarInputStream() {
    if (confVarByteArray == null) {
      try {
        // Create a Hadoop configuration without inheriting default settings.
        Configuration conf = new Configuration(false);

        applyDefaultNonNullConfVars(conf);

        ByteArrayOutputStream confVarBaos = new ByteArrayOutputStream();
        conf.writeXml(confVarBaos);
        confVarByteArray = confVarBaos.toByteArray();
      } catch (Exception e) {
        // We're pretty screwed if we can't load the default conf vars
        throw new RuntimeException("Failed to initialize default Hive configuration variables!", e);
      }
    }
    return new LoopingByteArrayInputStream(confVarByteArray);
  }

  public void verifyAndSet(String name, String value) throws IllegalArgumentException {
    if (restrictList.contains(name)) {
      throw new IllegalArgumentException("Cannot modify " + name + " at runtime");
    }
    set(name, value);
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Integer.class) : var.varname;
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    assert (var.valClass == Integer.class) : var.varname;
    conf.setInt(var.varname, val);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public void setIntVar(ConfVars var, int val) {
    setIntVar(this, var, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Long.class) : var.varname;
    return conf.getLong(var.varname, var.defaultLongVal);
  }

  public static long getLongVar(Configuration conf, ConfVars var, long defaultVal) {
    return conf.getLong(var.varname, defaultVal);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    assert (var.valClass == Long.class) : var.varname;
    conf.setLong(var.varname, val);
  }

  public long getLongVar(ConfVars var) {
    return getLongVar(this, var);
  }

  public void setLongVar(ConfVars var, long val) {
    setLongVar(this, var, val);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Float.class) : var.varname;
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public static float getFloatVar(Configuration conf, ConfVars var, float defaultVal) {
    return conf.getFloat(var.varname, defaultVal);
  }

  public static void setFloatVar(Configuration conf, ConfVars var, float val) {
    assert (var.valClass == Float.class) : var.varname;
    conf.setFloat(var.varname, val);
  }

  public float getFloatVar(ConfVars var) {
    return getFloatVar(this, var);
  }

  public void setFloatVar(ConfVars var, float val) {
    setFloatVar(this, var, val);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Boolean.class) : var.varname;
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var, boolean defaultVal) {
    return conf.getBoolean(var.varname, defaultVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    assert (var.valClass == Boolean.class) : var.varname;
    conf.setBoolean(var.varname, val);
  }

  public boolean getBoolVar(ConfVars var) {
    return getBoolVar(this, var);
  }

  public void setBoolVar(ConfVars var, boolean val) {
    setBoolVar(this, var, val);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    assert (var.valClass == String.class) : var.varname;
    return conf.get(var.varname, var.defaultVal);
  }

  public static String getVar(Configuration conf, ConfVars var, String defaultVal) {
    return conf.get(var.varname, defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert (var.valClass == String.class) : var.varname;
    conf.set(var.varname, val);
  }

  public static ConfVars getConfVars(String name) {
    return vars.get(name);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public void logVars(PrintStream ps) {
    for (ConfVars one : ConfVars.values()) {
      ps.println(one.varname + "=" + ((get(one.varname) != null) ? get(one.varname) : ""));
    }
  }

  public HiveConf() {
    super();
    initialize(this.getClass());
  }

  public HiveConf(Class<?> cls) {
    super();
    initialize(cls);
  }

  public HiveConf(Configuration other, Class<?> cls) {
    super(other);
    initialize(cls);
  }

  /**
   * Copy constructor
   */
  public HiveConf(HiveConf other) {
    super(other);
    hiveJar = other.hiveJar;
    auxJars = other.auxJars;
    origProp = (Properties)other.origProp.clone();
    restrictList.addAll(other.restrictList);
  }

  public Properties getAllProperties() {
    return getProperties(this);
  }

  private static Properties getProperties(Configuration conf) {
    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    Properties p = new Properties();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      p.setProperty(e.getKey(), e.getValue());
    }
    return p;
  }

  private void initialize(Class<?> cls) {
    hiveJar = (new JobConf(cls)).getJar();

    // preserve the original configuration
    origProp = getAllProperties();

    // Overlay the ConfVars. Note that this ignores ConfVars with null values
    addResource(getConfVarInputStream());

    // Overlay hive-site.xml if it exists
    if (hiveSiteURL != null) {
      addResource(hiveSiteURL);
    }

    // Overlay the values of any system properties whose names appear in the list of ConfVars
    applySystemProperties();

    if(this.get("hive.metastore.local", null) != null) {
      l4j.warn("DEPRECATED: Configuration property hive.metastore.local no longer has any " +
      		"effect. Make sure to provide a valid value for hive.metastore.uris if you are " +
      		"connecting to a remote metastore.");
    }

    // if the running class was loaded directly (through eclipse) rather than through a
    // jar then this would be needed
    if (hiveJar == null) {
      hiveJar = this.get(ConfVars.HIVEJAR.varname);
    }

    if (auxJars == null) {
      auxJars = this.get(ConfVars.HIVEAUXJARS.varname);
    }

    if (getBoolVar(ConfVars.METASTORE_SCHEMA_VERIFICATION)) {
      setBoolVar(ConfVars.METASTORE_AUTO_CREATE_SCHEMA, false);
      setBoolVar(ConfVars.METASTORE_FIXED_DATASTORE, true);
    }

    // setup list of conf vars that are not allowed to change runtime
    setupRestrictList();
  }


  /**
   * Apply system properties to this object if the property name is defined in ConfVars
   * and the value is non-null and not an empty string.
   */
  private void applySystemProperties() {
    Map<String, String> systemProperties = getConfSystemProperties();
    for (Entry<String, String> systemProperty : systemProperties.entrySet()) {
      this.set(systemProperty.getKey(), systemProperty.getValue());
    }
  }

  /**
   * This method returns a mapping from config variable name to its value for all config variables
   * which have been set using System properties
   */
  public static Map<String, String> getConfSystemProperties() {
    Map<String, String> systemProperties = new HashMap<String, String>();

    for (ConfVars oneVar : ConfVars.values()) {
      if (System.getProperty(oneVar.varname) != null) {
        if (System.getProperty(oneVar.varname).length() > 0) {
          systemProperties.put(oneVar.varname, System.getProperty(oneVar.varname));
        }
      }
    }

    return systemProperties;
  }

  /**
   * Overlays ConfVar properties with non-null values
   */
  private static void applyDefaultNonNullConfVars(Configuration conf) {
    for (ConfVars var : ConfVars.values()) {
      if (var.defaultVal == null) {
        // Don't override ConfVars with null values
        continue;
      }
      conf.set(var.varname, var.defaultVal);
    }
  }

  public Properties getChangedProperties() {
    Properties ret = new Properties();
    Properties newProp = getAllProperties();

    for (Object one : newProp.keySet()) {
      String oneProp = (String) one;
      String oldValue = origProp.getProperty(oneProp);
      if (!StringUtils.equals(oldValue, newProp.getProperty(oneProp))) {
        ret.setProperty(oneProp, newProp.getProperty(oneProp));
      }
    }
    return (ret);
  }

  public String getJar() {
    return hiveJar;
  }

  /**
   * @return the auxJars
   */
  public String getAuxJars() {
    return auxJars;
  }

  /**
   * @param auxJars the auxJars to set
   */
  public void setAuxJars(String auxJars) {
    this.auxJars = auxJars;
    setVar(this, ConfVars.HIVEAUXJARS, auxJars);
  }

  public URL getHiveDefaultLocation() {
    return hiveDefaultURL;
  }

  public static void setHiveSiteLocation(URL location) {
    hiveSiteURL = location;
  }

  public static URL getHiveSiteLocation() {
    return hiveSiteURL;
  }

  /**
   * @return the user name set in hadoop.job.ugi param or the current user from System
   * @throws IOException
   */
  public String getUser() throws IOException {
    try {
      UserGroupInformation ugi = ShimLoader.getHadoopShims()
        .getUGIForConf(this);
      return ugi.getUserName();
    } catch (LoginException le) {
      throw new IOException(le);
    }
  }

  public static String getColumnInternalName(int pos) {
    return "_col" + pos;
  }

  public static int getPositionFromInternalName(String internalName) {
    Pattern internalPattern = Pattern.compile("_col([0-9]+)");
    Matcher m = internalPattern.matcher(internalName);
    if (!m.matches()){
      return -1;
    } else {
      return Integer.parseInt(m.group(1));
    }
  }

  /**
   * validate value for a ConfVar, return non-null string for fail message
   */
  public static interface Validator {
    String validate(String value);
  }

  public static class StringsValidator implements Validator {
    private final Set<String> expected = new LinkedHashSet<String>();
    private StringsValidator(String... values) {
      for (String value : values) {
        expected.add(value.toLowerCase());
      }
    }
    @Override
    public String validate(String value) {
      if (value == null || !expected.contains(value.toLowerCase())) {
        return "Invalid value.. expects one of " + expected;
      }
      return null;
    }
  }

  public static class LongRangeValidator implements Validator {
    private final long lower, upper;

    public LongRangeValidator(long lower, long upper) {
      this.lower = lower;
      this.upper = upper;
    }

    @Override
    public String validate(String value) {
      try {
        if(value == null) {
          return "Value cannot be null";
        }
        value = value.trim();
        long lvalue = Long.parseLong(value);
        if (lvalue < lower || lvalue > upper) {
          return "Invalid value  " + value + ", which should be in between " + lower + " and " + upper;
        }
      } catch (NumberFormatException e) {
        return e.toString();
      }
      return null;
    }
  }

  public static class PatternValidator implements Validator {
    private final List<Pattern> expected = new ArrayList<Pattern>();
    private PatternValidator(String... values) {
      for (String value : values) {
        expected.add(Pattern.compile(value));
      }
    }
    @Override
    public String validate(String value) {
      if (value == null) {
        return "Invalid value.. expects one of patterns " + expected;
      }
      for (Pattern pattern : expected) {
        if (pattern.matcher(value).matches()) {
          return null;
        }
      }
      return "Invalid value.. expects one of patterns " + expected;
    }
  }

  public static class RatioValidator implements Validator {
    @Override
    public String validate(String value) {
      try {
        float fvalue = Float.valueOf(value);
        if (fvalue <= 0 || fvalue >= 1) {
          return "Invalid ratio " + value + ", which should be in between 0 to 1";
        }
      } catch (NumberFormatException e) {
        return e.toString();
      }
      return null;
    }
  }

  /**
   * Append comma separated list of config vars to the restrict List
   * @param restrictListStr
   */
  public void addToRestrictList(String restrictListStr) {
    if (restrictListStr == null) {
      return;
    }
    String oldList = this.getVar(ConfVars.HIVE_CONF_RESTRICTED_LIST);
    if (oldList == null || oldList.isEmpty()) {
      this.setVar(ConfVars.HIVE_CONF_RESTRICTED_LIST, restrictListStr);
    } else {
      this.setVar(ConfVars.HIVE_CONF_RESTRICTED_LIST, oldList + "," + restrictListStr);
    }
    setupRestrictList();
  }

  /**
   * Add the HIVE_CONF_RESTRICTED_LIST values to restrictList,
   * including HIVE_CONF_RESTRICTED_LIST itself
   */
  private void setupRestrictList() {
    String restrictListStr = this.getVar(ConfVars.HIVE_CONF_RESTRICTED_LIST);
    restrictList.clear();
    if (restrictListStr != null) {
      for (String entry : restrictListStr.split(",")) {
        restrictList.add(entry.trim());
      }
    }
    restrictList.add(ConfVars.HIVE_CONF_RESTRICTED_LIST.toString());
  }
}
