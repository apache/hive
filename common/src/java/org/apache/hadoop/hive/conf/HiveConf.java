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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.conf.Validator.PatternSet;
import org.apache.hadoop.hive.conf.Validator.RangeValidator;
import org.apache.hadoop.hive.conf.Validator.RatioValidator;
import org.apache.hadoop.hive.conf.Validator.StringSet;
import org.apache.hadoop.hive.conf.Validator.TimeValidator;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hive.common.HiveCompat;

import com.google.common.base.Joiner;

/**
 * Hive Configuration.
 */
public class HiveConf extends Configuration {
  protected String hiveJar;
  protected Properties origProp;
  protected String auxJars;
  private static final Log l4j = LogFactory.getLog(HiveConf.class);
  private static boolean loadMetastoreConfig = false;
  private static boolean loadHiveServer2Config = false;
  private static URL hiveDefaultURL = null;
  private static URL hiveSiteURL = null;
  private static URL hivemetastoreSiteUrl = null;
  private static URL hiveServer2SiteUrl = null;

  private static byte[] confVarByteArray = null;


  private static final Map<String, ConfVars> vars = new HashMap<String, ConfVars>();
  private static final Map<String, ConfVars> metaConfs = new HashMap<String, ConfVars>();
  private final List<String> restrictList = new ArrayList<String>();

  private Pattern modWhiteListPattern = null;
  private volatile boolean isSparkConfigUpdated = false;

  public boolean getSparkConfigUpdated() {
    return isSparkConfigUpdated;
  }

  public void setSparkConfigUpdated(boolean isSparkConfigUpdated) {
    this.isSparkConfigUpdated = isSparkConfigUpdated;
  }

  static {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = HiveConf.class.getClassLoader();
    }

    hiveDefaultURL = classLoader.getResource("hive-default.xml");

    // Look for hive-site.xml on the CLASSPATH and log its location if found.
    hiveSiteURL = classLoader.getResource("hive-site.xml");
    hivemetastoreSiteUrl = classLoader.getResource("hivemetastore-site.xml");
    hiveServer2SiteUrl = classLoader.getResource("hiveserver2-site.xml");

    for (ConfVars confVar : ConfVars.values()) {
      vars.put(confVar.varname, confVar);
    }
    Configuration.addDeprecation("hive.server2.enable.impersonation", "hive.server2.enable.doAs");
    Configuration.addDeprecation("hive.server2.enable.SSL", "hive.server2.use.SSL");
  }

  /**
   * Metastore related options that the db is initialized against. When a conf
   * var in this is list is changed, the metastore instance for the CLI will
   * be recreated so that the change will take effect.
   */
  public static final HiveConf.ConfVars[] metaVars = {
      HiveConf.ConfVars.METASTOREWAREHOUSE,
      HiveConf.ConfVars.METASTOREURIS,
      HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES,
      HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES,
      HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY,
      HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,
      HiveConf.ConfVars.METASTOREPWD,
      HiveConf.ConfVars.METASTORECONNECTURLHOOK,
      HiveConf.ConfVars.METASTORECONNECTURLKEY,
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
      HiveConf.ConfVars.METASTORE_ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS,
      HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES,
      HiveConf.ConfVars.USERS_IN_ADMIN_ROLE,
      HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
      HiveConf.ConfVars.HIVE_TXN_MANAGER,
      HiveConf.ConfVars.HIVE_TXN_TIMEOUT,
      HiveConf.ConfVars.HIVE_TXN_MAX_OPEN_BATCH,
      };

  /**
   * User configurable Metastore vars
   */
  public static final HiveConf.ConfVars[] metaConfVars = {
      HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL,
      HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL_DDL
  };

  static {
    for (ConfVars confVar : metaConfVars) {
      metaConfs.put(confVar.varname, confVar);
    }
  }

  /**
   * dbVars are the parameters can be set per database. If these
   * parameters are set as a database property, when switching to that
   * database, the HiveConf variable will be changed. The change of these
   * parameters will effectively change the DFS and MapReduce clusters
   * for different databases.
   */
  public static final HiveConf.ConfVars[] dbVars = {
    HiveConf.ConfVars.HADOOPBIN,
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
    SCRIPTWRAPPER("hive.exec.script.wrapper", null, ""),
    PLAN("hive.exec.plan", "", ""),
    PLAN_SERIALIZATION("hive.plan.serialization.format", "kryo",
        "Query plan format serialization between client and task nodes. \n" +
        "Two supported values are : kryo and javaXML. Kryo is default."),
    STAGINGDIR("hive.exec.stagingdir", ".hive-staging",
        "Directory name that will be created inside table locations in order to support HDFS encryption. " +
        "This is replaces ${hive.exec.scratchdir} for query results with the exception of read-only tables. " +
        "In all cases ${hive.exec.scratchdir} is still used for other temporary files, such as job plans."),
    SCRATCHDIR("hive.exec.scratchdir", "/tmp/hive",
        "HDFS root scratch dir for Hive jobs which gets created with write all (733) permission. " +
        "For each connecting user, an HDFS scratch dir: ${hive.exec.scratchdir}/<username> is created, " +
        "with ${hive.scratch.dir.permission}."),
    LOCALSCRATCHDIR("hive.exec.local.scratchdir",
        "${system:java.io.tmpdir}" + File.separator + "${system:user.name}",
        "Local scratch space for Hive jobs"),
    DOWNLOADED_RESOURCES_DIR("hive.downloaded.resources.dir",
        "${system:java.io.tmpdir}" + File.separator + "${hive.session.id}_resources",
        "Temporary local directory for added resources in the remote file system."),
    SCRATCHDIRPERMISSION("hive.scratch.dir.permission", "700",
        "The permission for the user specific scratch directories that get created."),
    SUBMITVIACHILD("hive.exec.submitviachild", false, ""),
    SUBMITLOCALTASKVIACHILD("hive.exec.submit.local.task.via.child", true,
        "Determines whether local tasks (typically mapjoin hashtable generation phase) runs in \n" +
        "separate JVM (true recommended) or not. \n" +
        "Avoids the overhead of spawning new JVM, but can lead to out-of-memory issues."),
    SCRIPTERRORLIMIT("hive.exec.script.maxerrsize", 100000,
        "Maximum number of bytes a script is allowed to emit to standard error (per map-reduce task). \n" +
        "This prevents runaway scripts from filling logs partitions to capacity"),
    ALLOWPARTIALCONSUMP("hive.exec.script.allow.partial.consumption", false,
        "When enabled, this option allows a user script to exit successfully without consuming \n" +
        "all the data from the standard input."),
    STREAMREPORTERPERFIX("stream.stderr.reporter.prefix", "reporter:",
        "Streaming jobs that log to standard error with this prefix can log counter or status information."),
    STREAMREPORTERENABLED("stream.stderr.reporter.enabled", true,
        "Enable consumption of status and counter messages for streaming jobs."),
    COMPRESSRESULT("hive.exec.compress.output", false,
        "This controls whether the final outputs of a query (to a local/HDFS file or a Hive table) is compressed. \n" +
        "The compression codec and other options are determined from Hadoop config variables mapred.output.compress*"),
    COMPRESSINTERMEDIATE("hive.exec.compress.intermediate", false,
        "This controls whether intermediate files produced by Hive between multiple map-reduce jobs are compressed. \n" +
        "The compression codec and other options are determined from Hadoop config variables mapred.output.compress*"),
    COMPRESSINTERMEDIATECODEC("hive.intermediate.compression.codec", "", ""),
    COMPRESSINTERMEDIATETYPE("hive.intermediate.compression.type", "", ""),
    BYTESPERREDUCER("hive.exec.reducers.bytes.per.reducer", (long) (256 * 1000 * 1000),
        "size per reducer.The default is 256Mb, i.e if the input size is 1G, it will use 4 reducers."),
    MAXREDUCERS("hive.exec.reducers.max", 1009,
        "max number of reducers will be used. If the one specified in the configuration parameter mapred.reduce.tasks is\n" +
        "negative, Hive will use this one as the max number of reducers when automatically determine number of reducers."),
    PREEXECHOOKS("hive.exec.pre.hooks", "",
        "Comma-separated list of pre-execution hooks to be invoked for each statement. \n" +
        "A pre-execution hook is specified as the name of a Java class which implements the \n" +
        "org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext interface."),
    POSTEXECHOOKS("hive.exec.post.hooks", "",
        "Comma-separated list of post-execution hooks to be invoked for each statement. \n" +
        "A post-execution hook is specified as the name of a Java class which implements the \n" +
        "org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext interface."),
    ONFAILUREHOOKS("hive.exec.failure.hooks", "",
        "Comma-separated list of on-failure hooks to be invoked for each statement. \n" +
        "An on-failure hook is specified as the name of Java class which implements the \n" +
        "org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext interface."),
    QUERYREDACTORHOOKS("hive.exec.query.redactor.hooks", "",
        "Comma-separated list of hooks to be invoked for each query which can \n" +
        "tranform the query before it's placed in the job.xml file. Must be a Java class which \n" +
        "extends from the org.apache.hadoop.hive.ql.hooks.Redactor abstract class."),
    CLIENTSTATSPUBLISHERS("hive.client.stats.publishers", "",
        "Comma-separated list of statistics publishers to be invoked on counters on each job. \n" +
        "A client stats publisher is specified as the name of a Java class which implements the \n" +
        "org.apache.hadoop.hive.ql.stats.ClientStatsPublisher interface."),
    EXECPARALLEL("hive.exec.parallel", false, "Whether to execute jobs in parallel"),
    EXECPARALLETHREADNUMBER("hive.exec.parallel.thread.number", 8,
        "How many jobs at most can be executed in parallel"),
    HIVESPECULATIVEEXECREDUCERS("hive.mapred.reduce.tasks.speculative.execution", true,
        "Whether speculative execution for reducers should be turned on. "),
    HIVECOUNTERSPULLINTERVAL("hive.exec.counters.pull.interval", 1000L,
        "The interval with which to poll the JobTracker for the counters the running job. \n" +
        "The smaller it is the more load there will be on the jobtracker, the higher it is the less granular the caught will be."),
    DYNAMICPARTITIONING("hive.exec.dynamic.partition", true,
        "Whether or not to allow dynamic partitions in DML/DDL."),
    DYNAMICPARTITIONINGMODE("hive.exec.dynamic.partition.mode", "strict",
        "In strict mode, the user must specify at least one static partition\n" +
        "in case the user accidentally overwrites all partitions.\n" +
        "In nonstrict mode all partitions are allowed to be dynamic."),
    DYNAMICPARTITIONMAXPARTS("hive.exec.max.dynamic.partitions", 1000,
        "Maximum number of dynamic partitions allowed to be created in total."),
    DYNAMICPARTITIONMAXPARTSPERNODE("hive.exec.max.dynamic.partitions.pernode", 100,
        "Maximum number of dynamic partitions allowed to be created in each mapper/reducer node."),
    MAXCREATEDFILES("hive.exec.max.created.files", 100000L,
        "Maximum number of HDFS files created by all mappers/reducers in a MapReduce job."),
    DEFAULTPARTITIONNAME("hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__",
        "The default partition name in case the dynamic partition column value is null/empty string or any other values that cannot be escaped. \n" +
        "This value must not contain any special character used in HDFS URI (e.g., ':', '%', '/' etc). \n" +
        "The user has to be aware that the dynamic partition value should not contain this value to avoid confusions."),
    DEFAULT_ZOOKEEPER_PARTITION_NAME("hive.lockmgr.zookeeper.default.partition.name", "__HIVE_DEFAULT_ZOOKEEPER_PARTITION__", ""),

    // Whether to show a link to the most failed task + debugging tips
    SHOW_JOB_FAIL_DEBUG_INFO("hive.exec.show.job.failure.debug.info", true,
        "If a job fails, whether to provide a link in the CLI to the task with the\n" +
        "most failures, along with debugging hints if applicable."),
    JOB_DEBUG_CAPTURE_STACKTRACES("hive.exec.job.debug.capture.stacktraces", true,
        "Whether or not stack traces parsed from the task logs of a sampled failed task \n" +
        "for each failed job should be stored in the SessionState"),
    JOB_DEBUG_TIMEOUT("hive.exec.job.debug.timeout", 30000, ""),
    TASKLOG_DEBUG_TIMEOUT("hive.exec.tasklog.debug.timeout", 20000, ""),
    OUTPUT_FILE_EXTENSION("hive.output.file.extension", null,
        "String used as a file extension for output files. \n" +
        "If not set, defaults to the codec extension for text files (e.g. \".gz\"), or no extension otherwise."),

    HIVE_IN_TEST("hive.in.test", false, "internal usage only, true in test mode", true),

    HIVE_IN_TEZ_TEST("hive.in.tez.test", false, "internal use only, true when in testing tez",
        true),

    LOCALMODEAUTO("hive.exec.mode.local.auto", false,
        "Let Hive determine whether to run in local mode automatically"),
    LOCALMODEMAXBYTES("hive.exec.mode.local.auto.inputbytes.max", 134217728L,
        "When hive.exec.mode.local.auto is true, input bytes should less than this for local mode."),
    LOCALMODEMAXINPUTFILES("hive.exec.mode.local.auto.input.files.max", 4,
        "When hive.exec.mode.local.auto is true, the number of tasks should less than this for local mode."),

    DROPIGNORESNONEXISTENT("hive.exec.drop.ignorenonexistent", true,
        "Do not report an error if DROP TABLE/VIEW/Index specifies a non-existent table/view/index"),

    HIVEIGNOREMAPJOINHINT("hive.ignore.mapjoin.hint", true, "Ignore the mapjoin hint"),

    HIVE_FILE_MAX_FOOTER("hive.file.max.footer", 100,
        "maximum number of lines for footer user can define for a table file"),

    HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES("hive.resultset.use.unique.column.names", true,
        "Make column names unique in the result set by qualifying column names with table alias if needed.\n" +
        "Table alias will be added to column names for queries of type \"select *\" or \n" +
        "if query explicitly uses table alias \"select r1.x..\"."),

    // Hadoop Configuration Properties
    // Properties with null values are ignored and exist only for the purpose of giving us
    // a symbolic name to reference in the Hive source code. Properties with non-null
    // values will override any values set in the underlying Hadoop configuration.
    HADOOPBIN("hadoop.bin.path", findHadoopBinary(), "", true),
    HIVE_FS_HAR_IMPL("fs.har.impl", "org.apache.hadoop.hive.shims.HiveHarFileSystem",
        "The implementation for accessing Hadoop Archives. Note that this won't be applicable to Hadoop versions less than 0.20"),
    HADOOPFS(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPFS"), null, "", true),
    HADOOPMAPFILENAME(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPMAPFILENAME"), null, "", true),
    HADOOPMAPREDINPUTDIR(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPMAPREDINPUTDIR"), null, "", true),
    HADOOPMAPREDINPUTDIRRECURSIVE(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPMAPREDINPUTDIRRECURSIVE"), false, "", true),
    MAPREDMAXSPLITSIZE(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMAXSPLITSIZE"), 256000000L, "", true),
    MAPREDMINSPLITSIZE(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZE"), 1L, "", true),
    MAPREDMINSPLITSIZEPERNODE(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZEPERNODE"), 1L, "", true),
    MAPREDMINSPLITSIZEPERRACK(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDMINSPLITSIZEPERRACK"), 1L, "", true),
    // The number of reduce tasks per job. Hadoop sets this value to 1 by default
    // By setting this property to -1, Hive will automatically determine the correct
    // number of reducers.
    HADOOPNUMREDUCERS(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPNUMREDUCERS"), -1, "", true),
    HADOOPJOBNAME(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPJOBNAME"), null, "", true),
    HADOOPSPECULATIVEEXECREDUCERS(ShimLoader.getHadoopShims().getHadoopConfNames().get("HADOOPSPECULATIVEEXECREDUCERS"), true, "", true),
    MAPREDSETUPCLEANUPNEEDED(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDSETUPCLEANUPNEEDED"), false, "", true),
    MAPREDTASKCLEANUPNEEDED(ShimLoader.getHadoopShims().getHadoopConfNames().get("MAPREDTASKCLEANUPNEEDED"), false, "", true),

    // Metastore stuff. Be sure to update HiveConf.metaVars when you add something here!
    METASTOREWAREHOUSE("hive.metastore.warehouse.dir", "/user/hive/warehouse",
        "location of default database for the warehouse"),
    METASTOREURIS("hive.metastore.uris", "",
        "Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore."),

    METASTORETHRIFTCONNECTIONRETRIES("hive.metastore.connect.retries", 3,
        "Number of retries while opening a connection to metastore"),
    METASTORETHRIFTFAILURERETRIES("hive.metastore.failure.retries", 1,
        "Number of retries upon failure of Thrift metastore calls"),

    METASTORE_CLIENT_CONNECT_RETRY_DELAY("hive.metastore.client.connect.retry.delay", "1s",
        new TimeValidator(TimeUnit.SECONDS),
        "Number of seconds for the client to wait between consecutive connection attempts"),
    METASTORE_CLIENT_SOCKET_TIMEOUT("hive.metastore.client.socket.timeout", "600s",
        new TimeValidator(TimeUnit.SECONDS),
        "MetaStore Client socket timeout in seconds"),
    METASTOREPWD("javax.jdo.option.ConnectionPassword", "mine",
        "password to use against metastore database"),
    METASTORECONNECTURLHOOK("hive.metastore.ds.connection.url.hook", "",
        "Name of the hook to use for retrieving the JDO connection URL. If empty, the value in javax.jdo.option.ConnectionURL is used"),
    METASTOREMULTITHREADED("javax.jdo.option.Multithreaded", true,
        "Set this to true if multiple threads access metastore through JDO concurrently."),
    METASTORECONNECTURLKEY("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=metastore_db;create=true",
        "JDBC connect string for a JDBC metastore"),
    HMSHANDLERATTEMPTS("hive.hmshandler.retry.attempts", 10,
        "The number of times to retry a HMSHandler call if there were a connection error."),
    HMSHANDLERINTERVAL("hive.hmshandler.retry.interval", "2000ms",
        new TimeValidator(TimeUnit.MILLISECONDS), "The time between HMSHandler retry attempts on failure."),
    HMSHANDLERFORCERELOADCONF("hive.hmshandler.force.reload.conf", false,
        "Whether to force reloading of the HMSHandler configuration (including\n" +
        "the connection URL, before the next metastore query that accesses the\n" +
        "datastore. Once reloaded, this value is reset to false. Used for\n" +
        "testing only."),
    METASTORESERVERMAXMESSAGESIZE("hive.metastore.server.max.message.size", 100*1024*1024,
        "Maximum message size in bytes a HMS will accept."),
    METASTORESERVERMINTHREADS("hive.metastore.server.min.threads", 200,
        "Minimum number of worker threads in the Thrift server's pool."),
    METASTORESERVERMAXTHREADS("hive.metastore.server.max.threads", 1000,
        "Maximum number of worker threads in the Thrift server's pool."),
    METASTORE_TCP_KEEP_ALIVE("hive.metastore.server.tcp.keepalive", true,
        "Whether to enable TCP keepalive for the metastore server. Keepalive will prevent accumulation of half-open connections."),

    METASTORE_INT_ORIGINAL("hive.metastore.archive.intermediate.original",
        "_INTERMEDIATE_ORIGINAL",
        "Intermediate dir suffixes used for archiving. Not important what they\n" +
        "are, as long as collisions are avoided"),
    METASTORE_INT_ARCHIVED("hive.metastore.archive.intermediate.archived",
        "_INTERMEDIATE_ARCHIVED", ""),
    METASTORE_INT_EXTRACTED("hive.metastore.archive.intermediate.extracted",
        "_INTERMEDIATE_EXTRACTED", ""),
    METASTORE_KERBEROS_KEYTAB_FILE("hive.metastore.kerberos.keytab.file", "",
        "The path to the Kerberos Keytab file containing the metastore Thrift server's service principal."),
    METASTORE_KERBEROS_PRINCIPAL("hive.metastore.kerberos.principal",
        "hive-metastore/_HOST@EXAMPLE.COM",
        "The service principal for the metastore Thrift server. \n" +
        "The special string _HOST will be replaced automatically with the correct host name."),
    METASTORE_USE_THRIFT_SASL("hive.metastore.sasl.enabled", false,
        "If true, the metastore Thrift interface will be secured with SASL. Clients must authenticate with Kerberos."),
    METASTORE_USE_THRIFT_FRAMED_TRANSPORT("hive.metastore.thrift.framed.transport.enabled", false,
        "If true, the metastore Thrift interface will use TFramedTransport. When false (default) a standard TTransport is used."),
    METASTORE_USE_THRIFT_COMPACT_PROTOCOL("hive.metastore.thrift.compact.protocol.enabled", false,
        "If true, the metastore Thrift interface will use TCompactProtocol. When false (default) TBinaryProtocol will be used.\n" +
        "Setting it to true will break compatibility with older clients running TBinaryProtocol."),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS("hive.cluster.delegation.token.store.class",
        "org.apache.hadoop.hive.thrift.MemoryTokenStore",
        "The delegation token store implementation. Set to org.apache.hadoop.hive.thrift.ZooKeeperTokenStore for load-balanced cluster."),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_ZK_CONNECTSTR(
        "hive.cluster.delegation.token.store.zookeeper.connectString", "",
        "The ZooKeeper token store connect string. You can re-use the configuration value\n" +
        "set in hive.zookeeper.quorum, by leaving this parameter unset."),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_ZK_ZNODE(
        "hive.cluster.delegation.token.store.zookeeper.znode", "/hivedelegation",
        "The root path for token store data. Note that this is used by both HiveServer2 and\n" +
        "MetaStore to store delegation Token. One directory gets created for each of them.\n" +
        "The final directory names would have the servername appended to it (HIVESERVER2,\n" +
        "METASTORE)."),
    METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_ZK_ACL(
        "hive.cluster.delegation.token.store.zookeeper.acl", "",
        "ACL for token store entries. Comma separated list of ACL entries. For example:\n" +
        "sasl:hive/host1@MY.DOMAIN:cdrwa,sasl:hive/host2@MY.DOMAIN:cdrwa\n" +
        "Defaults to all permissions for the hiveserver2/metastore process user."),
    METASTORE_CACHE_PINOBJTYPES("hive.metastore.cache.pinobjtypes", "Table,StorageDescriptor,SerDeInfo,Partition,Database,Type,FieldSchema,Order",
        "List of comma separated metastore object types that should be pinned in the cache"),
    METASTORE_CONNECTION_POOLING_TYPE("datanucleus.connectionPoolingType", "BONECP",
        "Specify connection pool library for datanucleus"),
    METASTORE_VALIDATE_TABLES("datanucleus.validateTables", false,
        "validates existing schema against code. turn this on if you want to verify existing schema"),
    METASTORE_VALIDATE_COLUMNS("datanucleus.validateColumns", false,
        "validates existing schema against code. turn this on if you want to verify existing schema"),
    METASTORE_VALIDATE_CONSTRAINTS("datanucleus.validateConstraints", false,
        "validates existing schema against code. turn this on if you want to verify existing schema"),
    METASTORE_STORE_MANAGER_TYPE("datanucleus.storeManagerType", "rdbms", "metadata store type"),
    METASTORE_AUTO_CREATE_SCHEMA("datanucleus.autoCreateSchema", true,
        "creates necessary schema on a startup if one doesn't exist. set this to false, after creating it once"),
    METASTORE_FIXED_DATASTORE("datanucleus.fixedDatastore", false, ""),
    METASTORE_SCHEMA_VERIFICATION("hive.metastore.schema.verification", false,
        "Enforce metastore schema version consistency.\n" +
        "True: Verify that version information stored in metastore matches with one from Hive jars.  Also disable automatic\n" +
        "      schema migration attempt. Users are required to manually migrate schema after Hive upgrade which ensures\n" +
        "      proper metastore schema migration. (Default)\n" +
        "False: Warn if the version information stored in metastore doesn't match with one from in Hive jars."),
    METASTORE_SCHEMA_VERIFICATION_RECORD_VERSION("hive.metastore.schema.verification.record.version", true,
      "When true the current MS version is recorded in the VERSION table. If this is disabled and verification is\n" +
      " enabled the MS will be unusable."),
    METASTORE_AUTO_START_MECHANISM_MODE("datanucleus.autoStartMechanismMode", "checked",
        "throw exception if metadata tables are incorrect"),
    METASTORE_TRANSACTION_ISOLATION("datanucleus.transactionIsolation", "read-committed",
        "Default transaction isolation level for identity generation."),
    METASTORE_CACHE_LEVEL2("datanucleus.cache.level2", false,
        "Use a level 2 cache. Turn this off if metadata is changed independently of Hive metastore server"),
    METASTORE_CACHE_LEVEL2_TYPE("datanucleus.cache.level2.type", "none", ""),
    METASTORE_IDENTIFIER_FACTORY("datanucleus.identifierFactory", "datanucleus1",
        "Name of the identifier factory to use when generating table/column names etc. \n" +
        "'datanucleus1' is used for backward compatibility with DataNucleus v1"),
    METASTORE_USE_LEGACY_VALUE_STRATEGY("datanucleus.rdbms.useLegacyNativeValueStrategy", true, ""),
    METASTORE_PLUGIN_REGISTRY_BUNDLE_CHECK("datanucleus.plugin.pluginRegistryBundleCheck", "LOG",
        "Defines what happens when plugin bundles are found and are duplicated [EXCEPTION|LOG|NONE]"),
    METASTORE_BATCH_RETRIEVE_MAX("hive.metastore.batch.retrieve.max", 300,
        "Maximum number of objects (tables/partitions) can be retrieved from metastore in one batch. \n" +
        "The higher the number, the less the number of round trips is needed to the Hive metastore server, \n" +
        "but it may also cause higher memory requirement at the client side."),
    METASTORE_BATCH_RETRIEVE_TABLE_PARTITION_MAX(
        "hive.metastore.batch.retrieve.table.partition.max", 1000,
        "Maximum number of table partitions that metastore internally retrieves in one batch."),

    METASTORE_INIT_HOOKS("hive.metastore.init.hooks", "",
        "A comma separated list of hooks to be invoked at the beginning of HMSHandler initialization. \n" +
        "An init hook is specified as the name of Java class which extends org.apache.hadoop.hive.metastore.MetaStoreInitListener."),
    METASTORE_PRE_EVENT_LISTENERS("hive.metastore.pre.event.listeners", "",
        "List of comma separated listeners for metastore events."),
    METASTORE_EVENT_LISTENERS("hive.metastore.event.listeners", "", ""),
    METASTORE_EVENT_DB_LISTENER_TTL("hive.metastore.event.db.listener.timetolive", "86400s",
        new TimeValidator(TimeUnit.SECONDS),
        "time after which events will be removed from the database listener queue"),
    METASTORE_AUTHORIZATION_STORAGE_AUTH_CHECKS("hive.metastore.authorization.storage.checks", false,
        "Should the metastore do authorization checks against the underlying storage (usually hdfs) \n" +
        "for operations like drop-partition (disallow the drop-partition if the user in\n" +
        "question doesn't have permissions to delete the corresponding directory\n" +
        "on the storage)."),
    METASTORE_EVENT_CLEAN_FREQ("hive.metastore.event.clean.freq", "0s",
        new TimeValidator(TimeUnit.SECONDS),
        "Frequency at which timer task runs to purge expired events in metastore."),
    METASTORE_EVENT_EXPIRY_DURATION("hive.metastore.event.expiry.duration", "0s",
        new TimeValidator(TimeUnit.SECONDS),
        "Duration after which events expire from events table"),
    METASTORE_EXECUTE_SET_UGI("hive.metastore.execute.setugi", true,
        "In unsecure mode, setting this property to true will cause the metastore to execute DFS operations using \n" +
        "the client's reported user and group permissions. Note that this property must be set on \n" +
        "both the client and server sides. Further note that its best effort. \n" +
        "If client sets its to true and server sets it to false, client setting will be ignored."),
    METASTORE_PARTITION_NAME_WHITELIST_PATTERN("hive.metastore.partition.name.whitelist.pattern", "",
        "Partition names will be checked against this regex pattern and rejected if not matched."),

    METASTORE_INTEGER_JDO_PUSHDOWN("hive.metastore.integral.jdo.pushdown", false,
        "Allow JDO query pushdown for integral partition columns in metastore. Off by default. This\n" +
        "improves metastore perf for integral columns, especially if there's a large number of partitions.\n" +
        "However, it doesn't work correctly with integral values that are not normalized (e.g. have\n" +
        "leading zeroes, like 0012). If metastore direct SQL is enabled and works, this optimization\n" +
        "is also irrelevant."),
    METASTORE_TRY_DIRECT_SQL("hive.metastore.try.direct.sql", true,
        "Whether the Hive metastore should try to use direct SQL queries instead of the\n" +
        "DataNucleus for certain read paths. This can improve metastore performance when\n" +
        "fetching many partitions or column statistics by orders of magnitude; however, it\n" +
        "is not guaranteed to work on all RDBMS-es and all versions. In case of SQL failures,\n" +
        "the metastore will fall back to the DataNucleus, so it's safe even if SQL doesn't\n" +
        "work for all queries on your datastore. If all SQL queries fail (for example, your\n" +
        "metastore is backed by MongoDB), you might want to disable this to save the\n" +
        "try-and-fall-back cost."),
    METASTORE_DIRECT_SQL_PARTITION_BATCH_SIZE("hive.metastore.direct.sql.batch.size", 0,
        "Batch size for partition and other object retrieval from the underlying DB in direct\n" +
        "SQL. For some DBs like Oracle and MSSQL, there are hardcoded or perf-based limitations\n" +
        "that necessitate this. For DBs that can handle the queries, this isn't necessary and\n" +
        "may impede performance. -1 means no batching, 0 means automatic batching."),
    METASTORE_TRY_DIRECT_SQL_DDL("hive.metastore.try.direct.sql.ddl", true,
        "Same as hive.metastore.try.direct.sql, for read statements within a transaction that\n" +
        "modifies metastore data. Due to non-standard behavior in Postgres, if a direct SQL\n" +
        "select query has incorrect syntax or something similar inside a transaction, the\n" +
        "entire transaction will fail and fall-back to DataNucleus will not be possible. You\n" +
        "should disable the usage of direct SQL inside transactions if that happens in your case."),
    METASTORE_ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS("hive.metastore.orm.retrieveMapNullsAsEmptyStrings",false,
        "Thrift does not support nulls in maps, so any nulls present in maps retrieved from ORM must " +
        "either be pruned or converted to empty strings. Some backing dbs such as Oracle persist empty strings " +
        "as nulls, so we should set this parameter if we wish to reverse that behaviour. For others, " +
        "pruning is the correct behaviour"),
    METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES(
        "hive.metastore.disallow.incompatible.col.type.changes", false,
        "If true (default is false), ALTER TABLE operations which change the type of a\n" +
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

    NEWTABLEDEFAULTPARA("hive.table.parameters.default", "",
        "Default property values for newly created tables"),
    DDL_CTL_PARAMETERS_WHITELIST("hive.ddl.createtablelike.properties.whitelist", "",
        "Table Properties to copy over when executing a Create Table Like."),
    METASTORE_RAW_STORE_IMPL("hive.metastore.rawstore.impl", "org.apache.hadoop.hive.metastore.ObjectStore",
        "Name of the class that implements org.apache.hadoop.hive.metastore.rawstore interface. \n" +
        "This class is used to store and retrieval of raw metadata objects such as table, database"),
    METASTORE_CONNECTION_DRIVER("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver",
        "Driver class name for a JDBC metastore"),
    METASTORE_MANAGER_FACTORY_CLASS("javax.jdo.PersistenceManagerFactoryClass",
        "org.datanucleus.api.jdo.JDOPersistenceManagerFactory",
        "class implementing the jdo persistence"),
    METASTORE_EXPRESSION_PROXY_CLASS("hive.metastore.expression.proxy",
        "org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore", ""),
    METASTORE_DETACH_ALL_ON_COMMIT("javax.jdo.option.DetachAllOnCommit", true,
        "Detaches all objects from session so that they can be used after transaction is committed"),
    METASTORE_NON_TRANSACTIONAL_READ("javax.jdo.option.NonTransactionalRead", true,
        "Reads outside of transactions"),
    METASTORE_CONNECTION_USER_NAME("javax.jdo.option.ConnectionUserName", "APP",
        "Username to use against metastore database"),
    METASTORE_END_FUNCTION_LISTENERS("hive.metastore.end.function.listeners", "",
        "List of comma separated listeners for the end of metastore functions."),
    METASTORE_PART_INHERIT_TBL_PROPS("hive.metastore.partition.inherit.table.properties", "",
        "List of comma separated keys occurring in table properties which will get inherited to newly created partitions. \n" +
        "* implies all the keys will get inherited."),
    METASTORE_FILTER_HOOK("hive.metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl",
        "Metastore hook class for filtering the metadata read results"),

    // Parameters for exporting metadata on table drop (requires the use of the)
    // org.apache.hadoop.hive.ql.parse.MetaDataExportListener preevent listener
    METADATA_EXPORT_LOCATION("hive.metadata.export.location", "",
        "When used in conjunction with the org.apache.hadoop.hive.ql.parse.MetaDataExportListener pre event listener, \n" +
        "it is the location to which the metadata will be exported. The default is an empty string, which results in the \n" +
        "metadata being exported to the current user's home directory on HDFS."),
    MOVE_EXPORTED_METADATA_TO_TRASH("hive.metadata.move.exported.metadata.to.trash", true,
        "When used in conjunction with the org.apache.hadoop.hive.ql.parse.MetaDataExportListener pre event listener, \n" +
        "this setting determines if the metadata that is exported will subsequently be moved to the user's trash directory \n" +
        "alongside the dropped table data. This ensures that the metadata will be cleaned up along with the dropped table data."),

    // CLI
    CLIIGNOREERRORS("hive.cli.errors.ignore", false, ""),
    CLIPRINTCURRENTDB("hive.cli.print.current.db", false,
        "Whether to include the current database in the Hive prompt."),
    CLIPROMPT("hive.cli.prompt", "hive",
        "Command line prompt configuration value. Other hiveconf can be used in this configuration value. \n" +
        "Variable substitution will only be invoked at the Hive CLI startup."),
    CLIPRETTYOUTPUTNUMCOLS("hive.cli.pretty.output.num.cols", -1,
        "The number of columns to use when formatting output generated by the DESCRIBE PRETTY table_name command.\n" +
        "If the value of this property is -1, then Hive will use the auto-detected terminal width."),

    HIVE_METASTORE_FS_HANDLER_CLS("hive.metastore.fs.handler.class", "org.apache.hadoop.hive.metastore.HiveMetaStoreFsImpl", ""),

    // Things we log in the jobconf

    // session identifier
    HIVESESSIONID("hive.session.id", "", ""),
    // whether session is running in silent mode or not
    HIVESESSIONSILENT("hive.session.silent", false, ""),

    HIVE_SESSION_HISTORY_ENABLED("hive.session.history.enabled", false,
        "Whether to log Hive query, query plan, runtime statistics etc."),

    HIVEQUERYSTRING("hive.query.string", "",
        "Query being executed (might be multiple per a session)"),

    HIVEQUERYID("hive.query.id", "",
        "ID for query being executed (might be multiple per a session)"),

    HIVEJOBNAMELENGTH("hive.jobname.length", 50, "max jobname length"),

    // hive jar
    HIVEJAR("hive.jar.path", "",
        "The location of hive_cli.jar that is used when submitting jobs in a separate jvm."),
    HIVEAUXJARS("hive.aux.jars.path", "",
        "The location of the plugin jars that contain implementations of user defined functions and serdes."),

    // reloadable jars
    HIVERELOADABLEJARS("hive.reloadable.aux.jars.path", "",
        "Jars can be renewed by executing reload command. And these jars can be "
            + "used as the auxiliary classes like creating a UDF or SerDe."),

    // hive added files and jars
    HIVEADDEDFILES("hive.added.files.path", "", "This an internal parameter."),
    HIVEADDEDJARS("hive.added.jars.path", "", "This an internal parameter."),
    HIVEADDEDARCHIVES("hive.added.archives.path", "", "This an internal parameter."),

    HIVE_CURRENT_DATABASE("hive.current.database", "", "Database name used by current session. Internal usage only.", true),

    // for hive script operator
    HIVES_AUTO_PROGRESS_TIMEOUT("hive.auto.progress.timeout", "0s",
        new TimeValidator(TimeUnit.SECONDS),
        "How long to run autoprogressor for the script/UDTF operators.\n" +
        "Set to 0 for forever."),
    HIVESCRIPTAUTOPROGRESS("hive.script.auto.progress", false,
        "Whether Hive Transform/Map/Reduce Clause should automatically send progress information to TaskTracker \n" +
        "to avoid the task getting killed because of inactivity.  Hive sends progress information when the script is \n" +
        "outputting to stderr.  This option removes the need of periodically producing stderr messages, \n" +
        "but users should be cautious because this may prevent infinite loops in the scripts to be killed by TaskTracker."),
    HIVESCRIPTIDENVVAR("hive.script.operator.id.env.var", "HIVE_SCRIPT_OPERATOR_ID",
        "Name of the environment variable that holds the unique script operator ID in the user's \n" +
        "transform function (the custom mapper/reducer that the user has specified in the query)"),
    HIVESCRIPTTRUNCATEENV("hive.script.operator.truncate.env", false,
        "Truncate each environment variable for external script in scripts operator to 20KB (to fit system limits)"),
    HIVESCRIPT_ENV_BLACKLIST("hive.script.operator.env.blacklist",
        "hive.txn.valid.txns,hive.script.operator.env.blacklist",
        "Comma separated list of keys from the configuration file not to convert to environment " +
        "variables when envoking the script operator"),
    HIVEMAPREDMODE("hive.mapred.mode", "nonstrict",
        "The mode in which the Hive operations are being performed. \n" +
        "In strict mode, some risky queries are not allowed to run. They include:\n" +
        "  Cartesian Product.\n" +
        "  No partition being picked up for a query.\n" +
        "  Comparing bigints and strings.\n" +
        "  Comparing bigints and doubles.\n" +
        "  Orderby without limit."),
    HIVEALIAS("hive.alias", "", ""),
    HIVEMAPSIDEAGGREGATE("hive.map.aggr", true, "Whether to use map-side aggregation in Hive Group By queries"),
    HIVEGROUPBYSKEW("hive.groupby.skewindata", false, "Whether there is skew in data to optimize group by queries"),
    HIVE_OPTIMIZE_MULTI_GROUPBY_COMMON_DISTINCTS("hive.optimize.multigroupby.common.distincts", true,
        "Whether to optimize a multi-groupby query with the same distinct.\n" +
        "Consider a query like:\n" +
        "\n" +
        "  from src\n" +
        "    insert overwrite table dest1 select col1, count(distinct colx) group by col1\n" +
        "    insert overwrite table dest2 select col2, count(distinct colx) group by col2;\n" +
        "\n" +
        "With this parameter set to true, first we spray by the distinct value (colx), and then\n" +
        "perform the 2 groups bys. This makes sense if map-side aggregation is turned off. However,\n" +
        "with maps-side aggregation, it might be useful in some cases to treat the 2 inserts independently, \n" +
        "thereby performing the query above in 2MR jobs instead of 3 (due to spraying by distinct key first).\n" +
        "If this parameter is turned off, we don't consider the fact that the distinct key is the same across\n" +
        "different MR jobs."),
    HIVEJOINEMITINTERVAL("hive.join.emit.interval", 1000,
        "How many rows in the right-most join operand Hive should buffer before emitting the join result."),
    HIVEJOINCACHESIZE("hive.join.cache.size", 25000,
        "How many rows in the joining tables (except the streaming table) should be cached in memory."),

    // CBO related
    HIVE_CBO_ENABLED("hive.cbo.enable", true, "Flag to control enabling Cost Based Optimizations using Calcite framework."),

    // hive.mapjoin.bucket.cache.size has been replaced by hive.smbjoin.cache.row,
    // need to remove by hive .13. Also, do not change default (see SMB operator)
    HIVEMAPJOINBUCKETCACHESIZE("hive.mapjoin.bucket.cache.size", 100, ""),

    HIVEMAPJOINUSEOPTIMIZEDTABLE("hive.mapjoin.optimized.hashtable", true,
        "Whether Hive should use memory-optimized hash table for MapJoin. Only works on Tez,\n" +
        "because memory-optimized hashtable cannot be serialized."),
    HIVEHASHTABLEWBSIZE("hive.mapjoin.optimized.hashtable.wbsize", 10 * 1024 * 1024,
        "Optimized hashtable (see hive.mapjoin.optimized.hashtable) uses a chain of buffers to\n" +
        "store data. This is one buffer size. HT may be slightly faster if this is larger, but for small\n" +
        "joins unnecessary memory will be allocated and then trimmed."),

    HIVESMBJOINCACHEROWS("hive.smbjoin.cache.rows", 10000,
        "How many rows with the same key value should be cached in memory per smb joined table."),
    HIVEGROUPBYMAPINTERVAL("hive.groupby.mapaggr.checkinterval", 100000,
        "Number of rows after which size of the grouping keys/aggregation classes is performed"),
    HIVEMAPAGGRHASHMEMORY("hive.map.aggr.hash.percentmemory", (float) 0.5,
        "Portion of total memory to be used by map-side group aggregation hash table"),
    HIVEMAPJOINFOLLOWEDBYMAPAGGRHASHMEMORY("hive.mapjoin.followby.map.aggr.hash.percentmemory", (float) 0.3,
        "Portion of total memory to be used by map-side group aggregation hash table, when this group by is followed by map join"),
    HIVEMAPAGGRMEMORYTHRESHOLD("hive.map.aggr.hash.force.flush.memory.threshold", (float) 0.9,
        "The max memory to be used by map-side group aggregation hash table.\n" +
        "If the memory usage is higher than this number, force to flush data"),
    HIVEMAPAGGRHASHMINREDUCTION("hive.map.aggr.hash.min.reduction", (float) 0.5,
        "Hash aggregation will be turned off if the ratio between hash  table size and input rows is bigger than this number. \n" +
        "Set to 1 to make sure hash aggregation is never turned off."),
    HIVEMULTIGROUPBYSINGLEREDUCER("hive.multigroupby.singlereducer", true,
        "Whether to optimize multi group by query to generate single M/R  job plan. If the multi group by query has \n" +
        "common group by keys, it will be optimized to generate single M/R job."),
    HIVE_MAP_GROUPBY_SORT("hive.map.groupby.sorted", false,
        "If the bucketing/sorting properties of the table exactly match the grouping key, whether to perform \n" +
        "the group by in the mapper by using BucketizedHiveInputFormat. The only downside to this\n" +
        "is that it limits the number of mappers to the number of files."),
    HIVE_MAP_GROUPBY_SORT_TESTMODE("hive.map.groupby.sorted.testmode", false,
        "If the bucketing/sorting properties of the table exactly match the grouping key, whether to perform \n" +
        "the group by in the mapper by using BucketizedHiveInputFormat. If the test mode is set, the plan\n" +
        "is not converted, but a query property is set to denote the same."),
    HIVE_GROUPBY_ORDERBY_POSITION_ALIAS("hive.groupby.orderby.position.alias", false,
        "Whether to enable using Column Position Alias in Group By or Order By"),
    HIVE_NEW_JOB_GROUPING_SET_CARDINALITY("hive.new.job.grouping.set.cardinality", 30,
        "Whether a new map-reduce job should be launched for grouping sets/rollups/cubes.\n" +
        "For a query like: select a, b, c, count(1) from T group by a, b, c with rollup;\n" +
        "4 rows are created per row: (a, b, c), (a, b, null), (a, null, null), (null, null, null).\n" +
        "This can lead to explosion across map-reduce boundary if the cardinality of T is very high,\n" +
        "and map-side aggregation does not do a very good job. \n" +
        "\n" +
        "This parameter decides if Hive should add an additional map-reduce job. If the grouping set\n" +
        "cardinality (4 in the example above), is more than this value, a new MR job is added under the\n" +
        "assumption that the original group by will reduce the data size."),

    // Max filesize used to do a single copy (after that, distcp is used)
    HIVE_EXEC_COPYFILE_MAXSIZE("hive.exec.copyfile.maxsize", 32L * 1024 * 1024 /*32M*/,
        "Maximum file size (in Mb) that Hive uses to do single HDFS copies between directories." +
        "Distributed copies (distcp) will be used instead for bigger files so that copies can be done faster."),

    // for hive udtf operator
    HIVEUDTFAUTOPROGRESS("hive.udtf.auto.progress", false,
        "Whether Hive should automatically send progress information to TaskTracker \n" +
        "when using UDTF's to prevent the task getting killed because of inactivity.  Users should be cautious \n" +
        "because this may prevent TaskTracker from killing tasks with infinite loops."),

    HIVEDEFAULTFILEFORMAT("hive.default.fileformat", "TextFile", new StringSet("TextFile", "SequenceFile", "RCfile", "ORC"),
        "Default file format for CREATE TABLE statement. Users can explicitly override it by CREATE TABLE ... STORED AS [FORMAT]"),
    HIVEQUERYRESULTFILEFORMAT("hive.query.result.fileformat", "TextFile", new StringSet("TextFile", "SequenceFile", "RCfile"),
        "Default file format for storing result of the query."),
    HIVECHECKFILEFORMAT("hive.fileformat.check", true, "Whether to check file format or not when loading data files"),

    // default serde for rcfile
    HIVEDEFAULTRCFILESERDE("hive.default.rcfile.serde",
        "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
        "The default SerDe Hive will use for the RCFile format"),

    HIVEDEFAULTSERDE("hive.default.serde",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "The default SerDe Hive will use for storage formats that do not specify a SerDe."),

    SERDESUSINGMETASTOREFORSCHEMA("hive.serdes.using.metastore.for.schema",
        "org.apache.hadoop.hive.ql.io.orc.OrcSerde,org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe," +
        "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe,org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe," +
        "org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe,org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe," +
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe",
        "SerDes retriving schema from metastore. This an internal parameter. Check with the hive dev. team"),

    HIVEHISTORYFILELOC("hive.querylog.location",
        "${system:java.io.tmpdir}" + File.separator + "${system:user.name}",
        "Location of Hive run time structured log file"),

    HIVE_LOG_INCREMENTAL_PLAN_PROGRESS("hive.querylog.enable.plan.progress", true,
        "Whether to log the plan's progress every time a job's progress is checked.\n" +
        "These logs are written to the location specified by hive.querylog.location"),

    HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL("hive.querylog.plan.progress.interval", "60000ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "The interval to wait between logging the plan's progress.\n" +
        "If there is a whole number percentage change in the progress of the mappers or the reducers,\n" +
        "the progress is logged regardless of this value.\n" +
        "The actual interval will be the ceiling of (this value divided by the value of\n" +
        "hive.exec.counters.pull.interval) multiplied by the value of hive.exec.counters.pull.interval\n" +
        "I.e. if it is not divide evenly by the value of hive.exec.counters.pull.interval it will be\n" +
        "logged less frequently than specified.\n" +
        "This only has an effect if hive.querylog.enable.plan.progress is set to true."),

    HIVESCRIPTSERDE("hive.script.serde", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "The default SerDe for transmitting input data to and reading output data from the user scripts. "),
    HIVESCRIPTRECORDREADER("hive.script.recordreader",
        "org.apache.hadoop.hive.ql.exec.TextRecordReader",
        "The default record reader for reading data from the user scripts. "),
    HIVESCRIPTRECORDWRITER("hive.script.recordwriter",
        "org.apache.hadoop.hive.ql.exec.TextRecordWriter",
        "The default record writer for writing data to the user scripts. "),
    HIVESCRIPTESCAPE("hive.transform.escape.input", false,
        "This adds an option to escape special chars (newlines, carriage returns and\n" +
        "tabs) when they are passed to the user script. This is useful if the Hive tables\n" +
        "can contain data that contains special characters."),
    HIVEBINARYRECORDMAX("hive.binary.record.max.length", 1000,
        "Read from a binary stream and treat each hive.binary.record.max.length bytes as a record. \n" +
        "The last record before the end of stream can have less than hive.binary.record.max.length bytes"),

    // HWI
    HIVEHWILISTENHOST("hive.hwi.listen.host", "0.0.0.0", "This is the host address the Hive Web Interface will listen on"),
    HIVEHWILISTENPORT("hive.hwi.listen.port", "9999", "This is the port the Hive Web Interface will listen on"),
    HIVEHWIWARFILE("hive.hwi.war.file", "${env:HWI_WAR_FILE}",
        "This sets the path to the HWI war file, relative to ${HIVE_HOME}. "),

    HIVEHADOOPMAXMEM("hive.mapred.local.mem", 0, "mapper/reducer memory in local mode"),

    //small table file size
    HIVESMALLTABLESFILESIZE("hive.mapjoin.smalltable.filesize", 25000000L,
        "The threshold for the input file size of the small tables; if the file size is smaller \n" +
        "than this threshold, it will try to convert the common join into map join"),

    HIVESAMPLERANDOMNUM("hive.sample.seednumber", 0,
        "A number used to percentage sampling. By changing this number, user will change the subsets of data sampled."),

    // test mode in hive mode
    HIVETESTMODE("hive.test.mode", false,
        "Whether Hive is running in test mode. If yes, it turns on sampling and prefixes the output tablename."),
    HIVETESTMODEPREFIX("hive.test.mode.prefix", "test_",
        "In test mode, specfies prefixes for the output table"),
    HIVETESTMODESAMPLEFREQ("hive.test.mode.samplefreq", 32,
        "In test mode, specfies sampling frequency for table, which is not bucketed,\n" +
        "For example, the following query:\n" +
        "  INSERT OVERWRITE TABLE dest SELECT col1 from src\n" +
        "would be converted to\n" +
        "  INSERT OVERWRITE TABLE test_dest\n" +
        "  SELECT col1 from src TABLESAMPLE (BUCKET 1 out of 32 on rand(1))"),
    HIVETESTMODENOSAMPLE("hive.test.mode.nosamplelist", "",
        "In test mode, specifies comma separated table names which would not apply sampling"),
    HIVETESTMODEDUMMYSTATAGGR("hive.test.dummystats.aggregator", "", "internal variable for test"),
    HIVETESTMODEDUMMYSTATPUB("hive.test.dummystats.publisher", "", "internal variable for test"),

    HIVEMERGEMAPFILES("hive.merge.mapfiles", true,
        "Merge small files at the end of a map-only job"),
    HIVEMERGEMAPREDFILES("hive.merge.mapredfiles", false,
        "Merge small files at the end of a map-reduce job"),
    HIVEMERGETEZFILES("hive.merge.tezfiles", false, "Merge small files at the end of a Tez DAG"),
    HIVEMERGESPARKFILES("hive.merge.sparkfiles", false, "Merge small files at the end of a Spark DAG Transformation"),
    HIVEMERGEMAPFILESSIZE("hive.merge.size.per.task", (long) (256 * 1000 * 1000),
        "Size of merged files at the end of the job"),
    HIVEMERGEMAPFILESAVGSIZE("hive.merge.smallfiles.avgsize", (long) (16 * 1000 * 1000),
        "When the average output file size of a job is less than this number, Hive will start an additional \n" +
        "map-reduce job to merge the output files into bigger files. This is only done for map-only jobs \n" +
        "if hive.merge.mapfiles is true, and for map-reduce jobs if hive.merge.mapredfiles is true."),
    HIVEMERGERCFILEBLOCKLEVEL("hive.merge.rcfile.block.level", true, ""),
    HIVEMERGEORCFILESTRIPELEVEL("hive.merge.orcfile.stripe.level", true,
        "When hive.merge.mapfiles, hive.merge.mapredfiles or hive.merge.tezfiles is enabled\n" +
        "while writing a table with ORC file format, enabling this config will do stripe-level\n" +
        "fast merge for small ORC files. Note that enabling this config will not honor the\n" +
        "padding tolerance config (hive.exec.orc.block.padding.tolerance)."),

    HIVEUSEEXPLICITRCFILEHEADER("hive.exec.rcfile.use.explicit.header", true,
        "If this is set the header for RCFiles will simply be RCF.  If this is not\n" +
        "set the header will be that borrowed from sequence files, e.g. SEQ- followed\n" +
        "by the input and output RCFile formats."),
    HIVEUSERCFILESYNCCACHE("hive.exec.rcfile.use.sync.cache", true, ""),

    HIVE_RCFILE_RECORD_INTERVAL("hive.io.rcfile.record.interval", Integer.MAX_VALUE, ""),
    HIVE_RCFILE_COLUMN_NUMBER_CONF("hive.io.rcfile.column.number.conf", 0, ""),
    HIVE_RCFILE_TOLERATE_CORRUPTIONS("hive.io.rcfile.tolerate.corruptions", false, ""),
    HIVE_RCFILE_RECORD_BUFFER_SIZE("hive.io.rcfile.record.buffer.size", 4194304, ""),   // 4M

    PARQUET_MEMORY_POOL_RATIO("parquet.memory.pool.ratio", 0.5f,
        "Maximum fraction of heap that can be used by Parquet file writers in one task.\n" +
        "It is for avoiding OutOfMemory error in tasks. Work with Parquet 1.6.0 and above.\n" +
        "This config parameter is defined in Parquet, so that it does not start with 'hive.'."),
    HIVE_PARQUET_TIMESTAMP_SKIP_CONVERSION("hive.parquet.timestamp.skip.conversion", true,
      "Current Hive implementation of parquet stores timestamps to UTC, this flag allows skipping of the conversion" +
      "on reading parquet files from other tools"),
    HIVE_ORC_FILE_MEMORY_POOL("hive.exec.orc.memory.pool", 0.5f,
        "Maximum fraction of heap that can be used by ORC file writers"),
    HIVE_ORC_WRITE_FORMAT("hive.exec.orc.write.format", null,
        "Define the version of the file to write. Possible values are 0.11 and 0.12.\n" +
        "If this parameter is not defined, ORC will use the run length encoding (RLE)\n" +
        "introduced in Hive 0.12. Any value other than 0.11 results in the 0.12 encoding."),
    HIVE_ORC_DEFAULT_STRIPE_SIZE("hive.exec.orc.default.stripe.size",
        64L * 1024 * 1024,
        "Define the default ORC stripe size, in bytes."),
    HIVE_ORC_DEFAULT_BLOCK_SIZE("hive.exec.orc.default.block.size", 256L * 1024 * 1024,
        "Define the default file system block size for ORC files."),

    HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD("hive.exec.orc.dictionary.key.size.threshold", 0.8f,
        "If the number of keys in a dictionary is greater than this fraction of the total number of\n" +
        "non-null rows, turn off dictionary encoding.  Use 1 to always use dictionary encoding."),
    HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE("hive.exec.orc.default.row.index.stride", 10000,
        "Define the default ORC index stride in number of rows. (Stride is the number of rows\n" +
        "an index entry represents.)"),
    HIVE_ORC_ROW_INDEX_STRIDE_DICTIONARY_CHECK("hive.orc.row.index.stride.dictionary.check", true,
        "If enabled dictionary check will happen after first row index stride (default 10000 rows)\n" +
        "else dictionary check will happen before writing first stripe. In both cases, the decision\n" +
        "to use dictionary or not will be retained thereafter."),
    HIVE_ORC_DEFAULT_BUFFER_SIZE("hive.exec.orc.default.buffer.size", 256 * 1024,
        "Define the default ORC buffer size, in bytes."),
    HIVE_ORC_DEFAULT_BLOCK_PADDING("hive.exec.orc.default.block.padding", true,
        "Define the default block padding, which pads stripes to the HDFS block boundaries."),
    HIVE_ORC_BLOCK_PADDING_TOLERANCE("hive.exec.orc.block.padding.tolerance", 0.05f,
        "Define the tolerance for block padding as a decimal fraction of stripe size (for\n" +
        "example, the default value 0.05 is 5% of the stripe size). For the defaults of 64Mb\n" +
        "ORC stripe and 256Mb HDFS blocks, the default block padding tolerance of 5% will\n" +
        "reserve a maximum of 3.2Mb for padding within the 256Mb block. In that case, if the\n" +
        "available size within the block is more than 3.2Mb, a new smaller stripe will be\n" +
        "inserted to fit within that space. This will make sure that no stripe written will\n" +
        "cross block boundaries and cause remote reads within a node local task."),
    HIVE_ORC_DEFAULT_COMPRESS("hive.exec.orc.default.compress", "ZLIB", "Define the default compression codec for ORC file"),

    HIVE_ORC_ENCODING_STRATEGY("hive.exec.orc.encoding.strategy", "SPEED", new StringSet("SPEED", "COMPRESSION"),
        "Define the encoding strategy to use while writing data. Changing this will\n" +
        "only affect the light weight encoding for integers. This flag will not\n" +
        "change the compression level of higher level compression codec (like ZLIB)."),

    HIVE_ORC_COMPRESSION_STRATEGY("hive.exec.orc.compression.strategy", "SPEED", new StringSet("SPEED", "COMPRESSION"),
         "Define the compression strategy to use while writing data. \n" +
         "This changes the compression level of higher level compression codec (like ZLIB)."),

    HIVE_ORC_INCLUDE_FILE_FOOTER_IN_SPLITS("hive.orc.splits.include.file.footer", false,
        "If turned on splits generated by orc will include metadata about the stripes in the file. This\n" +
        "data is read remotely (from the client or HS2 machine) and sent to all the tasks."),
    HIVE_ORC_CACHE_STRIPE_DETAILS_SIZE("hive.orc.cache.stripe.details.size", 10000,
        "Cache size for keeping meta info about orc splits cached in the client."),
    HIVE_ORC_COMPUTE_SPLITS_NUM_THREADS("hive.orc.compute.splits.num.threads", 10,
        "How many threads orc should use to create splits in parallel."),
    HIVE_ORC_SKIP_CORRUPT_DATA("hive.exec.orc.skip.corrupt.data", false,
        "If ORC reader encounters corrupt data, this value will be used to determine\n" +
        "whether to skip the corrupt data or throw exception. The default behavior is to throw exception."),

    HIVE_ORC_ZEROCOPY("hive.exec.orc.zerocopy", false,
        "Use zerocopy reads with ORC. (This requires Hadoop 2.3 or later.)"),

    HIVE_LAZYSIMPLE_EXTENDED_BOOLEAN_LITERAL("hive.lazysimple.extended_boolean_literal", false,
        "LazySimpleSerde uses this property to determine if it treats 'T', 't', 'F', 'f',\n" +
        "'1', and '0' as extened, legal boolean literal, in addition to 'TRUE' and 'FALSE'.\n" +
        "The default is false, which means only 'TRUE' and 'FALSE' are treated as legal\n" +
        "boolean literal."),

    HIVESKEWJOIN("hive.optimize.skewjoin", false,
        "Whether to enable skew join optimization. \n" +
        "The algorithm is as follows: At runtime, detect the keys with a large skew. Instead of\n" +
        "processing those keys, store them temporarily in an HDFS directory. In a follow-up map-reduce\n" +
        "job, process those skewed keys. The same key need not be skewed for all the tables, and so,\n" +
        "the follow-up map-reduce job (for the skewed keys) would be much faster, since it would be a\n" +
        "map-join."),
    HIVECONVERTJOIN("hive.auto.convert.join", true,
        "Whether Hive enables the optimization about converting common join into mapjoin based on the input file size"),
    HIVECONVERTJOINNOCONDITIONALTASK("hive.auto.convert.join.noconditionaltask", true,
        "Whether Hive enables the optimization about converting common join into mapjoin based on the input file size. \n" +
        "If this parameter is on, and the sum of size for n-1 of the tables/partitions for a n-way join is smaller than the\n" +
        "specified size, the join is directly converted to a mapjoin (there is no conditional task)."),

    HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD("hive.auto.convert.join.noconditionaltask.size",
        10000000L,
        "If hive.auto.convert.join.noconditionaltask is off, this parameter does not take affect. \n" +
        "However, if it is on, and the sum of size for n-1 of the tables/partitions for a n-way join is smaller than this size, \n" +
        "the join is directly converted to a mapjoin(there is no conditional task). The default is 10MB"),
    HIVECONVERTJOINUSENONSTAGED("hive.auto.convert.join.use.nonstaged", false,
        "For conditional joins, if input stream from a small alias can be directly applied to join operator without \n" +
        "filtering or projection, the alias need not to be pre-staged in distributed cache via mapred local task.\n" +
        "Currently, this is not working with vectorization or tez execution engine."),
    HIVESKEWJOINKEY("hive.skewjoin.key", 100000,
        "Determine if we get a skew key in join. If we see more than the specified number of rows with the same key in join operator,\n" +
        "we think the key as a skew join key. "),
    HIVESKEWJOINMAPJOINNUMMAPTASK("hive.skewjoin.mapjoin.map.tasks", 10000,
        "Determine the number of map task used in the follow up map join job for a skew join.\n" +
        "It should be used together with hive.skewjoin.mapjoin.min.split to perform a fine grained control."),
    HIVESKEWJOINMAPJOINMINSPLIT("hive.skewjoin.mapjoin.min.split", 33554432L,
        "Determine the number of map task at most used in the follow up map join job for a skew join by specifying \n" +
        "the minimum split size. It should be used together with hive.skewjoin.mapjoin.map.tasks to perform a fine grained control."),

    HIVESENDHEARTBEAT("hive.heartbeat.interval", 1000,
        "Send a heartbeat after this interval - used by mapjoin and filter operators"),
    HIVELIMITMAXROWSIZE("hive.limit.row.max.size", 100000L,
        "When trying a smaller subset of data for simple LIMIT, how much size we need to guarantee each row to have at least."),
    HIVELIMITOPTLIMITFILE("hive.limit.optimize.limit.file", 10,
        "When trying a smaller subset of data for simple LIMIT, maximum number of files we can sample."),
    HIVELIMITOPTENABLE("hive.limit.optimize.enable", false,
        "Whether to enable to optimization to trying a smaller subset of data for simple LIMIT first."),
    HIVELIMITOPTMAXFETCH("hive.limit.optimize.fetch.max", 50000,
        "Maximum number of rows allowed for a smaller subset of data for simple LIMIT, if it is a fetch query. \n" +
        "Insert queries are not restricted by this limit."),
    HIVELIMITPUSHDOWNMEMORYUSAGE("hive.limit.pushdown.memory.usage", -1f,
        "The max memory to be used for hash in RS operator for top K selection."),
    HIVELIMITTABLESCANPARTITION("hive.limit.query.max.table.partition", -1,
        "This controls how many partitions can be scanned for each partitioned table.\n" +
        "The default value \"-1\" means no limit."),

    HIVEHASHTABLEKEYCOUNTADJUSTMENT("hive.hashtable.key.count.adjustment", 1.0f,
        "Adjustment to mapjoin hashtable size derived from table and column statistics; the estimate" +
        " of the number of keys is divided by this value. If the value is 0, statistics are not used" +
        "and hive.hashtable.initialCapacity is used instead."),
    HIVEHASHTABLETHRESHOLD("hive.hashtable.initialCapacity", 100000, "Initial capacity of " +
        "mapjoin hashtable if statistics are absent, or if hive.hashtable.stats.key.estimate.adjustment is set to 0"),
    HIVEHASHTABLELOADFACTOR("hive.hashtable.loadfactor", (float) 0.75, ""),
    HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE("hive.mapjoin.followby.gby.localtask.max.memory.usage", (float) 0.55,
        "This number means how much memory the local task can take to hold the key/value into an in-memory hash table \n" +
        "when this map join is followed by a group by. If the local task's memory usage is more than this number, \n" +
        "the local task will abort by itself. It means the data of the small table is too large to be held in memory."),
    HIVEHASHTABLEMAXMEMORYUSAGE("hive.mapjoin.localtask.max.memory.usage", (float) 0.90,
        "This number means how much memory the local task can take to hold the key/value into an in-memory hash table. \n" +
        "If the local task's memory usage is more than this number, the local task will abort by itself. \n" +
        "It means the data of the small table is too large to be held in memory."),
    HIVEHASHTABLESCALE("hive.mapjoin.check.memory.rows", (long)100000,
        "The number means after how many rows processed it needs to check the memory usage"),

    HIVEDEBUGLOCALTASK("hive.debug.localtask",false, ""),

    HIVEINPUTFORMAT("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",
        "The default input format. Set this to HiveInputFormat if you encounter problems with CombineHiveInputFormat."),
    HIVETEZINPUTFORMAT("hive.tez.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat",
        "The default input format for tez. Tez groups splits in the AM."),

    HIVETEZCONTAINERSIZE("hive.tez.container.size", -1,
        "By default Tez will spawn containers of the size of a mapper. This can be used to overwrite."),
    HIVETEZCPUVCORES("hive.tez.cpu.vcores", -1,
        "By default Tez will ask for however many cpus map-reduce is configured to use per container.\n" +
        "This can be used to overwrite."),
    HIVETEZJAVAOPTS("hive.tez.java.opts", null,
        "By default Tez will use the Java options from map tasks. This can be used to overwrite."),
    HIVETEZLOGLEVEL("hive.tez.log.level", "INFO",
        "The log level to use for tasks executing as part of the DAG.\n" +
        "Used only if hive.tez.java.opts is used to configure Java options."),

    HIVEENFORCEBUCKETING("hive.enforce.bucketing", false,
        "Whether bucketing is enforced. If true, while inserting into the table, bucketing is enforced."),
    HIVEENFORCESORTING("hive.enforce.sorting", false,
        "Whether sorting is enforced. If true, while inserting into the table, sorting is enforced."),
    HIVEOPTIMIZEBUCKETINGSORTING("hive.optimize.bucketingsorting", true,
        "If hive.enforce.bucketing or hive.enforce.sorting is true, don't create a reducer for enforcing \n" +
        "bucketing/sorting for queries of the form: \n" +
        "insert overwrite table T2 select * from T1;\n" +
        "where T1 and T2 are bucketed/sorted by the same keys into the same number of buckets."),
    HIVEPARTITIONER("hive.mapred.partitioner", "org.apache.hadoop.hive.ql.io.DefaultHivePartitioner", ""),
    HIVEENFORCESORTMERGEBUCKETMAPJOIN("hive.enforce.sortmergebucketmapjoin", false,
        "If the user asked for sort-merge bucketed map-side join, and it cannot be performed, should the query fail or not ?"),
    HIVEENFORCEBUCKETMAPJOIN("hive.enforce.bucketmapjoin", false,
        "If the user asked for bucketed map-side join, and it cannot be performed, \n" +
        "should the query fail or not ? For example, if the buckets in the tables being joined are\n" +
        "not a multiple of each other, bucketed map-side join cannot be performed, and the\n" +
        "query will fail if hive.enforce.bucketmapjoin is set to true."),

    HIVE_AUTO_SORTMERGE_JOIN("hive.auto.convert.sortmerge.join", false,
        "Will the join be automatically converted to a sort-merge join, if the joined tables pass the criteria for sort-merge join."),
    HIVE_AUTO_SORTMERGE_JOIN_BIGTABLE_SELECTOR(
        "hive.auto.convert.sortmerge.join.bigtable.selection.policy",
        "org.apache.hadoop.hive.ql.optimizer.AvgPartitionSizeBasedBigTableSelectorForAutoSMJ",
        "The policy to choose the big table for automatic conversion to sort-merge join. \n" +
        "By default, the table with the largest partitions is assigned the big table. All policies are:\n" +
        ". based on position of the table - the leftmost table is selected\n" +
        "org.apache.hadoop.hive.ql.optimizer.LeftmostBigTableSMJ.\n" +
        ". based on total size (all the partitions selected in the query) of the table \n" +
        "org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ.\n" +
        ". based on average size (all the partitions selected in the query) of the table \n" +
        "org.apache.hadoop.hive.ql.optimizer.AvgPartitionSizeBasedBigTableSelectorForAutoSMJ.\n" +
        "New policies can be added in future."),
    HIVE_AUTO_SORTMERGE_JOIN_TOMAPJOIN(
        "hive.auto.convert.sortmerge.join.to.mapjoin", false,
        "If hive.auto.convert.sortmerge.join is set to true, and a join was converted to a sort-merge join, \n" +
        "this parameter decides whether each table should be tried as a big table, and effectively a map-join should be\n" +
        "tried. That would create a conditional task with n+1 children for a n-way join (1 child for each table as the\n" +
        "big table), and the backup task will be the sort-merge join. In some cases, a map-join would be faster than a\n" +
        "sort-merge join, if there is no advantage of having the output bucketed and sorted. For example, if a very big sorted\n" +
        "and bucketed table with few files (say 10 files) are being joined with a very small sorter and bucketed table\n" +
        "with few files (10 files), the sort-merge join will only use 10 mappers, and a simple map-only join might be faster\n" +
        "if the complete small table can fit in memory, and a map-join can be performed."),

    HIVESCRIPTOPERATORTRUST("hive.exec.script.trust", false, ""),
    HIVEROWOFFSET("hive.exec.rowoffset", false,
        "Whether to provide the row offset virtual column"),

    HIVE_COMBINE_INPUT_FORMAT_SUPPORTS_SPLITTABLE("hive.hadoop.supports.splittable.combineinputformat", false, ""),

    // Optimizer
    HIVEOPTINDEXFILTER("hive.optimize.index.filter", false,
        "Whether to enable automatic use of indexes"),
    HIVEINDEXAUTOUPDATE("hive.optimize.index.autoupdate", false,
        "Whether to update stale indexes automatically"),
    HIVEOPTPPD("hive.optimize.ppd", true,
        "Whether to enable predicate pushdown"),
    HIVEPPDRECOGNIZETRANSITIVITY("hive.ppd.recognizetransivity", true,
        "Whether to transitively replicate predicate filters over equijoin conditions."),
    HIVEPPDREMOVEDUPLICATEFILTERS("hive.ppd.remove.duplicatefilters", true,
        "Whether to push predicates down into storage handlers.  Ignored when hive.optimize.ppd is false."),
    // Constant propagation optimizer
    HIVEOPTCONSTANTPROPAGATION("hive.optimize.constant.propagation", true, "Whether to enable constant propagation optimizer"),
    HIVEIDENTITYPROJECTREMOVER("hive.optimize.remove.identity.project", true, "Removes identity project from operator tree"),
    HIVEMETADATAONLYQUERIES("hive.optimize.metadataonly", true, ""),
    HIVENULLSCANOPTIMIZE("hive.optimize.null.scan", true, "Dont scan relations which are guaranteed to not generate any rows"),
    HIVEOPTPPD_STORAGE("hive.optimize.ppd.storage", true,
        "Whether to push predicates down to storage handlers"),
    HIVEOPTGROUPBY("hive.optimize.groupby", true,
        "Whether to enable the bucketed group by from bucketed partitions/tables."),
    HIVEOPTBUCKETMAPJOIN("hive.optimize.bucketmapjoin", false,
        "Whether to try bucket mapjoin"),
    HIVEOPTSORTMERGEBUCKETMAPJOIN("hive.optimize.bucketmapjoin.sortedmerge", false,
        "Whether to try sorted bucket merge map join"),
    HIVEOPTREDUCEDEDUPLICATION("hive.optimize.reducededuplication", true,
        "Remove extra map-reduce jobs if the data is already clustered by the same key which needs to be used again. \n" +
        "This should always be set to true. Since it is a new feature, it has been made configurable."),
    HIVEOPTREDUCEDEDUPLICATIONMINREDUCER("hive.optimize.reducededuplication.min.reducer", 4,
        "Reduce deduplication merges two RSs by moving key/parts/reducer-num of the child RS to parent RS. \n" +
        "That means if reducer-num of the child RS is fixed (order by or forced bucketing) and small, it can make very slow, single MR.\n" +
        "The optimization will be automatically disabled if number of reducers would be less than specified value."),

    HIVEOPTSORTDYNAMICPARTITION("hive.optimize.sort.dynamic.partition", false,
        "When enabled dynamic partitioning column will be globally sorted.\n" +
        "This way we can keep only one record writer open for each partition value\n" +
        "in the reducer thereby reducing the memory pressure on reducers."),

    HIVESAMPLINGFORORDERBY("hive.optimize.sampling.orderby", false, "Uses sampling on order-by clause for parallel execution."),
    HIVESAMPLINGNUMBERFORORDERBY("hive.optimize.sampling.orderby.number", 1000, "Total number of samples to be obtained."),
    HIVESAMPLINGPERCENTFORORDERBY("hive.optimize.sampling.orderby.percent", 0.1f, new RatioValidator(),
        "Probability with which a row will be chosen."),

    // whether to optimize union followed by select followed by filesink
    // It creates sub-directories in the final output, so should not be turned on in systems
    // where MAPREDUCE-1501 is not present
    HIVE_OPTIMIZE_UNION_REMOVE("hive.optimize.union.remove", false,
        "Whether to remove the union and push the operators between union and the filesink above union. \n" +
        "This avoids an extra scan of the output by union. This is independently useful for union\n" +
        "queries, and specially useful when hive.optimize.skewjoin.compiletime is set to true, since an\n" +
        "extra union is inserted.\n" +
        "\n" +
        "The merge is triggered if either of hive.merge.mapfiles or hive.merge.mapredfiles is set to true.\n" +
        "If the user has set hive.merge.mapfiles to true and hive.merge.mapredfiles to false, the idea was the\n" +
        "number of reducers are few, so the number of files anyway are small. However, with this optimization,\n" +
        "we are increasing the number of files possibly by a big margin. So, we merge aggressively."),
    HIVEOPTCORRELATION("hive.optimize.correlation", false, "exploit intra-query correlations."),

    HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES("hive.mapred.supports.subdirectories", false,
        "Whether the version of Hadoop which is running supports sub-directories for tables/partitions. \n" +
        "Many Hive optimizations can be applied if the Hadoop version supports sub-directories for\n" +
        "tables/partitions. It was added by MAPREDUCE-1501"),

    HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME("hive.optimize.skewjoin.compiletime", false,
        "Whether to create a separate plan for skewed keys for the tables in the join.\n" +
        "This is based on the skewed keys stored in the metadata. At compile time, the plan is broken\n" +
        "into different joins: one for the skewed keys, and the other for the remaining keys. And then,\n" +
        "a union is performed for the 2 joins generated above. So unless the same skewed key is present\n" +
        "in both the joined tables, the join for the skewed key will be performed as a map-side join.\n" +
        "\n" +
        "The main difference between this parameter and hive.optimize.skewjoin is that this parameter\n" +
        "uses the skew information stored in the metastore to optimize the plan at compile time itself.\n" +
        "If there is no skew information in the metadata, this parameter will not have any affect.\n" +
        "Both hive.optimize.skewjoin.compiletime and hive.optimize.skewjoin should be set to true.\n" +
        "Ideally, hive.optimize.skewjoin should be renamed as hive.optimize.skewjoin.runtime, but not doing\n" +
        "so for backward compatibility.\n" +
        "\n" +
        "If the skew information is correctly stored in the metadata, hive.optimize.skewjoin.compiletime\n" +
        "would change the query plan to take care of it, and hive.optimize.skewjoin will be a no-op."),

    // Indexes
    HIVEOPTINDEXFILTER_COMPACT_MINSIZE("hive.optimize.index.filter.compact.minsize", (long) 5 * 1024 * 1024 * 1024,
        "Minimum size (in bytes) of the inputs on which a compact index is automatically used."), // 5G
    HIVEOPTINDEXFILTER_COMPACT_MAXSIZE("hive.optimize.index.filter.compact.maxsize", (long) -1,
        "Maximum size (in bytes) of the inputs on which a compact index is automatically used.  A negative number is equivalent to infinity."), // infinity
    HIVE_INDEX_COMPACT_QUERY_MAX_ENTRIES("hive.index.compact.query.max.entries", (long) 10000000,
        "The maximum number of index entries to read during a query that uses the compact index. Negative value is equivalent to infinity."), // 10M
    HIVE_INDEX_COMPACT_QUERY_MAX_SIZE("hive.index.compact.query.max.size", (long) 10 * 1024 * 1024 * 1024,
        "The maximum number of bytes that a query using the compact index can read. Negative value is equivalent to infinity."), // 10G
    HIVE_INDEX_COMPACT_BINARY_SEARCH("hive.index.compact.binary.search", true,
        "Whether or not to use a binary search to find the entries in an index table that match the filter, where possible"),

    // Statistics
    HIVESTATSAUTOGATHER("hive.stats.autogather", true,
        "A flag to gather statistics automatically during the INSERT OVERWRITE command."),
    HIVESTATSDBCLASS("hive.stats.dbclass", "fs", new PatternSet("jdbc(:.*)", "hbase", "counter", "custom", "fs"),
        "The storage that stores temporary Hive statistics. In filesystem based statistics collection ('fs'), \n" +
        "each task writes statistics it has collected in a file on the filesystem, which will be aggregated \n" +
        "after the job has finished. Supported values are fs (filesystem), jdbc:database (where database \n" +
        "can be derby, mysql, etc.), hbase, counter, and custom as defined in StatsSetupConst.java."), // StatsSetupConst.StatDB
    HIVESTATSJDBCDRIVER("hive.stats.jdbcdriver",
        "org.apache.derby.jdbc.EmbeddedDriver",
        "The JDBC driver for the database that stores temporary Hive statistics."),
    HIVESTATSDBCONNECTIONSTRING("hive.stats.dbconnectionstring",
        "jdbc:derby:;databaseName=TempStatsStore;create=true",
        "The default connection string for the database that stores temporary Hive statistics."), // automatically create database
    HIVE_STATS_DEFAULT_PUBLISHER("hive.stats.default.publisher", "",
        "The Java class (implementing the StatsPublisher interface) that is used by default if hive.stats.dbclass is custom type."),
    HIVE_STATS_DEFAULT_AGGREGATOR("hive.stats.default.aggregator", "",
        "The Java class (implementing the StatsAggregator interface) that is used by default if hive.stats.dbclass is custom type."),
    HIVE_STATS_JDBC_TIMEOUT("hive.stats.jdbc.timeout", "30s", new TimeValidator(TimeUnit.SECONDS),
        "Timeout value used by JDBC connection and statements."),
    HIVE_STATS_ATOMIC("hive.stats.atomic", false,
        "whether to update metastore stats only if all stats are available"),
    HIVE_STATS_RETRIES_MAX("hive.stats.retries.max", 0,
        "Maximum number of retries when stats publisher/aggregator got an exception updating intermediate database. \n" +
        "Default is no tries on failures."),
    HIVE_STATS_RETRIES_WAIT("hive.stats.retries.wait", "3000ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "The base waiting window before the next retry. The actual wait time is calculated by " +
        "baseWindow * failures baseWindow * (failure + 1) * (random number between [0.0,1.0])."),
    HIVE_STATS_COLLECT_RAWDATASIZE("hive.stats.collect.rawdatasize", true,
        "should the raw data size be collected when analyzing tables"),
    CLIENT_STATS_COUNTERS("hive.client.stats.counters", "",
        "Subset of counters that should be of interest for hive.client.stats.publishers (when one wants to limit their publishing). \n" +
        "Non-display names should be used"),
    //Subset of counters that should be of interest for hive.client.stats.publishers (when one wants to limit their publishing). Non-display names should be used".
    HIVE_STATS_RELIABLE("hive.stats.reliable", false,
        "Whether queries will fail because stats cannot be collected completely accurately. \n" +
        "If this is set to true, reading/writing from/into a partition may fail because the stats\n" +
        "could not be computed accurately."),
    HIVE_STATS_COLLECT_PART_LEVEL_STATS("hive.analyze.stmt.collect.partlevel.stats", true,
        "analyze table T compute statistics for columns. Queries like these should compute partition"
        + "level stats for partitioned table even when no part spec is specified."),
    HIVE_STATS_GATHER_NUM_THREADS("hive.stats.gather.num.threads", 10,
        "Number of threads used by partialscan/noscan analyze command for partitioned tables.\n" +
        "This is applicable only for file formats that implement StatsProvidingRecordReader (like ORC)."),
    // Collect table access keys information for operators that can benefit from bucketing
    HIVE_STATS_COLLECT_TABLEKEYS("hive.stats.collect.tablekeys", false,
        "Whether join and group by keys on tables are derived and maintained in the QueryPlan.\n" +
        "This is useful to identify how tables are accessed and to determine if they should be bucketed."),
    // Collect column access information
    HIVE_STATS_COLLECT_SCANCOLS("hive.stats.collect.scancols", false,
        "Whether column accesses are tracked in the QueryPlan.\n" +
        "This is useful to identify how tables are accessed and to determine if there are wasted columns that can be trimmed."),
    // standard error allowed for ndv estimates. A lower value indicates higher accuracy and a
    // higher compute cost.
    HIVE_STATS_NDV_ERROR("hive.stats.ndv.error", (float)20.0,
        "Standard error expressed in percentage. Provides a tradeoff between accuracy and compute cost. \n" +
        "A lower value for error indicates higher accuracy and a higher compute cost."),
    HIVE_STATS_KEY_PREFIX_MAX_LENGTH("hive.stats.key.prefix.max.length", 150,
        "Determines if when the prefix of the key used for intermediate stats collection\n" +
        "exceeds a certain length, a hash of the key is used instead.  If the value < 0 then hashing"),
    HIVE_STATS_KEY_PREFIX_RESERVE_LENGTH("hive.stats.key.prefix.reserve.length", 24,
        "Reserved length for postfix of stats key. Currently only meaningful for counter type which should\n" +
        "keep length of full stats key smaller than max length configured by hive.stats.key.prefix.max.length.\n" +
        "For counter type, it should be bigger than the length of LB spec if exists."),
    HIVE_STATS_KEY_PREFIX("hive.stats.key.prefix", "", "", true), // internal usage only
    // if length of variable length data type cannot be determined this length will be used.
    HIVE_STATS_MAX_VARIABLE_LENGTH("hive.stats.max.variable.length", 100,
        "To estimate the size of data flowing through operators in Hive/Tez(for reducer estimation etc.),\n" +
        "average row size is multiplied with the total number of rows coming out of each operator.\n" +
        "Average row size is computed from average column size of all columns in the row. In the absence\n" +
        "of column statistics, for variable length columns (like string, bytes etc.), this value will be\n" +
        "used. For fixed length columns their corresponding Java equivalent sizes are used\n" +
        "(float - 4 bytes, double - 8 bytes etc.)."),
    // if number of elements in list cannot be determined, this value will be used
    HIVE_STATS_LIST_NUM_ENTRIES("hive.stats.list.num.entries", 10,
        "To estimate the size of data flowing through operators in Hive/Tez(for reducer estimation etc.),\n" +
        "average row size is multiplied with the total number of rows coming out of each operator.\n" +
        "Average row size is computed from average column size of all columns in the row. In the absence\n" +
        "of column statistics and for variable length complex columns like list, the average number of\n" +
        "entries/values can be specified using this config."),
    // if number of elements in map cannot be determined, this value will be used
    HIVE_STATS_MAP_NUM_ENTRIES("hive.stats.map.num.entries", 10,
        "To estimate the size of data flowing through operators in Hive/Tez(for reducer estimation etc.),\n" +
        "average row size is multiplied with the total number of rows coming out of each operator.\n" +
        "Average row size is computed from average column size of all columns in the row. In the absence\n" +
        "of column statistics and for variable length complex columns like map, the average number of\n" +
        "entries/values can be specified using this config."),
    // statistics annotation fetches stats for each partition, which can be expensive. turning
    // this off will result in basic sizes being fetched from namenode instead
    HIVE_STATS_FETCH_PARTITION_STATS("hive.stats.fetch.partition.stats", true,
        "Annotation of operator tree with statistics information requires partition level basic\n" +
        "statistics like number of rows, data size and file size. Partition statistics are fetched from\n" +
        "metastore. Fetching partition statistics for each needed partition can be expensive when the\n" +
        "number of partitions is high. This flag can be used to disable fetching of partition statistics\n" +
        "from metastore. When this flag is disabled, Hive will make calls to filesystem to get file sizes\n" +
        "and will estimate the number of rows from row schema."),
    // statistics annotation fetches column statistics for all required columns which can
    // be very expensive sometimes
    HIVE_STATS_FETCH_COLUMN_STATS("hive.stats.fetch.column.stats", false,
        "Annotation of operator tree with statistics information requires column statistics.\n" +
        "Column statistics are fetched from metastore. Fetching column statistics for each needed column\n" +
        "can be expensive when the number of columns is high. This flag can be used to disable fetching\n" +
        "of column statistics from metastore."),
    // in the absence of column statistics, the estimated number of rows/data size that will
    // be emitted from join operator will depend on this factor
    HIVE_STATS_JOIN_FACTOR("hive.stats.join.factor", (float) 1.1,
        "Hive/Tez optimizer estimates the data size flowing through each of the operators. JOIN operator\n" +
        "uses column statistics to estimate the number of rows flowing out of it and hence the data size.\n" +
        "In the absence of column statistics, this factor determines the amount of rows that flows out\n" +
        "of JOIN operator."),
    // in the absence of uncompressed/raw data size, total file size will be used for statistics
    // annotation. But the file may be compressed, encoded and serialized which may be lesser in size
    // than the actual uncompressed/raw data size. This factor will be multiplied to file size to estimate
    // the raw data size.
    HIVE_STATS_DESERIALIZATION_FACTOR("hive.stats.deserialization.factor", (float) 1.0,
        "Hive/Tez optimizer estimates the data size flowing through each of the operators. In the absence\n" +
        "of basic statistics like number of rows and data size, file size is used to estimate the number\n" +
        "of rows and data size. Since files in tables/partitions are serialized (and optionally\n" +
        "compressed) the estimates of number of rows and data size cannot be reliably determined.\n" +
        "This factor is multiplied with the file size to account for serialization and compression."),

    // Concurrency
    HIVE_SUPPORT_CONCURRENCY("hive.support.concurrency", false,
        "Whether Hive supports concurrency control or not. \n" +
        "A ZooKeeper instance must be up and running when using zookeeper Hive lock manager "),
    HIVE_LOCK_MANAGER("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager", ""),
    HIVE_LOCK_NUMRETRIES("hive.lock.numretries", 100,
        "The number of times you want to try to get all the locks"),
    HIVE_UNLOCK_NUMRETRIES("hive.unlock.numretries", 10,
        "The number of times you want to retry to do one unlock"),
    HIVE_LOCK_SLEEP_BETWEEN_RETRIES("hive.lock.sleep.between.retries", "60s",
        new TimeValidator(TimeUnit.SECONDS),
        "The sleep time between various retries"),
    HIVE_LOCK_MAPRED_ONLY("hive.lock.mapred.only.operation", false,
        "This param is to control whether or not only do lock on queries\n" +
        "that need to execute at least one mapred job."),

     // Zookeeper related configs
    HIVE_ZOOKEEPER_QUORUM("hive.zookeeper.quorum", "",
        "List of ZooKeeper servers to talk to. This is needed for: \n" +
        "1. Read/write locks - when hive.lock.manager is set to \n" +
        "org.apache.hadoop.hive.ql.lockmgr.zookeeper.ZooKeeperHiveLockManager, \n" +
        "2. When HiveServer2 supports service discovery via Zookeeper.\n" +
        "3. For delegation token storage if zookeeper store is used, if\n" +
        "hive.cluster.delegation.token.store.zookeeper.connectString is not set"),

    HIVE_ZOOKEEPER_CLIENT_PORT("hive.zookeeper.client.port", "2181",
        "The port of ZooKeeper servers to talk to.\n" +
        "If the list of Zookeeper servers specified in hive.zookeeper.quorum\n" +
        "does not contain port numbers, this value is used."),
    HIVE_ZOOKEEPER_SESSION_TIMEOUT("hive.zookeeper.session.timeout", "600000ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "ZooKeeper client's session timeout (in milliseconds). The client is disconnected, and as a result, all locks released, \n" +
        "if a heartbeat is not sent in the timeout."),
    HIVE_ZOOKEEPER_NAMESPACE("hive.zookeeper.namespace", "hive_zookeeper_namespace",
        "The parent node under which all ZooKeeper nodes are created."),
    HIVE_ZOOKEEPER_CLEAN_EXTRA_NODES("hive.zookeeper.clean.extra.nodes", false,
        "Clean extra nodes at the end of the session."),
    HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES("hive.zookeeper.connection.max.retries", 3,
        "Max number of times to retry when connecting to the ZooKeeper server."),
    HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME("hive.zookeeper.connection.basesleeptime", "1000ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Initial amount of time (in milliseconds) to wait between retries\n" +
        "when connecting to the ZooKeeper server when using ExponentialBackoffRetry policy."),

    // Transactions
    HIVE_TXN_MANAGER("hive.txn.manager",
        "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager",
        "Set to org.apache.hadoop.hive.ql.lockmgr.DbTxnManager as part of turning on Hive\n" +
        "transactions, which also requires appropriate settings for hive.compactor.initiator.on,\n" +
        "hive.compactor.worker.threads, hive.support.concurrency (true), hive.enforce.bucketing\n" +
        "(true), and hive.exec.dynamic.partition.mode (nonstrict).\n" +
        "The default DummyTxnManager replicates pre-Hive-0.13 behavior and provides\n" +
        "no transactions."),
    HIVE_TXN_TIMEOUT("hive.txn.timeout", "300s", new TimeValidator(TimeUnit.SECONDS),
        "time after which transactions are declared aborted if the client has not sent a heartbeat."),

    HIVE_TXN_MAX_OPEN_BATCH("hive.txn.max.open.batch", 1000,
        "Maximum number of transactions that can be fetched in one call to open_txns().\n" +
        "This controls how many transactions streaming agents such as Flume or Storm open\n" +
        "simultaneously. The streaming agent then writes that number of entries into a single\n" +
        "file (per Flume agent or Storm bolt). Thus increasing this value decreases the number\n" +
        "of delta files created by streaming agents. But it also increases the number of open\n" +
        "transactions that Hive has to track at any given time, which may negatively affect\n" +
        "read performance."),

    HIVE_COMPACTOR_INITIATOR_ON("hive.compactor.initiator.on", false,
        "Whether to run the initiator and cleaner threads on this metastore instance or not.\n" +
        "Set this to true on one instance of the Thrift metastore service as part of turning\n" +
        "on Hive transactions. For a complete list of parameters required for turning on\n" +
        "transactions, see hive.txn.manager."),

    HIVE_COMPACTOR_WORKER_THREADS("hive.compactor.worker.threads", 0,
        "How many compactor worker threads to run on this metastore instance. Set this to a\n" +
        "positive number on one or more instances of the Thrift metastore service as part of\n" +
        "turning on Hive transactions. For a complete list of parameters required for turning\n" +
        "on transactions, see hive.txn.manager.\n" +
        "Worker threads spawn MapReduce jobs to do compactions. They do not do the compactions\n" +
        "themselves. Increasing the number of worker threads will decrease the time it takes\n" +
        "tables or partitions to be compacted once they are determined to need compaction.\n" +
        "It will also increase the background load on the Hadoop cluster as more MapReduce jobs\n" +
        "will be running in the background."),

    HIVE_COMPACTOR_WORKER_TIMEOUT("hive.compactor.worker.timeout", "86400s",
        new TimeValidator(TimeUnit.SECONDS),
        "Time in seconds after which a compaction job will be declared failed and the\n" +
        "compaction re-queued."),

    HIVE_COMPACTOR_CHECK_INTERVAL("hive.compactor.check.interval", "300s",
        new TimeValidator(TimeUnit.SECONDS),
        "Time in seconds between checks to see if any tables or partitions need to be\n" +
        "compacted. This should be kept high because each check for compaction requires\n" +
        "many calls against the NameNode.\n" +
        "Decreasing this value will reduce the time it takes for compaction to be started\n" +
        "for a table or partition that requires compaction. However, checking if compaction\n" +
        "is needed requires several calls to the NameNode for each table or partition that\n" +
        "has had a transaction done on it since the last major compaction. So decreasing this\n" +
        "value will increase the load on the NameNode."),

    HIVE_COMPACTOR_DELTA_NUM_THRESHOLD("hive.compactor.delta.num.threshold", 10,
        "Number of delta directories in a table or partition that will trigger a minor\n" +
        "compaction."),

    HIVE_COMPACTOR_DELTA_PCT_THRESHOLD("hive.compactor.delta.pct.threshold", 0.1f,
        "Percentage (fractional) size of the delta files relative to the base that will trigger\n" +
        "a major compaction. (1.0 = 100%, so the default 0.1 = 10%.)"),

    HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD("hive.compactor.abortedtxn.threshold", 1000,
        "Number of aborted transactions involving a given table or partition that will trigger\n" +
        "a major compaction."),

    HIVE_COMPACTOR_CLEANER_RUN_INTERVAL("hive.compactor.cleaner.run.interval", "5000ms",
        new TimeValidator(TimeUnit.MILLISECONDS), "Time between runs of the cleaner thread"),

    // For HBase storage handler
    HIVE_HBASE_WAL_ENABLED("hive.hbase.wal.enabled", true,
        "Whether writes to HBase should be forced to the write-ahead log. \n" +
        "Disabling this improves HBase write performance at the risk of lost writes in case of a crash."),
    HIVE_HBASE_GENERATE_HFILES("hive.hbase.generatehfiles", false,
        "True when HBaseStorageHandler should generate hfiles instead of operate against the online table."),
    HIVE_HBASE_SNAPSHOT_NAME("hive.hbase.snapshot.name", null, "The HBase table snapshot name to use."),
    HIVE_HBASE_SNAPSHOT_RESTORE_DIR("hive.hbase.snapshot.restoredir", "/tmp", "The directory in which to " +
        "restore the HBase table snapshot."),

    // For har files
    HIVEARCHIVEENABLED("hive.archive.enabled", false, "Whether archiving operations are permitted"),

    HIVEOPTGBYUSINGINDEX("hive.optimize.index.groupby", false,
        "Whether to enable optimization of group-by queries using Aggregate indexes."),

    HIVEOUTERJOINSUPPORTSFILTERS("hive.outerjoin.supports.filters", true, ""),

    HIVEFETCHTASKCONVERSION("hive.fetch.task.conversion", "more", new StringSet("none", "minimal", "more"),
        "Some select queries can be converted to single FETCH task minimizing latency.\n" +
        "Currently the query should be single sourced not having any subquery and should not have\n" +
        "any aggregations or distincts (which incurs RS), lateral views and joins.\n" +
        "0. none : disable hive.fetch.task.conversion\n" +
        "1. minimal : SELECT STAR, FILTER on partition columns, LIMIT only\n" +
        "2. more    : SELECT, FILTER, LIMIT only (support TABLESAMPLE and virtual columns)"
    ),
    HIVEFETCHTASKCONVERSIONTHRESHOLD("hive.fetch.task.conversion.threshold", 1073741824L,
        "Input threshold for applying hive.fetch.task.conversion. If target table is native, input length\n" +
        "is calculated by summation of file lengths. If it's not native, storage handler for the table\n" +
        "can optionally implement org.apache.hadoop.hive.ql.metadata.InputEstimator interface."),

    HIVEFETCHTASKAGGR("hive.fetch.task.aggr", false,
        "Aggregation queries with no group-by clause (for example, select count(*) from src) execute\n" +
        "final aggregations in single reduce task. If this is set true, Hive delegates final aggregation\n" +
        "stage to fetch task, possibly decreasing the query time."),

    HIVEOPTIMIZEMETADATAQUERIES("hive.compute.query.using.stats", false,
        "When set to true Hive will answer a few queries like count(1) purely using stats\n" +
        "stored in metastore. For basic stats collection turn on the config hive.stats.autogather to true.\n" +
        "For more advanced stats collection need to run analyze table queries."),

    // Serde for FetchTask
    HIVEFETCHOUTPUTSERDE("hive.fetch.output.serde", "org.apache.hadoop.hive.serde2.DelimitedJSONSerDe",
        "The SerDe used by FetchTask to serialize the fetch output."),

    HIVEEXPREVALUATIONCACHE("hive.cache.expr.evaluation", true,
        "If true, the evaluation result of a deterministic expression referenced twice or more\n" +
        "will be cached.\n" +
        "For example, in a filter condition like '.. where key + 10 = 100 or key + 10 = 0'\n" +
        "the expression 'key + 10' will be evaluated/cached once and reused for the following\n" +
        "expression ('key + 10 = 0'). Currently, this is applied only to expressions in select\n" +
        "or filter operators."),

    // Hive Variables
    HIVEVARIABLESUBSTITUTE("hive.variable.substitute", true,
        "This enables substitution using syntax like ${var} ${system:var} and ${env:var}."),
    HIVEVARIABLESUBSTITUTEDEPTH("hive.variable.substitute.depth", 40,
        "The maximum replacements the substitution engine will do."),

    HIVECONFVALIDATION("hive.conf.validation", true,
        "Enables type checking for registered Hive configurations"),

    SEMANTIC_ANALYZER_HOOK("hive.semantic.analyzer.hook", "", ""),
    HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE(
        "hive.test.authz.sstd.hs2.mode", false, "test hs2 mode from .q tests", true),
    HIVE_AUTHORIZATION_ENABLED("hive.security.authorization.enabled", false,
        "enable or disable the Hive client authorization"),
    HIVE_AUTHORIZATION_MANAGER("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider",
        "The Hive client authorization manager class name. The user defined authorization class should implement \n" +
        "interface org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider."),
    HIVE_AUTHENTICATOR_MANAGER("hive.security.authenticator.manager",
        "org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator",
        "hive client authenticator manager class name. The user defined authenticator should implement \n" +
        "interface org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider."),
    HIVE_METASTORE_AUTHORIZATION_MANAGER("hive.security.metastore.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.DefaultHiveMetastoreAuthorizationProvider",
        "Names of authorization manager classes (comma separated) to be used in the metastore\n" +
        "for authorization. The user defined authorization class should implement interface\n" +
        "org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider.\n" +
        "All authorization manager classes have to successfully authorize the metastore API\n" +
        "call for the command execution to be allowed."),
    HIVE_METASTORE_AUTHORIZATION_AUTH_READS("hive.security.metastore.authorization.auth.reads", true,
        "If this is true, metastore authorizer authorizes read actions on database, table"),
    HIVE_METASTORE_AUTHENTICATOR_MANAGER("hive.security.metastore.authenticator.manager",
        "org.apache.hadoop.hive.ql.security.HadoopDefaultMetastoreAuthenticator",
        "authenticator manager class name to be used in the metastore for authentication. \n" +
        "The user defined authenticator should implement interface org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider."),
    HIVE_AUTHORIZATION_TABLE_USER_GRANTS("hive.security.authorization.createtable.user.grants", "",
        "the privileges automatically granted to some users whenever a table gets created.\n" +
        "An example like \"userX,userY:select;userZ:create\" will grant select privilege to userX and userY,\n" +
        "and grant create privilege to userZ whenever a new table created."),
    HIVE_AUTHORIZATION_TABLE_GROUP_GRANTS("hive.security.authorization.createtable.group.grants",
        "",
        "the privileges automatically granted to some groups whenever a table gets created.\n" +
        "An example like \"groupX,groupY:select;groupZ:create\" will grant select privilege to groupX and groupY,\n" +
        "and grant create privilege to groupZ whenever a new table created."),
    HIVE_AUTHORIZATION_TABLE_ROLE_GRANTS("hive.security.authorization.createtable.role.grants", "",
        "the privileges automatically granted to some roles whenever a table gets created.\n" +
        "An example like \"roleX,roleY:select;roleZ:create\" will grant select privilege to roleX and roleY,\n" +
        "and grant create privilege to roleZ whenever a new table created."),
    HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS("hive.security.authorization.createtable.owner.grants",
        "",
        "The privileges automatically granted to the owner whenever a table gets created.\n" +
        "An example like \"select,drop\" will grant select and drop privilege to the owner\n" +
        "of the table. Note that the default gives the creator of a table no access to the\n" +
        "table (but see HIVE-8067)."),
    HIVE_AUTHORIZATION_TASK_FACTORY("hive.security.authorization.task.factory",
        "org.apache.hadoop.hive.ql.parse.authorization.RestrictedHiveAuthorizationTaskFactoryImpl",
        "Authorization DDL task factory implementation"),

    // if this is not set default value is set during config initialization
    // Default value can't be set in this constructor as it would refer names in other ConfVars
    // whose constructor would not have been called
    HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST(
        "hive.security.authorization.sqlstd.confwhitelist", "",
        "List of comma separated Java regexes. Configurations parameters that match these\n" +
        "regexes can be modified by user when SQL standard authorization is enabled.\n" +
        "To get the default value, use the 'set <param>' command.\n" +
        "Note that the hive.conf.restricted.list checks are still enforced after the white list\n" +
        "check"),

    HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST_APPEND(
        "hive.security.authorization.sqlstd.confwhitelist.append", "",
        "List of comma separated Java regexes, to be appended to list set in\n" +
        "hive.security.authorization.sqlstd.confwhitelist. Using this list instead\n" +
        "of updating the original list means that you can append to the defaults\n" +
        "set by SQL standard authorization instead of replacing it entirely."),

    HIVE_CLI_PRINT_HEADER("hive.cli.print.header", false, "Whether to print the names of the columns in query output."),

    HIVE_ERROR_ON_EMPTY_PARTITION("hive.error.on.empty.partition", false,
        "Whether to throw an exception if dynamic partition insert generates empty results."),

    HIVE_INDEX_COMPACT_FILE("hive.index.compact.file", "", "internal variable"),
    HIVE_INDEX_BLOCKFILTER_FILE("hive.index.blockfilter.file", "", "internal variable"),
    HIVE_INDEX_IGNORE_HDFS_LOC("hive.index.compact.file.ignore.hdfs", false,
        "When true the HDFS location stored in the index file will be ignored at runtime.\n" +
        "If the data got moved or the name of the cluster got changed, the index data should still be usable."),

    HIVE_EXIM_URI_SCHEME_WL("hive.exim.uri.scheme.whitelist", "hdfs,pfile",
        "A comma separated list of acceptable URI schemes for import and export."),
    // temporary variable for testing. This is added just to turn off this feature in case of a bug in
    // deployment. It has not been documented in hive-default.xml intentionally, this should be removed
    // once the feature is stable
    HIVE_MAPPER_CANNOT_SPAN_MULTIPLE_PARTITIONS("hive.mapper.cannot.span.multiple.partitions", false, ""),
    HIVE_REWORK_MAPREDWORK("hive.rework.mapredwork", false,
        "should rework the mapred work or not.\n" +
        "This is first introduced by SymlinkTextInputFormat to replace symlink files with real paths at compile time."),
    HIVE_CONCATENATE_CHECK_INDEX ("hive.exec.concatenate.check.index", true,
        "If this is set to true, Hive will throw error when doing\n" +
        "'alter table tbl_name [partSpec] concatenate' on a table/partition\n" +
        "that has indexes on it. The reason the user want to set this to true\n" +
        "is because it can help user to avoid handling all index drop, recreation,\n" +
        "rebuild work. This is very helpful for tables with thousands of partitions."),
    HIVE_IO_EXCEPTION_HANDLERS("hive.io.exception.handlers", "",
        "A list of io exception handler class names. This is used\n" +
        "to construct a list exception handlers to handle exceptions thrown\n" +
        "by record readers"),

    // operation log configuration
    HIVE_SERVER2_LOGGING_OPERATION_ENABLED("hive.server2.logging.operation.enabled", true,
        "When true, HS2 will save operation logs and make them available for clients"),
    HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION("hive.server2.logging.operation.log.location",
        "${system:java.io.tmpdir}" + File.separator + "${system:user.name}" + File.separator +
            "operation_logs",
        "Top level directory where operation logs are stored if logging functionality is enabled"),
    HIVE_SERVER2_LOGGING_OPERATION_VERBOSE("hive.server2.logging.operation.verbose", false,
            "When true, HS2 operation logs available for clients will be verbose"),
    // logging configuration
    HIVE_LOG4J_FILE("hive.log4j.file", "",
        "Hive log4j configuration file.\n" +
        "If the property is not set, then logging will be initialized using hive-log4j.properties found on the classpath.\n" +
        "If the property is set, the value must be a valid URI (java.net.URI, e.g. \"file:///tmp/my-logging.properties\"), \n" +
        "which you can then extract a URL from and pass to PropertyConfigurator.configure(URL)."),
    HIVE_EXEC_LOG4J_FILE("hive.exec.log4j.file", "",
        "Hive log4j configuration file for execution mode(sub command).\n" +
        "If the property is not set, then logging will be initialized using hive-exec-log4j.properties found on the classpath.\n" +
        "If the property is set, the value must be a valid URI (java.net.URI, e.g. \"file:///tmp/my-logging.properties\"), \n" +
        "which you can then extract a URL from and pass to PropertyConfigurator.configure(URL)."),

    HIVE_LOG_EXPLAIN_OUTPUT("hive.log.explain.output", false,
        "Whether to log explain output for every query.\n" +
        "When enabled, will log EXPLAIN EXTENDED output for the query at INFO log4j log level."),

    // prefix used to auto generated column aliases (this should be started with '_')
    HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL("hive.autogen.columnalias.prefix.label", "_c",
        "String used as a prefix when auto generating column alias.\n" +
        "By default the prefix label will be appended with a column position number to form the column alias. \n" +
        "Auto generation would happen if an aggregate function is used in a select clause without an explicit alias."),
    HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME(
        "hive.autogen.columnalias.prefix.includefuncname", false,
        "Whether to include function name in the column alias auto generated by Hive."),

    HIVE_PERF_LOGGER("hive.exec.perf.logger", "org.apache.hadoop.hive.ql.log.PerfLogger",
        "The class responsible for logging client side performance metrics. \n" +
        "Must be a subclass of org.apache.hadoop.hive.ql.log.PerfLogger"),
    HIVE_START_CLEANUP_SCRATCHDIR("hive.start.cleanup.scratchdir", false,
        "To cleanup the Hive scratchdir when starting the Hive Server"),
    HIVE_INSERT_INTO_MULTILEVEL_DIRS("hive.insert.into.multilevel.dirs", false,
        "Where to insert into multilevel directories like\n" +
        "\"insert directory '/HIVEFT25686/chinna/' from table\""),
    HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS("hive.warehouse.subdir.inherit.perms", true,
        "Set this to false if the table directories should be created\n" +
        "with the permissions derived from dfs umask instead of\n" +
        "inheriting the permission of the warehouse or database directory."),
    HIVE_INSERT_INTO_EXTERNAL_TABLES("hive.insert.into.external.tables", true,
        "whether insert into external tables is allowed"),

    HIVE_DRIVER_RUN_HOOKS("hive.exec.driver.run.hooks", "",
        "A comma separated list of hooks which implement HiveDriverRunHook. Will be run at the beginning " +
        "and end of Driver.run, these will be run in the order specified."),
    HIVE_DDL_OUTPUT_FORMAT("hive.ddl.output.format", null,
        "The data format to use for DDL output.  One of \"text\" (for human\n" +
        "readable text) or \"json\" (for a json object)."),
    HIVE_ENTITY_SEPARATOR("hive.entity.separator", "@",
        "Separator used to construct names of tables and partitions. For example, dbname@tablename@partitionname"),
    HIVE_CAPTURE_TRANSFORM_ENTITY("hive.entity.capture.transform", false,
        "Compiler to capture transform URI referred in the query"),
    HIVE_DISPLAY_PARTITION_COLUMNS_SEPARATELY("hive.display.partition.cols.separately", true,
        "In older Hive version (0.10 and earlier) no distinction was made between\n" +
        "partition columns or non-partition columns while displaying columns in describe\n" +
        "table. From 0.12 onwards, they are displayed separately. This flag will let you\n" +
        "get old behavior, if desired. See, test-case in patch for HIVE-6689."),

    HIVE_SSL_PROTOCOL_BLACKLIST("hive.ssl.protocol.blacklist", "SSLv2,SSLv3",
        "SSL Versions to disable for all Hive Servers"),

     // HiveServer2 specific configs
    HIVE_SERVER2_MAX_START_ATTEMPTS("hive.server2.max.start.attempts", 30L, new RangeValidator(0L, null),
        "Number of times HiveServer2 will attempt to start before exiting, sleeping 60 seconds " +
        "between retries. \n The default of 30 will keep trying for 30 minutes."),
    HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY("hive.server2.support.dynamic.service.discovery", false,
        "Whether HiveServer2 supports dynamic service discovery for its clients. " +
        "To support this, each instance of HiveServer2 currently uses ZooKeeper to register itself, " +
        "when it is brought up. JDBC/ODBC clients should use the ZooKeeper ensemble: " +
        "hive.zookeeper.quorum in their connection string."),
    HIVE_SERVER2_ZOOKEEPER_NAMESPACE("hive.server2.zookeeper.namespace", "hiveserver2",
        "The parent node in ZooKeeper used by HiveServer2 when supporting dynamic service discovery."),
    // HiveServer2 global init file location
    HIVE_SERVER2_GLOBAL_INIT_FILE_LOCATION("hive.server2.global.init.file.location", "${env:HIVE_CONF_DIR}",
        "Either the location of a HS2 global init file or a directory containing a .hiverc file. If the \n" +
        "property is set, the value must be a valid path to an init file or directory where the init file is located."),
    HIVE_SERVER2_TRANSPORT_MODE("hive.server2.transport.mode", "binary", new StringSet("binary", "http"),
        "Transport mode of HiveServer2."),
    HIVE_SERVER2_THRIFT_BIND_HOST("hive.server2.thrift.bind.host", "",
        "Bind host on which to run the HiveServer2 Thrift service."),

    // http (over thrift) transport settings
    HIVE_SERVER2_THRIFT_HTTP_PORT("hive.server2.thrift.http.port", 10001,
        "Port number of HiveServer2 Thrift interface when hive.server2.transport.mode is 'http'."),
    HIVE_SERVER2_THRIFT_HTTP_PATH("hive.server2.thrift.http.path", "cliservice",
        "Path component of URL endpoint when in HTTP mode."),
    HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE("hive.server2.thrift.max.message.size", 100*1024*1024,
        "Maximum message size in bytes a HS2 server will accept."),
    HIVE_SERVER2_THRIFT_HTTP_MIN_WORKER_THREADS("hive.server2.thrift.http.min.worker.threads", 5,
        "Minimum number of worker threads when in HTTP mode."),
    HIVE_SERVER2_THRIFT_HTTP_MAX_WORKER_THREADS("hive.server2.thrift.http.max.worker.threads", 500,
        "Maximum number of worker threads when in HTTP mode."),
    HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME("hive.server2.thrift.http.max.idle.time", "1800s",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Maximum idle time for a connection on the server when in HTTP mode."),
    HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME("hive.server2.thrift.http.worker.keepalive.time", "60s",
        new TimeValidator(TimeUnit.SECONDS),
        "Keepalive time for an idle http worker thread. When the number of workers exceeds min workers, " +
        "excessive threads are killed after this time interval."),

    // binary transport settings
    HIVE_SERVER2_THRIFT_PORT("hive.server2.thrift.port", 10000,
        "Port number of HiveServer2 Thrift interface when hive.server2.transport.mode is 'binary'."),
    HIVE_SERVER2_THRIFT_SASL_QOP("hive.server2.thrift.sasl.qop", "auth",
        new StringSet("auth", "auth-int", "auth-conf"),
        "Sasl QOP value; set it to one of following values to enable higher levels of\n" +
        "protection for HiveServer2 communication with clients.\n" +
        "Setting hadoop.rpc.protection to a higher level than HiveServer2 does not\n" +
        "make sense in most situations. HiveServer2 ignores hadoop.rpc.protection in favor\n" +
        "of hive.server2.thrift.sasl.qop.\n" +
        "  \"auth\" - authentication only (default)\n" +
        "  \"auth-int\" - authentication plus integrity protection\n" +
        "  \"auth-conf\" - authentication plus integrity and confidentiality protection\n" +
        "This is applicable only if HiveServer2 is configured to use Kerberos authentication."),
    HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS("hive.server2.thrift.min.worker.threads", 5,
        "Minimum number of Thrift worker threads"),
    HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS("hive.server2.thrift.max.worker.threads", 500,
        "Maximum number of Thrift worker threads"),
    HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH(
        "hive.server2.thrift.exponential.backoff.slot.length", "100ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Binary exponential backoff slot time for Thrift clients during login to HiveServer2,\n" +
        "for retries until hitting Thrift client timeout"),
    HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT("hive.server2.thrift.login.timeout", "20s",
        new TimeValidator(TimeUnit.SECONDS), "Timeout for Thrift clients during login to HiveServer2"),
    HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME("hive.server2.thrift.worker.keepalive.time", "60s",
        new TimeValidator(TimeUnit.SECONDS),
        "Keepalive time (in seconds) for an idle worker thread. When the number of workers exceeds min workers, " +
        "excessive threads are killed after this time interval."),
    // Configuration for async thread pool in SessionManager
    HIVE_SERVER2_ASYNC_EXEC_THREADS("hive.server2.async.exec.threads", 100,
        "Number of threads in the async thread pool for HiveServer2"),
    HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT("hive.server2.async.exec.shutdown.timeout", "10s",
        new TimeValidator(TimeUnit.SECONDS),
        "How long HiveServer2 shutdown will wait for async threads to terminate."),
    HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE("hive.server2.async.exec.wait.queue.size", 100,
        "Size of the wait queue for async thread pool in HiveServer2.\n" +
        "After hitting this limit, the async thread pool will reject new requests."),
    HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME("hive.server2.async.exec.keepalive.time", "10s",
        new TimeValidator(TimeUnit.SECONDS),
        "Time that an idle HiveServer2 async thread (from the thread pool) will wait for a new task\n" +
        "to arrive before terminating"),
    HIVE_SERVER2_LONG_POLLING_TIMEOUT("hive.server2.long.polling.timeout", "5000ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Time that HiveServer2 will wait before responding to asynchronous calls that use long polling"),

    // HiveServer2 auth configuration
    HIVE_SERVER2_AUTHENTICATION("hive.server2.authentication", "NONE",
      new StringSet("NOSASL", "NONE", "LDAP", "KERBEROS", "PAM", "CUSTOM"),
        "Client authentication types.\n" +
        "  NONE: no authentication check\n" +
        "  LDAP: LDAP/AD based authentication\n" +
        "  KERBEROS: Kerberos/GSSAPI authentication\n" +
        "  CUSTOM: Custom authentication provider\n" +
        "          (Use with property hive.server2.custom.authentication.class)\n" +
        "  PAM: Pluggable authentication module\n" +
        "  NOSASL:  Raw transport"),
    HIVE_SERVER2_ALLOW_USER_SUBSTITUTION("hive.server2.allow.user.substitution", true,
        "Allow alternate user to be specified as part of HiveServer2 open connection request."),
    HIVE_SERVER2_KERBEROS_KEYTAB("hive.server2.authentication.kerberos.keytab", "",
        "Kerberos keytab file for server principal"),
    HIVE_SERVER2_KERBEROS_PRINCIPAL("hive.server2.authentication.kerberos.principal", "",
        "Kerberos server principal"),
    HIVE_SERVER2_SPNEGO_KEYTAB("hive.server2.authentication.spnego.keytab", "",
        "keytab file for SPNego principal, optional,\n" +
        "typical value would look like /etc/security/keytabs/spnego.service.keytab,\n" +
        "This keytab would be used by HiveServer2 when Kerberos security is enabled and \n" +
        "HTTP transport mode is used.\n" +
        "This needs to be set only if SPNEGO is to be used in authentication.\n" +
        "SPNego authentication would be honored only if valid\n" +
        "  hive.server2.authentication.spnego.principal\n" +
        "and\n" +
        "  hive.server2.authentication.spnego.keytab\n" +
        "are specified."),
    HIVE_SERVER2_SPNEGO_PRINCIPAL("hive.server2.authentication.spnego.principal", "",
        "SPNego service principal, optional,\n" +
        "typical value would look like HTTP/_HOST@EXAMPLE.COM\n" +
        "SPNego service principal would be used by HiveServer2 when Kerberos security is enabled\n" +
        "and HTTP transport mode is used.\n" +
        "This needs to be set only if SPNEGO is to be used in authentication."),
    HIVE_SERVER2_PLAIN_LDAP_URL("hive.server2.authentication.ldap.url", null,
        "LDAP connection URL(s),\n" +
         "this value could contain URLs to mutiple LDAP servers instances for HA,\n" +
         "each LDAP URL is separated by a SPACE character. URLs are used in the \n" +
         " order specified until a connection is successful."),
    HIVE_SERVER2_PLAIN_LDAP_BASEDN("hive.server2.authentication.ldap.baseDN", null, "LDAP base DN"),
    HIVE_SERVER2_PLAIN_LDAP_DOMAIN("hive.server2.authentication.ldap.Domain", null, ""),
    HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS("hive.server2.custom.authentication.class", null,
        "Custom authentication class. Used when property\n" +
        "'hive.server2.authentication' is set to 'CUSTOM'. Provided class\n" +
        "must be a proper implementation of the interface\n" +
        "org.apache.hive.service.auth.PasswdAuthenticationProvider. HiveServer2\n" +
        "will call its Authenticate(user, passed) method to authenticate requests.\n" +
        "The implementation may optionally implement Hadoop's\n" +
        "org.apache.hadoop.conf.Configurable class to grab Hive's Configuration object."),
    HIVE_SERVER2_PAM_SERVICES("hive.server2.authentication.pam.services", null,
      "List of the underlying pam services that should be used when auth type is PAM\n" +
      "A file with the same name must exist in /etc/pam.d"),

    HIVE_SERVER2_ENABLE_DOAS("hive.server2.enable.doAs", true,
        "Setting this property to true will have HiveServer2 execute\n" +
        "Hive operations as the user making the calls to it."),
    HIVE_SERVER2_TABLE_TYPE_MAPPING("hive.server2.table.type.mapping", "CLASSIC", new StringSet("CLASSIC", "HIVE"),
        "This setting reflects how HiveServer2 will report the table types for JDBC and other\n" +
        "client implementations that retrieve the available tables and supported table types\n" +
        "  HIVE : Exposes Hive's native table types like MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW\n" +
        "  CLASSIC : More generic types like TABLE and VIEW"),
    HIVE_SERVER2_SESSION_HOOK("hive.server2.session.hook", "", ""),
    HIVE_SERVER2_USE_SSL("hive.server2.use.SSL", false,
        "Set this to true for using SSL encryption in HiveServer2."),
    HIVE_SERVER2_SSL_KEYSTORE_PATH("hive.server2.keystore.path", "",
        "SSL certificate keystore location."),
    HIVE_SERVER2_SSL_KEYSTORE_PASSWORD("hive.server2.keystore.password", "",
        "SSL certificate keystore password."),
    HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE("hive.server2.map.fair.scheduler.queue", true,
        "If the YARN fair scheduler is configured and HiveServer2 is running in non-impersonation mode,\n" +
        "this setting determines the user for fair scheduler queue mapping.\n" +
        "If set to true (default), the logged-in user determines the fair scheduler queue\n" +
        "for submitted jobs, so that map reduce resource usage can be tracked by user.\n" +
        "If set to false, all Hive jobs go to the 'hive' user's queue."),
    HIVE_SERVER2_BUILTIN_UDF_WHITELIST("hive.server2.builtin.udf.whitelist", "",
        "Comma separated list of builtin udf names allowed in queries.\n" +
        "An empty whitelist allows all builtin udfs to be executed. " +
        " The udf black list takes precedence over udf white list"),
    HIVE_SERVER2_BUILTIN_UDF_BLACKLIST("hive.server2.builtin.udf.blacklist", "",
         "Comma separated list of udfs names. These udfs will not be allowed in queries." +
         " The udf black list takes precedence over udf white list"),

    HIVE_SECURITY_COMMAND_WHITELIST("hive.security.command.whitelist", "set,reset,dfs,add,list,delete,reload,compile",
        "Comma separated list of non-SQL Hive commands users are authorized to execute"),

    HIVE_SERVER2_SESSION_CHECK_INTERVAL("hive.server2.session.check.interval", "0ms",
        new TimeValidator(TimeUnit.MILLISECONDS, 3000l, true, null, false),
        "The check interval for session/operation timeout, which can be disabled by setting to zero or negative value."),
    HIVE_SERVER2_IDLE_SESSION_TIMEOUT("hive.server2.idle.session.timeout", "0ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Session will be closed when it's not accessed for this duration, which can be disabled by setting to zero or negative value."),
    HIVE_SERVER2_IDLE_OPERATION_TIMEOUT("hive.server2.idle.operation.timeout", "0ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Operation will be closed when it's not accessed for this duration of time, which can be disabled by setting to zero value.\n" +
        "  With positive value, it's checked for operations in terminal state only (FINISHED, CANCELED, CLOSED, ERROR).\n" +
        "  With negative value, it's checked for all of the operations regardless of state."),
    HIVE_SERVER2_IDLE_SESSION_CHECK_OPERATION("hive.server2.idle.session.check.operation", false,
        "Session will be considered to be idle only if there is no activity, and there is no pending operation.\n" +
        "This setting takes effect only if session idle timeout (hive.server2.idle.session.timeout) and checking\n" +
        "(hive.server2.session.check.interval) are enabled."),

    HIVE_CONF_RESTRICTED_LIST("hive.conf.restricted.list",
        "hive.security.authenticator.manager,hive.security.authorization.manager,hive.users.in.admin.role",
        "Comma separated list of configuration options which are immutable at runtime"),

    // If this is set all move tasks at the end of a multi-insert query will only begin once all
    // outputs are ready
    HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES(
        "hive.multi.insert.move.tasks.share.dependencies", false,
        "If this is set all move tasks for tables/partitions (not directories) at the end of a\n" +
        "multi-insert query will only begin once the dependencies for all these move tasks have been\n" +
        "met.\n" +
        "Advantages: If concurrency is enabled, the locks will only be released once the query has\n" +
        "            finished, so with this config enabled, the time when the table/partition is\n" +
        "            generated will be much closer to when the lock on it is released.\n" +
        "Disadvantages: If concurrency is not enabled, with this disabled, the tables/partitions which\n" +
        "               are produced by this query and finish earlier will be available for querying\n" +
        "               much earlier.  Since the locks are only released once the query finishes, this\n" +
        "               does not apply if concurrency is enabled."),

    HIVE_INFER_BUCKET_SORT("hive.exec.infer.bucket.sort", false,
        "If this is set, when writing partitions, the metadata will include the bucketing/sorting\n" +
        "properties with which the data was written if any (this will not overwrite the metadata\n" +
        "inherited from the table if the table is bucketed/sorted)"),

    HIVE_INFER_BUCKET_SORT_NUM_BUCKETS_POWER_TWO(
        "hive.exec.infer.bucket.sort.num.buckets.power.two", false,
        "If this is set, when setting the number of reducers for the map reduce task which writes the\n" +
        "final output files, it will choose a number which is a power of two, unless the user specifies\n" +
        "the number of reducers to use using mapred.reduce.tasks.  The number of reducers\n" +
        "may be set to a power of two, only to be followed by a merge task meaning preventing\n" +
        "anything from being inferred.\n" +
        "With hive.exec.infer.bucket.sort set to true:\n" +
        "Advantages:  If this is not set, the number of buckets for partitions will seem arbitrary,\n" +
        "             which means that the number of mappers used for optimized joins, for example, will\n" +
        "             be very low.  With this set, since the number of buckets used for any partition is\n" +
        "             a power of two, the number of mappers used for optimized joins will be the least\n" +
        "             number of buckets used by any partition being joined.\n" +
        "Disadvantages: This may mean a much larger or much smaller number of reducers being used in the\n" +
        "               final map reduce job, e.g. if a job was originally going to take 257 reducers,\n" +
        "               it will now take 512 reducers, similarly if the max number of reducers is 511,\n" +
        "               and a job was going to use this many, it will now use 256 reducers."),

    HIVEOPTLISTBUCKETING("hive.optimize.listbucketing", false,
        "Enable list bucketing optimizer. Default value is false so that we disable it by default."),

    // Allow TCP Keep alive socket option for for HiveServer or a maximum timeout for the socket.
    SERVER_READ_SOCKET_TIMEOUT("hive.server.read.socket.timeout", "10s",
        new TimeValidator(TimeUnit.SECONDS),
        "Timeout for the HiveServer to close the connection if no response from the client. By default, 10 seconds."),
    SERVER_TCP_KEEP_ALIVE("hive.server.tcp.keepalive", true,
        "Whether to enable TCP keepalive for the Hive Server. Keepalive will prevent accumulation of half-open connections."),

    HIVE_DECODE_PARTITION_NAME("hive.decode.partition.name", false,
        "Whether to show the unquoted partition names in query results."),

    HIVE_EXECUTION_ENGINE("hive.execution.engine", "mr", new StringSet("mr", "tez", "spark"),
        "Chooses execution engine. Options are: mr (Map reduce, default), tez (hadoop 2 only), spark"),
    HIVE_JAR_DIRECTORY("hive.jar.directory", null,
        "This is the location hive in tez mode will look for to find a site wide \n" +
        "installed hive instance."),
    HIVE_USER_INSTALL_DIR("hive.user.install.directory", "hdfs:///user/",
        "If hive (in tez mode only) cannot find a usable hive jar in \"hive.jar.directory\", \n" +
        "it will upload the hive jar to \"hive.user.install.directory/user.name\"\n" +
        "and use it to run queries."),

    // Vectorization enabled
    HIVE_VECTORIZATION_ENABLED("hive.vectorized.execution.enabled", false,
        "This flag should be set to true to enable vectorized mode of query execution.\n" +
        "The default value is false."),
    HIVE_VECTORIZATION_REDUCE_ENABLED("hive.vectorized.execution.reduce.enabled", true,
            "This flag should be set to true to enable vectorized mode of the reduce-side of query execution.\n" +
            "The default value is true."),
    HIVE_VECTORIZATION_REDUCE_GROUPBY_ENABLED("hive.vectorized.execution.reduce.groupby.enabled", true,
            "This flag should be set to true to enable vectorized mode of the reduce-side GROUP BY query execution.\n" +
            "The default value is true."),
    HIVE_VECTORIZATION_GROUPBY_CHECKINTERVAL("hive.vectorized.groupby.checkinterval", 100000,
        "Number of entries added to the group by aggregation hash before a recomputation of average entry size is performed."),
    HIVE_VECTORIZATION_GROUPBY_MAXENTRIES("hive.vectorized.groupby.maxentries", 1000000,
        "Max number of entries in the vector group by aggregation hashtables. \n" +
        "Exceeding this will trigger a flush irrelevant of memory pressure condition."),
    HIVE_VECTORIZATION_GROUPBY_FLUSH_PERCENT("hive.vectorized.groupby.flush.percent", (float) 0.1,
        "Percent of entries in the group by aggregation hash flushed when the memory threshold is exceeded."),

    HIVE_TYPE_CHECK_ON_INSERT("hive.typecheck.on.insert", true, ""),
    HIVE_HADOOP_CLASSPATH("hive.hadoop.classpath", null,
        "For Windows OS, we need to pass HIVE_HADOOP_CLASSPATH Java parameter while starting HiveServer2 \n" +
        "using \"-hiveconf hive.hadoop.classpath=%HIVE_LIB%\"."),

    HIVE_RPC_QUERY_PLAN("hive.rpc.query.plan", false,
        "Whether to send the query plan via local resource or RPC"),
    HIVE_AM_SPLIT_GENERATION("hive.compute.splits.in.am", true,
        "Whether to generate the splits locally or in the AM (tez only)"),

    HIVE_PREWARM_ENABLED("hive.prewarm.enabled", false, "Enables container prewarm for Tez (Hadoop 2 only)"),
    HIVE_PREWARM_NUM_CONTAINERS("hive.prewarm.numcontainers", 10, "Controls the number of containers to prewarm for Tez (Hadoop 2 only)"),

    HIVESTAGEIDREARRANGE("hive.stageid.rearrange", "none", new StringSet("none", "idonly", "traverse", "execution"), ""),
    HIVEEXPLAINDEPENDENCYAPPENDTASKTYPES("hive.explain.dependency.append.tasktype", false, ""),

    HIVECOUNTERGROUP("hive.counters.group.name", "HIVE",
        "The name of counter group for internal Hive variables (CREATED_FILE, FATAL_ERROR, etc.)"),

    HIVE_SERVER2_TEZ_DEFAULT_QUEUES("hive.server2.tez.default.queues", "",
        "A list of comma separated values corresponding to YARN queues of the same name.\n" +
        "When HiveServer2 is launched in Tez mode, this configuration needs to be set\n" +
        "for multiple Tez sessions to run in parallel on the cluster."),
    HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE("hive.server2.tez.sessions.per.default.queue", 1,
        "A positive integer that determines the number of Tez sessions that should be\n" +
        "launched on each of the queues specified by \"hive.server2.tez.default.queues\".\n" +
        "Determines the parallelism on each queue."),
    HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS("hive.server2.tez.initialize.default.sessions", false,
        "This flag is used in HiveServer2 to enable a user to use HiveServer2 without\n" +
        "turning on Tez for HiveServer2. The user could potentially want to run queries\n" +
        "over Tez without the pool of sessions."),

    HIVE_QUOTEDID_SUPPORT("hive.support.quoted.identifiers", "column",
        new StringSet("none", "column"),
        "Whether to use quoted identifier. 'none' or 'column' can be used. \n" +
        "  none: default(past) behavior. Implies only alphaNumeric and underscore are valid characters in identifiers.\n" +
        "  column: implies column names can contain any character."
    ),

    // role names are case-insensitive
    USERS_IN_ADMIN_ROLE("hive.users.in.admin.role", "", false,
        "Comma separated list of users who are in admin role for bootstrapping.\n" +
        "More users can be added in ADMIN role later."),

    HIVE_COMPAT("hive.compat", HiveCompat.DEFAULT_COMPAT_LEVEL,
        "Enable (configurable) deprecated behaviors by setting desired level of backward compatibility.\n" +
        "Setting to 0.12:\n" +
        "  Maintains division behavior: int / int = double"),
    HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ("hive.convert.join.bucket.mapjoin.tez", false,
        "Whether joins can be automatically converted to bucket map joins in hive \n" +
        "when tez is used as the execution engine."),

    HIVE_CHECK_CROSS_PRODUCT("hive.exec.check.crossproducts", true,
        "Check if a plan contains a Cross Product. If there is one, output a warning to the Session's console."),
    HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL("hive.localize.resource.wait.interval", "5000ms",
        new TimeValidator(TimeUnit.MILLISECONDS),
        "Time to wait for another thread to localize the same resource for hive-tez."),
    HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS("hive.localize.resource.num.wait.attempts", 5,
        "The number of attempts waiting for localizing a resource in hive-tez."),
    TEZ_AUTO_REDUCER_PARALLELISM("hive.tez.auto.reducer.parallelism", false,
        "Turn on Tez' auto reducer parallelism feature. When enabled, Hive will still estimate data sizes\n" +
        "and set parallelism estimates. Tez will sample source vertices' output sizes and adjust the estimates at runtime as\n" +
        "necessary."),
    TEZ_MAX_PARTITION_FACTOR("hive.tez.max.partition.factor", 2f,
        "When auto reducer parallelism is enabled this factor will be used to over-partition data in shuffle edges."),
    TEZ_MIN_PARTITION_FACTOR("hive.tez.min.partition.factor", 0.25f,
        "When auto reducer parallelism is enabled this factor will be used to put a lower limit to the number\n" +
        "of reducers that tez specifies."),
    TEZ_DYNAMIC_PARTITION_PRUNING(
        "hive.tez.dynamic.partition.pruning", true,
        "When dynamic pruning is enabled, joins on partition keys will be processed by sending\n" +
        "events from the processing vertices to the Tez application master. These events will be\n" +
        "used to prune unnecessary partitions."),
    TEZ_DYNAMIC_PARTITION_PRUNING_MAX_EVENT_SIZE("hive.tez.dynamic.partition.pruning.max.event.size", 1*1024*1024L,
        "Maximum size of events sent by processors in dynamic pruning. If this size is crossed no pruning will take place."),
    TEZ_DYNAMIC_PARTITION_PRUNING_MAX_DATA_SIZE("hive.tez.dynamic.partition.pruning.max.data.size", 100*1024*1024L,
        "Maximum total data size of events in dynamic pruning."),
    TEZ_SMB_NUMBER_WAVES(
        "hive.tez.smb.number.waves",
        (float) 0.5,
        "The number of waves in which to run the SMB join. Account for cluster being occupied. Ideally should be 1 wave."),
    TEZ_EXEC_SUMMARY(
        "hive.tez.exec.print.summary",
        false,
        "Display breakdown of execution steps, for every query executed by the shell."),
    TEZ_EXEC_INPLACE_PROGRESS(
        "hive.tez.exec.inplace.progress",
        true,
        "Updates tez job execution progress in-place in the terminal."),
    SPARK_CLIENT_FUTURE_TIMEOUT("hive.spark.client.future.timeout",
      "60s", new TimeValidator(TimeUnit.SECONDS),
      "Timeout for requests from Hive client to remote Spark driver."),
    SPARK_JOB_MONITOR_TIMEOUT("hive.spark.job.monitor.timeout",
      "60s", new TimeValidator(TimeUnit.SECONDS),
      "Timeout for job monitor to get Spark job state."),
    SPARK_RPC_CLIENT_CONNECT_TIMEOUT("hive.spark.client.connect.timeout",
      "1000ms", new TimeValidator(TimeUnit.MILLISECONDS),
      "Timeout for remote Spark driver in connecting back to Hive client."),
    SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT("hive.spark.client.server.connect.timeout",
      "90000ms", new TimeValidator(TimeUnit.MILLISECONDS),
      "Timeout for handshake between Hive client and remote Spark driver.  Checked by both processes."),
    SPARK_RPC_SECRET_RANDOM_BITS("hive.spark.client.secret.bits", "256",
      "Number of bits of randomness in the generated secret for communication between Hive client and remote Spark driver. " +
      "Rounded down to the nearest multiple of 8."),
    SPARK_RPC_MAX_THREADS("hive.spark.client.rpc.threads", 8,
      "Maximum number of threads for remote Spark driver's RPC event loop."),
    SPARK_RPC_MAX_MESSAGE_SIZE("hive.spark.client.rpc.max.size", 50 * 1024 * 1024,
      "Maximum message size in bytes for communication between Hive client and remote Spark driver. Default is 50MB."),
    SPARK_RPC_CHANNEL_LOG_LEVEL("hive.spark.client.channel.log.level", null,
      "Channel logging level for remote Spark driver.  One of {DEBUG, ERROR, INFO, TRACE, WARN}."),
    SPARK_RPC_SASL_MECHANISM("hive.spark.client.rpc.sasl.mechanisms", "DIGEST-MD5",
      "Name of the SASL mechanism to use for authentication."),
    SPARK_ENABLED("hive.enable.spark.execution.engine", false, "Whether Spark is allowed as an execution engine");

    public final String varname;
    private final String defaultExpr;

    public final String defaultStrVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final boolean defaultBoolVal;

    private final Class<?> valClass;
    private final VarType valType;

    private final Validator validator;

    private final String description;

    private final boolean excluded;
    private final boolean caseSensitive;

    ConfVars(String varname, Object defaultVal, String description) {
      this(varname, defaultVal, null, description, true, false);
    }

    ConfVars(String varname, Object defaultVal, String description, boolean excluded) {
      this(varname, defaultVal, null, description, true, excluded);
    }

    ConfVars(String varname, String defaultVal, boolean caseSensitive, String description) {
      this(varname, defaultVal, null, description, caseSensitive, false);
    }

    ConfVars(String varname, Object defaultVal, Validator validator, String description) {
      this(varname, defaultVal, validator, description, true, false);
    }

    ConfVars(String varname, Object defaultVal, Validator validator, String description, boolean caseSensitive, boolean excluded) {
      this.varname = varname;
      this.validator = validator;
      this.description = description;
      this.defaultExpr = defaultVal == null ? null : String.valueOf(defaultVal);
      this.excluded = excluded;
      this.caseSensitive = caseSensitive;
      if (defaultVal == null || defaultVal instanceof String) {
        this.valClass = String.class;
        this.valType = VarType.STRING;
        this.defaultStrVal = SystemVariables.substitute((String)defaultVal);
        this.defaultIntVal = -1;
        this.defaultLongVal = -1;
        this.defaultFloatVal = -1;
        this.defaultBoolVal = false;
      } else if (defaultVal instanceof Integer) {
        this.valClass = Integer.class;
        this.valType = VarType.INT;
        this.defaultStrVal = null;
        this.defaultIntVal = (Integer)defaultVal;
        this.defaultLongVal = -1;
        this.defaultFloatVal = -1;
        this.defaultBoolVal = false;
      } else if (defaultVal instanceof Long) {
        this.valClass = Long.class;
        this.valType = VarType.LONG;
        this.defaultStrVal = null;
        this.defaultIntVal = -1;
        this.defaultLongVal = (Long)defaultVal;
        this.defaultFloatVal = -1;
        this.defaultBoolVal = false;
      } else if (defaultVal instanceof Float) {
        this.valClass = Float.class;
        this.valType = VarType.FLOAT;
        this.defaultStrVal = null;
        this.defaultIntVal = -1;
        this.defaultLongVal = -1;
        this.defaultFloatVal = (Float)defaultVal;
        this.defaultBoolVal = false;
      } else if (defaultVal instanceof Boolean) {
        this.valClass = Boolean.class;
        this.valType = VarType.BOOLEAN;
        this.defaultStrVal = null;
        this.defaultIntVal = -1;
        this.defaultLongVal = -1;
        this.defaultFloatVal = -1;
        this.defaultBoolVal = (Boolean)defaultVal;
      } else {
        throw new IllegalArgumentException("Not supported type value " + defaultVal.getClass() +
            " for name " + varname);
      }
    }

    public boolean isType(String value) {
      return valType.isType(value);
    }

    public Validator getValidator() {
      return validator;
    }

    public String validate(String value) {
      return validator == null ? null : validator.validate(value);
    }

    public String validatorDescription() {
      return validator == null ? null : validator.toDescription();
    }

    public String typeString() {
      String type = valType.typeString();
      if (valType == VarType.STRING && validator != null) {
        if (validator instanceof TimeValidator) {
          type += "(TIME)";
        }
      }
      return type;
    }

    public String getRawDescription() {
      return description;
    }

    public String getDescription() {
      String validator = validatorDescription();
      if (validator != null) {
        return validator + ".\n" + description;
      }
      return description;
    }

    public boolean isExcluded() {
      return excluded;
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
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

    public String getDefaultValue() {
      return valType.defaultValueString(this);
    }

    public String getDefaultExpr() {
      return defaultExpr;
    }

    enum VarType {
      STRING {
        @Override
        void checkType(String value) throws Exception { }
        @Override
        String defaultValueString(ConfVars confVar) { return confVar.defaultStrVal; }
      },
      INT {
        @Override
        void checkType(String value) throws Exception { Integer.valueOf(value); }
      },
      LONG {
        @Override
        void checkType(String value) throws Exception { Long.valueOf(value); }
      },
      FLOAT {
        @Override
        void checkType(String value) throws Exception { Float.valueOf(value); }
      },
      BOOLEAN {
        @Override
        void checkType(String value) throws Exception { Boolean.valueOf(value); }
      };

      boolean isType(String value) {
        try { checkType(value); } catch (Exception e) { return false; }
        return true;
      }
      String typeString() { return name().toUpperCase();}
      String defaultValueString(ConfVars confVar) { return confVar.defaultExpr; }
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
    if (modWhiteListPattern != null) {
      Matcher wlMatcher = modWhiteListPattern.matcher(name);
      if (!wlMatcher.matches()) {
        throw new IllegalArgumentException("Cannot modify " + name + " at runtime. "
            + "It is not in list of params that are allowed to be modified at runtime");
      }
    }
    if (restrictList.contains(name)) {
      throw new IllegalArgumentException("Cannot modify " + name + " at runtime. It is in the list"
          + "of parameters that can't be modified at runtime");
    }
    String oldValue = name != null ? get(name) : null;
    if (name == null || value == null || !value.equals(oldValue)) {
      // When either name or value is null, the set method below will fail,
      // and throw IllegalArgumentException
      set(name, value);
      isSparkConfigUpdated = isSparkRelatedConfig(name);
    }
  }

  /**
   * check whether spark related property is updated, which includes spark configurations,
   * RSC configurations and yarn configuration in Spark on YARN mode.
   * @param name
   * @return
   */
  private boolean isSparkRelatedConfig(String name) {
    boolean result = false;
    if (name.startsWith("spark")) { // Spark property.
      result = true;
    } else if (name.startsWith("yarn")) { // YARN property in Spark on YARN mode.
      String sparkMaster = get("spark.master");
      if (sparkMaster != null &&
        (sparkMaster.equals("yarn-client") || sparkMaster.equals("yarn-cluster"))) {
        result = true;
      }
    } else if (name.startsWith("hive.spark")) { // Remote Spark Context property.
      result = true;
    }

    return result;
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

  public static long getTimeVar(Configuration conf, ConfVars var, TimeUnit outUnit) {
    return toTime(getVar(conf, var), getDefaultTimeUnit(var), outUnit);
  }

  public static void setTimeVar(Configuration conf, ConfVars var, long time, TimeUnit timeunit) {
    assert (var.valClass == String.class) : var.varname;
    conf.set(var.varname, time + stringFor(timeunit));
  }

  public long getTimeVar(ConfVars var, TimeUnit outUnit) {
    return getTimeVar(this, var, outUnit);
  }

  public void setTimeVar(ConfVars var, long time, TimeUnit outUnit) {
    setTimeVar(this, var, time, outUnit);
  }

  private static TimeUnit getDefaultTimeUnit(ConfVars var) {
    TimeUnit inputUnit = null;
    if (var.validator instanceof TimeValidator) {
      inputUnit = ((TimeValidator)var.validator).getTimeUnit();
    }
    return inputUnit;
  }

  public static long toTime(String value, TimeUnit inputUnit, TimeUnit outUnit) {
    String[] parsed = parseTime(value.trim());
    return outUnit.convert(Long.valueOf(parsed[0].trim().trim()), unitFor(parsed[1].trim(), inputUnit));
  }

  private static String[] parseTime(String value) {
    char[] chars = value.toCharArray();
    int i = 0;
    for (; i < chars.length && (chars[i] == '-' || Character.isDigit(chars[i])); i++) {
    }
    return new String[] {value.substring(0, i), value.substring(i)};
  }

  public static TimeUnit unitFor(String unit, TimeUnit defaultUnit) {
    unit = unit.trim().toLowerCase();
    if (unit.isEmpty() || unit.equals("l")) {
      if (defaultUnit == null) {
        throw new IllegalArgumentException("Time unit is not specified");
      }
      return defaultUnit;
    } else if (unit.equals("d") || unit.startsWith("day")) {
      return TimeUnit.DAYS;
    } else if (unit.equals("h") || unit.startsWith("hour")) {
      return TimeUnit.HOURS;
    } else if (unit.equals("m") || unit.startsWith("min")) {
      return TimeUnit.MINUTES;
    } else if (unit.equals("s") || unit.startsWith("sec")) {
      return TimeUnit.SECONDS;
    } else if (unit.equals("ms") || unit.startsWith("msec")) {
      return TimeUnit.MILLISECONDS;
    } else if (unit.equals("us") || unit.startsWith("usec")) {
      return TimeUnit.MICROSECONDS;
    } else if (unit.equals("ns") || unit.startsWith("nsec")) {
      return TimeUnit.NANOSECONDS;
    }
    throw new IllegalArgumentException("Invalid time unit " + unit);
  }

  public static String stringFor(TimeUnit timeunit) {
    switch (timeunit) {
      case DAYS: return "day";
      case HOURS: return "hour";
      case MINUTES: return "min";
      case SECONDS: return "sec";
      case MILLISECONDS: return "msec";
      case MICROSECONDS: return "usec";
      case NANOSECONDS: return "nsec";
    }
    throw new IllegalArgumentException("Invalid timeunit " + timeunit);
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
    return conf.get(var.varname, var.defaultStrVal);
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

  public static ConfVars getMetaConf(String name) {
    return metaConfs.get(name);
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

    // if embedded metastore is to be used as per config so far
    // then this is considered like the metastore server case
    String msUri = this.getVar(HiveConf.ConfVars.METASTOREURIS);
    if(HiveConfUtil.isEmbeddedMetaStore(msUri)){
      setLoadMetastoreConfig(true);
    }

    // load hivemetastore-site.xml if this is metastore and file exists
    if (isLoadMetastoreConfig() && hivemetastoreSiteUrl != null) {
      addResource(hivemetastoreSiteUrl);
    }

    // load hiveserver2-site.xml if this is hiveserver2 and file exists
    // metastore can be embedded within hiveserver2, in such cases
    // the conf params in hiveserver2-site.xml will override whats defined
    // in hivemetastore-site.xml
    if (isLoadHiveServer2Config() && hiveServer2SiteUrl != null) {
      addResource(hiveServer2SiteUrl);
    }

    // Overlay the values of any system properties whose names appear in the list of ConfVars
    applySystemProperties();

    if ((this.get("hive.metastore.ds.retry.attempts") != null) ||
      this.get("hive.metastore.ds.retry.interval") != null) {
        l4j.warn("DEPRECATED: hive.metastore.ds.retry.* no longer has any effect.  " +
        "Use hive.hmshandler.retry.* instead");
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

    if (getBoolVar(HiveConf.ConfVars.HIVECONFVALIDATION)) {
      List<String> trimmed = new ArrayList<String>();
      for (Map.Entry<String,String> entry : this) {
        String key = entry.getKey();
        if (key == null || !key.startsWith("hive.")) {
          continue;
        }
        ConfVars var = HiveConf.getConfVars(key);
        if (var == null) {
          var = HiveConf.getConfVars(key.trim());
          if (var != null) {
            trimmed.add(key);
          }
        }
        if (var == null) {
          l4j.warn("HiveConf of name " + key + " does not exist");
        } else if (!var.isType(entry.getValue())) {
          l4j.warn("HiveConf " + var.varname + " expects " + var.typeString() + " type value");
        }
      }
      for (String key : trimmed) {
        set(key.trim(), getRaw(key));
        unset(key);
      }
    }

    setupSQLStdAuthWhiteList();

    // setup list of conf vars that are not allowed to change runtime
    setupRestrictList();

  }

  /**
   * If the config whitelist param for sql standard authorization is not set, set it up here.
   */
  private void setupSQLStdAuthWhiteList() {
    String whiteListParamsStr = getVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST);
    if (whiteListParamsStr == null || whiteListParamsStr.trim().isEmpty()) {
      // set the default configs in whitelist
      whiteListParamsStr = getSQLStdAuthDefaultWhiteListPattern();
    }
    setVar(ConfVars.HIVE_AUTHORIZATION_SQL_STD_AUTH_CONFIG_WHITELIST, whiteListParamsStr);
  }

  private static String getSQLStdAuthDefaultWhiteListPattern() {
    // create the default white list from list of safe config params
    // and regex list
    String confVarPatternStr = Joiner.on("|").join(convertVarsToRegex(sqlStdAuthSafeVarNames));
    String regexPatternStr = Joiner.on("|").join(sqlStdAuthSafeVarNameRegexes);
    return regexPatternStr + "|" + confVarPatternStr;
  }

  /**
   * @param paramList  list of parameter strings
   * @return list of parameter strings with "." replaced by "\."
   */
  private static String[] convertVarsToRegex(String[] paramList) {
    String[] regexes = new String[paramList.length];
    for(int i=0; i<paramList.length; i++) {
      regexes[i] = paramList[i].replace(".", "\\." );
    }
    return regexes;
  }

  /**
   * Default list of modifiable config parameters for sql standard authorization
   * For internal use only.
   */
  private static final String [] sqlStdAuthSafeVarNames = new String [] {
    ConfVars.BYTESPERREDUCER.varname,
    ConfVars.CLIENT_STATS_COUNTERS.varname,
    ConfVars.DEFAULTPARTITIONNAME.varname,
    ConfVars.DROPIGNORESNONEXISTENT.varname,
    ConfVars.HIVECOUNTERGROUP.varname,
    ConfVars.HIVEENFORCEBUCKETING.varname,
    ConfVars.HIVEENFORCEBUCKETMAPJOIN.varname,
    ConfVars.HIVEENFORCESORTING.varname,
    ConfVars.HIVEENFORCESORTMERGEBUCKETMAPJOIN.varname,
    ConfVars.HIVEEXPREVALUATIONCACHE.varname,
    ConfVars.HIVEGROUPBYSKEW.varname,
    ConfVars.HIVEHASHTABLELOADFACTOR.varname,
    ConfVars.HIVEHASHTABLETHRESHOLD.varname,
    ConfVars.HIVEIGNOREMAPJOINHINT.varname,
    ConfVars.HIVELIMITMAXROWSIZE.varname,
    ConfVars.HIVEMAPREDMODE.varname,
    ConfVars.HIVEMAPSIDEAGGREGATE.varname,
    ConfVars.HIVEOPTIMIZEMETADATAQUERIES.varname,
    ConfVars.HIVEROWOFFSET.varname,
    ConfVars.HIVEVARIABLESUBSTITUTE.varname,
    ConfVars.HIVEVARIABLESUBSTITUTEDEPTH.varname,
    ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME.varname,
    ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL.varname,
    ConfVars.HIVE_CHECK_CROSS_PRODUCT.varname,
    ConfVars.HIVE_COMPAT.varname,
    ConfVars.HIVE_CONCATENATE_CHECK_INDEX.varname,
    ConfVars.HIVE_DISPLAY_PARTITION_COLUMNS_SEPARATELY.varname,
    ConfVars.HIVE_ERROR_ON_EMPTY_PARTITION.varname,
    ConfVars.HIVE_EXECUTION_ENGINE.varname,
    ConfVars.HIVE_EXIM_URI_SCHEME_WL.varname,
    ConfVars.HIVE_FILE_MAX_FOOTER.varname,
    ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES.varname,
    ConfVars.HIVE_INSERT_INTO_MULTILEVEL_DIRS.varname,
    ConfVars.HIVE_LOCALIZE_RESOURCE_NUM_WAIT_ATTEMPTS.varname,
    ConfVars.HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES.varname,
    ConfVars.HIVE_QUOTEDID_SUPPORT.varname,
    ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES.varname,
    ConfVars.HIVE_STATS_COLLECT_PART_LEVEL_STATS.varname,
    ConfVars.JOB_DEBUG_CAPTURE_STACKTRACES.varname,
    ConfVars.JOB_DEBUG_TIMEOUT.varname,
    ConfVars.MAXCREATEDFILES.varname,
    ConfVars.MAXREDUCERS.varname,
    ConfVars.OUTPUT_FILE_EXTENSION.varname,
    ConfVars.SHOW_JOB_FAIL_DEBUG_INFO.varname,
    ConfVars.TASKLOG_DEBUG_TIMEOUT.varname,
  };

  /**
   * Default list of regexes for config parameters that are modifiable with
   * sql standard authorization enabled
   */
  static final String [] sqlStdAuthSafeVarNameRegexes = new String [] {
    "hive\\.auto\\..*",
    "hive\\.cbo\\..*",
    "hive\\.convert\\..*",
    "hive\\.exec\\.dynamic\\.partition.*",
    "hive\\.exec\\..*\\.dynamic\\.partitions\\..*",
    "hive\\.exec\\.compress\\..*",
    "hive\\.exec\\.infer\\..*",
    "hive\\.exec\\.mode.local\\..*",
    "hive\\.exec\\.orc\\..*",
    "hive\\.fetch.task\\..*",
    "hive\\.hbase\\..*",
    "hive\\.index\\..*",
    "hive\\.index\\..*",
    "hive\\.intermediate\\..*",
    "hive\\.join\\..*",
    "hive\\.limit\\..*",
    "hive\\.mapjoin\\..*",
    "hive\\.merge\\..*",
    "hive\\.optimize\\..*",
    "hive\\.orc\\..*",
    "hive\\.outerjoin\\..*",
    "hive\\.ppd\\..*",
    "hive\\.prewarm\\..*",
    "hive\\.skewjoin\\..*",
    "hive\\.smbjoin\\..*",
    "hive\\.stats\\..*",
    "hive\\.tez\\..*",
    "hive\\.vectorized\\..*",
    "mapred\\.map\\..*",
    "mapred\\.reduce\\..*",
    "mapred\\.output\\.compression\\.codec",
    "mapreduce\\.job\\.reduce\\.slowstart\\.completedmaps",
    "mapreduce\\.job\\.queuename",
    "mapreduce\\.input\\.fileinputformat\\.split\\.minsize",
    "mapreduce\\.map\\..*",
    "mapreduce\\.reduce\\..*",
    "tez\\.am\\..*",
    "tez\\.task\\..*",
    "tez\\.runtime\\..*",
  };



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
      String defaultValue = var.getDefaultValue();
      if (defaultValue == null) {
        // Don't override ConfVars with null values
        continue;
      }
      conf.set(var.varname, defaultValue);
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

  public static URL getMetastoreSiteLocation() {
    return hivemetastoreSiteUrl;
  }

  public static URL getHiveServer2SiteLocation() {
    return hiveServer2SiteUrl;
  }

  /**
   * @return the user name set in hadoop.job.ugi param or the current user from System
   * @throws IOException
   */
  public String getUser() throws IOException {
    try {
      UserGroupInformation ugi = Utils.getUGI();
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
   * Set white list of parameters that are allowed to be modified
   *
   * @param paramNameRegex
   */
  @LimitedPrivate(value = { "Currently only for use by HiveAuthorizer" })
  public void setModifiableWhiteListRegex(String paramNameRegex) {
    if (paramNameRegex == null) {
      return;
    }
    modWhiteListPattern = Pattern.compile(paramNameRegex);
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
    restrictList.add(ConfVars.HIVE_IN_TEST.varname);
    restrictList.add(ConfVars.HIVE_CONF_RESTRICTED_LIST.varname);
  }

  public static boolean isLoadMetastoreConfig() {
    return loadMetastoreConfig;
  }

  public static void setLoadMetastoreConfig(boolean loadMetastoreConfig) {
    HiveConf.loadMetastoreConfig = loadMetastoreConfig;
  }

  public static boolean isLoadHiveServer2Config() {
    return loadHiveServer2Config;
  }

  public static void setLoadHiveServer2Config(boolean loadHiveServer2Config) {
    HiveConf.loadHiveServer2Config = loadHiveServer2Config;
  }
}
