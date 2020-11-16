/* * Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.utils.CommonCliOptions;
import org.apache.hadoop.hive.metastore.utils.LogUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.eclipse.jetty.io.ssl.ALPNProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * TODO:pc remove application logic to a separate interface.
 */
public class HiveMetaStore {
  public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStore.class);
  public static final String PARTITION_NUMBER_EXCEED_LIMIT_MSG =
      "Number of partitions scanned (=%d) on table '%s' exceeds limit (=%d). This is controlled on the metastore server by %s.";

  // boolean that tells if the HiveMetaStore (remote) server is being used.
  // Can be used to determine if the calls to metastore api (HMSHandler) are being made with
  // embedded metastore or a remote one
  static boolean isMetaStoreRemote = false;

  // Used for testing to simulate method timeout.
  @VisibleForTesting
  static boolean TEST_TIMEOUT_ENABLED = false;
  @VisibleForTesting
  static long TEST_TIMEOUT_VALUE = -1;

  private static ShutdownHookManager shutdownHookMgr;

  public static final String ADMIN = "admin";
  public static final String PUBLIC = "public";
  /** MM write states. */
  public static final char MM_WRITE_OPEN = 'o', MM_WRITE_COMMITTED = 'c', MM_WRITE_ABORTED = 'a';

  static HadoopThriftAuthBridge.Server saslServer;
  static MetastoreDelegationTokenManager delegationTokenManager;
  static boolean useSasl;

  static final String NO_FILTER_STRING = "";
  static final int UNLIMITED_MAX_PARTITIONS = -1;
  static ZooKeeperHiveHelper zooKeeperHelper = null;
  static String msHost = null;

  public static boolean isRenameAllowed(Database srcDB, Database destDB) {
    if (!srcDB.getName().equalsIgnoreCase(destDB.getName())) {
      if (ReplChangeManager.isSourceOfReplication(srcDB) || ReplChangeManager.isSourceOfReplication(destDB)) {
        return false;
      }
    }
    return true;
  }

  static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf)
      throws MetaException {
    return newRetryingHMSHandler(baseHandler, conf, false);
  }

  static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf,
      boolean local) throws MetaException {
    return RetryingHMSHandler.getProxy(conf, baseHandler, local);
  }

  /**
   * Create retrying HMS handler for embedded metastore.
   *
   * <h1>IMPORTANT</h1>
   *
   * This method is called indirectly by HiveMetastoreClient and HiveMetaStoreClientPreCatalog
   * using reflection. It can not be removed and its arguments can't be changed without matching
   * change in HiveMetastoreClient and HiveMetaStoreClientPreCatalog.
   *
   * @param conf configuration to use
   * @throws MetaException
   */
  static ThriftHiveMetastore.Iface newRetryingHMSHandler(Configuration conf)
      throws MetaException {
    HMSHandler baseHandler = new HMSHandler("hive client", conf, false);
    return RetryingHMSHandler.getProxy(conf, baseHandler, true);
  }

  /**
   * Discard a current delegation token.
   *
   * @param tokenStrForm
   *          the token in string form
   */
  public static void cancelDelegationToken(String tokenStrForm
      ) throws IOException {
    delegationTokenManager.cancelDelegationToken(tokenStrForm);
  }

  /**
   * Get a new delegation token.
   *
   * @param renewer
   *          the designated renewer
   */
  public static String getDelegationToken(String owner, String renewer, String remoteAddr)
      throws IOException, InterruptedException {
    return delegationTokenManager.getDelegationToken(owner, renewer, remoteAddr);
  }

  /**
   * @return true if remote metastore has been created
   */
  public static boolean isMetaStoreRemote() {
    return isMetaStoreRemote;
  }

  /**
   * Renew a delegation token to extend its lifetime.
   *
   * @param tokenStrForm
   *          the token in string form
   */
  public static long renewDelegationToken(String tokenStrForm
      ) throws IOException {
    return delegationTokenManager.renewDelegationToken(tokenStrForm);
  }

  /**
   * HiveMetaStore specific CLI
   *
   */
  static public class HiveMetastoreCli extends CommonCliOptions {
    private int port;

    @SuppressWarnings("static-access")
    HiveMetastoreCli(Configuration configuration) {
      super("hivemetastore", true);
      this.port = MetastoreConf.getIntVar(configuration, ConfVars.SERVER_PORT);

      // -p port
      OPTIONS.addOption(OptionBuilder
          .hasArg()
          .withArgName("port")
          .withDescription("Hive Metastore port number, default:"
              + this.port)
          .create('p'));

    }

    @Override
    public void parse(String[] args) {
      super.parse(args);

      // support the old syntax "hivemetastore [port]" but complain
      args = commandLine.getArgs();
      if (args.length > 0) {
        // complain about the deprecated syntax -- but still run
        System.err.println(
            "This usage has been deprecated, consider using the new command "
                + "line syntax (run with -h to see usage information)");

        this.port = Integer.parseInt(args[0]);
      }

      // notice that command line options take precedence over the
      // deprecated (old style) naked args...

      if (commandLine.hasOption('p')) {
        this.port = Integer.parseInt(commandLine.getOptionValue('p'));
      } else {
        // legacy handling
        String metastorePort = System.getenv("METASTORE_PORT");
        if (metastorePort != null) {
          this.port = Integer.parseInt(metastorePort);
        }
      }
    }

    public int getPort() {
      return this.port;
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Throwable {
    final Configuration conf = MetastoreConf.newMetastoreConf();
    shutdownHookMgr = ShutdownHookManager.get();

    HiveMetastoreCli cli = new HiveMetastoreCli(conf);
    cli.parse(args);
    final boolean isCliVerbose = cli.isVerbose();
    // NOTE: It is critical to do this prior to initializing log4j, otherwise
    // any log specific settings via hiveconf will be ignored
    Properties hiveconf = cli.addHiveconfToSystemProperties();

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    try {
      // If the log4j.configuration property hasn't already been explicitly set,
      // use Hive's default log4j configuration
      if (System.getProperty("log4j.configurationFile") == null) {
        LogUtils.initHiveLog4j(conf);
      } else {
        //reconfigure log4j after settings via hiveconf are write into System Properties
        LoggerContext context =  (LoggerContext)LogManager.getContext(false);
        context.reconfigure();
      }
    } catch (LogUtils.LogInitializationException e) {
      HMSHandler.LOG.warn(e.getMessage());
    }
    startupShutdownMessage(HiveMetaStore.class, args, LOG);
    List<MetastoreLauncher> serverLaunchers = new ArrayList<>();

    String customServerClass = hiveconf.getProperty(ConfVars.CUSTOM_SERVER_CLASS));
    if (!customServerClass.isEmpty()) {
      Class<?> customServerLauncher = ClassLoader.getSystemClassLoader().loadClass(customServerClass);
      LOG.info("Loaded class {} as server launcher", customServerClass.getCanonicalName());
      MetastoreLauncher launcher = (MetastoreLauncher) customServerClass.newInstance();
      serverLaunchers.add(launcher);
    }

    serverLaunchers.add(new ThriftMetastoreLauncher());
    ExecutorService executorService = Executors.newFixedThreadPool(serverLaunchers.size());
    for (MetastoreLauncher serverLauncher : serverLaunchers) {
      executorService.execute(serverLauncher.configure(conf, cli));
    }
    shutdownHookMgr.addShutdownHook(executorService::shutdown, 10);
  }

  static int nextThreadId = 1000000;

  /**
   * Print a log message for starting up and shutting down
   * @param clazz the class of the server
   * @param args arguments
   * @param LOG the target log object
   */
  private static void startupShutdownMessage(Class<?> clazz, String[] args,
                                             final org.slf4j.Logger LOG) {
    final String hostname = getHostname();
    final String classname = clazz.getSimpleName();
    LOG.info(
        toStartupShutdownString("STARTUP_MSG: ", new String[] {
            "Starting " + classname,
            "  host = " + hostname,
            "  args = " + Arrays.asList(args),
            "  version = " + MetastoreVersionInfo.getVersion(),
            "  classpath = " + System.getProperty("java.class.path"),
            "  build = " + MetastoreVersionInfo.getUrl() + " -r "
                + MetastoreVersionInfo.getRevision()
                + "; compiled by '" + MetastoreVersionInfo.getUser()
                + "' on " + MetastoreVersionInfo.getDate()}
        )
    );

    shutdownHookMgr.addShutdownHook(
        () -> LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
            "Shutting down " + classname + " at " + hostname})), 0);

  }

  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  private static String toStartupShutdownString(String prefix, String [] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for(String s : msg) {
      b.append("\n")
          .append(prefix)
          .append(s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Return hostname without throwing exception.
   * @return hostname
   */
  private static String getHostname() {
    try {return "" + InetAddress.getLocalHost();}
    catch(UnknownHostException uhe) {return "" + uhe;}
  }
}
