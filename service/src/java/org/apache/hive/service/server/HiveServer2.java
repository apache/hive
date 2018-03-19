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

package org.apache.hive.service.server;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JvmPauseMonitor;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.cli.CommonCliOptions;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.session.ClearDanglingScratchDir;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.http.HttpServer;
import org.apache.hive.http.JdbcJarDownloadServlet;
import org.apache.hive.http.LlapServlet;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.servlet.HS2LeadershipStatus;
import org.apache.hive.service.servlet.HS2Peers;
import org.apache.hive.service.servlet.QueryProfileServlet;
import org.apache.logging.log4j.util.Strings;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static CountDownLatch deleteSignal;
  private static final Logger LOG = LoggerFactory.getLogger(HiveServer2.class);
  public static final String INSTANCE_URI_CONFIG = "hive.server2.instance.uri";
  private static final int SHUTDOWN_TIME = 60;
  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private PersistentEphemeralNode znode;
  private CuratorFramework zooKeeperClient;
  private boolean deregisteredWithZooKeeper = false; // Set to true only when deregistration happens
  private HttpServer webServer; // Web UI
  private TezSessionPoolManager tezSessionPoolManager;
  private WorkloadManager wm;
  private PamAuthenticator pamAuthenticator;
  private Map<String, String> confsToPublish = new HashMap<String, String>();
  private String serviceUri;
  private boolean serviceDiscovery;
  private boolean activePassiveHA;
  private LeaderLatchListener leaderLatchListener;
  private ExecutorService leaderActionsExecutorService;
  private HS2ActivePassiveHARegistry hs2HARegistry;
  private Hive sessionHive;
  private String wmQueue;
  private AtomicBoolean isLeader = new AtomicBoolean(false);

  public HiveServer2() {
    super(HiveServer2.class.getSimpleName());
    HiveConf.setLoadHiveServer2Config(true);
  }

  @VisibleForTesting
  public HiveServer2(PamAuthenticator pamAuthenticator) {
    super(HiveServer2.class.getSimpleName());
    HiveConf.setLoadHiveServer2Config(true);
    this.pamAuthenticator = pamAuthenticator;
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    //Initialize metrics first, as some metrics are for initialization stuff.
    try {
      if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_METRICS_ENABLED)) {
        MetricsFactory.init(hiveConf);
      }
    } catch (Throwable t) {
      LOG.warn("Could not initiate the HiveServer2 Metrics system.  Metrics may not be reported.", t);
    }

    cliService = new CLIService(this);
    addService(cliService);
    final HiveServer2 hiveServer2 = this;
    Runnable oomHook = new Runnable() {
      @Override
      public void run() {
        hiveServer2.stop();
      }
    };
    if (isHTTPTransportMode(hiveConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService, oomHook);
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService, oomHook);
    }
    addService(thriftCLIService);
    super.init(hiveConf);
    // Set host name in conf
    try {
      hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, getServerHost());
    } catch (Throwable t) {
      throw new Error("Unable to initialize HiveServer2", t);
    }
    if (HiveConf.getBoolVar(hiveConf, ConfVars.LLAP_HS2_ENABLE_COORDINATOR)) {
      // See method comment.
      try {
        LlapCoordinator.initializeInstance(hiveConf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // Trigger the creation of LLAP registry client, if in use. Clients may be using a different
    // cluster than the default one, but at least for the default case we'd have it covered.
    String llapHosts = HiveConf.getVar(hiveConf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    if (llapHosts != null && !llapHosts.isEmpty()) {
      LlapRegistryService.getClient(hiveConf);
    }

    try {
      sessionHive = Hive.get(hiveConf);
    } catch (HiveException e) {
      throw new RuntimeException("Failed to get metastore connection", e);
    }

    // Create views registry
    HiveMaterializedViewsRegistry.get().init();

    // Setup cache if enabled.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED)) {
      try {
        QueryResultsCache.initialize(hiveConf);
      } catch (Exception err) {
        throw new RuntimeException("Error initializing the query results cache", err);
      }
    }

    wmQueue = hiveConf.get(ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE.varname, "").trim();

    this.serviceDiscovery = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY);
    this.activePassiveHA = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE);

    // Setup web UI
    final int webUIPort;
    final String webHost;
    try {
      webUIPort = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT);
      webHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_BIND_HOST);
      // We disable web UI in tests unless the test is explicitly setting a
      // unique web ui port so that we don't mess up ptests.
      boolean uiDisabledInTest = hiveConf.getBoolVar(ConfVars.HIVE_IN_TEST) &&
          (webUIPort == Integer.valueOf(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
      if (uiDisabledInTest) {
        LOG.info("Web UI is disabled in test mode since webui port was not specified");
      } else {
        if (webUIPort <= 0) {
          LOG.info("Web UI is disabled since port is set to " + webUIPort);
        } else {
          LOG.info("Starting Web UI on port "+ webUIPort);
          HttpServer.Builder builder = new HttpServer.Builder("hiveserver2");
          builder.setPort(webUIPort).setConf(hiveConf);
          builder.setHost(webHost);
          builder.setMaxThreads(
            hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_MAX_THREADS));
          builder.setAdmins(hiveConf.getVar(ConfVars.USERS_IN_ADMIN_ROLE));
          // SessionManager is initialized
          builder.setContextAttribute("hive.sm",
            cliService.getSessionManager());
          hiveConf.set("startcode",
            String.valueOf(System.currentTimeMillis()));
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL)) {
            String keyStorePath = hiveConf.getVar(
              ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH);
            if (Strings.isBlank(keyStorePath)) {
              throw new IllegalArgumentException(
                ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH.varname
                  + " Not configured for SSL connection");
            }
            builder.setKeyStorePassword(ShimLoader.getHadoopShims().getPassword(
              hiveConf, ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PASSWORD.varname));
            builder.setKeyStorePath(keyStorePath);
            builder.setUseSSL(true);
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SPNEGO)) {
            String spnegoPrincipal = hiveConf.getVar(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_PRINCIPAL);
            String spnegoKeytab = hiveConf.getVar(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_KEYTAB);
            if (Strings.isBlank(spnegoPrincipal) || Strings.isBlank(spnegoKeytab)) {
              throw new IllegalArgumentException(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_PRINCIPAL.varname
                  + "/" + ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_KEYTAB.varname
                  + " Not configured for SPNEGO authentication");
            }
            builder.setSPNEGOPrincipal(spnegoPrincipal);
            builder.setSPNEGOKeytab(spnegoKeytab);
            builder.setUseSPNEGO(true);
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_PAM)) {
            if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL)) {
              String hiveServer2PamServices = hiveConf.getVar(ConfVars.HIVE_SERVER2_PAM_SERVICES);
              if (hiveServer2PamServices == null || hiveServer2PamServices.isEmpty()) {
                throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_PAM_SERVICES.varname + " are not configured.");
              }
              builder.setPAMAuthenticator(pamAuthenticator == null ? new PamAuthenticator(hiveConf) : pamAuthenticator);
              builder.setUsePAM(true);
            } else if (hiveConf.getBoolVar(ConfVars.HIVE_IN_TEST)) {
              builder.setPAMAuthenticator(pamAuthenticator == null ? new PamAuthenticator(hiveConf) : pamAuthenticator);
              builder.setUsePAM(true);
            } else {
              throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_WEBUI_USE_SSL.varname + " has false value. It is recommended to set to true when PAM is used.");
            }
          }
          if (serviceDiscovery && activePassiveHA) {
            builder.setContextAttribute("hs2.isLeader", isLeader);
            builder.setContextAttribute("hiveconf", hiveConf);
            builder.addServlet("leader", HS2LeadershipStatus.class);
            builder.addServlet("peers", HS2Peers.class);
          }
          builder.addServlet("llap", LlapServlet.class);
          builder.addServlet("jdbcjar", JdbcJarDownloadServlet.class);
          builder.setContextRootRewriteTarget("/hiveserver2.jsp");

          webServer = builder.build();
          webServer.addServlet("query_page", "/query_page", QueryProfileServlet.class);
        }
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }

    try {
      if (serviceDiscovery) {
        serviceUri = getServerInstanceURI();
        addConfsToPublish(hiveConf, confsToPublish, serviceUri);
        if (activePassiveHA) {
          hiveConf.set(INSTANCE_URI_CONFIG, serviceUri);
          leaderLatchListener = new HS2LeaderLatchListener(this, SessionState.get());
          leaderActionsExecutorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Leader Actions Handler Thread").build());
          hs2HARegistry = HS2ActivePassiveHARegistry.create(hiveConf, false);
        }
      }
    } catch (Exception e) {
      throw new ServiceException(e);
    }

    // Add a shutdown hook for catching SIGTERM & SIGINT
    ShutdownHookManager.addShutdownHook(() -> hiveServer2.stop());
  }

  private WMFullResourcePlan createTestResourcePlan() {
    WMFullResourcePlan resourcePlan;
    WMPool pool = new WMPool("testDefault", "llap");
    pool.setAllocFraction(1f);
    pool.setQueryParallelism(1);
    resourcePlan = new WMFullResourcePlan(
        new WMResourcePlan("testDefault"), Lists.newArrayList(pool));
    resourcePlan.getPlan().setDefaultPoolPath("testDefault");
    return resourcePlan;
  }

  public static boolean isHTTPTransportMode(Configuration hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.get(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
  }

  public static boolean isKerberosAuthMode(Configuration hiveConf) {
    String authMode = hiveConf.get(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname);
    if (authMode != null && (authMode.equalsIgnoreCase("KERBEROS"))) {
      return true;
    }
    return false;
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private final ACLProvider zooKeeperAclProvider = new ACLProvider() {

    @Override
    public List<ACL> getDefaultAcl() {
      List<ACL> nodeAcls = new ArrayList<ACL>();
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read all to the world
        nodeAcls.addAll(Ids.READ_ACL_UNSAFE);
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(Perms.ALL, Ids.AUTH_IDS));
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(Ids.OPEN_ACL_UNSAFE);
      }
      return nodeAcls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  /**
   * Adds a server instance to ZooKeeper as a znode.
   *
   * @param hiveConf
   * @throws Exception
   */
  private void addServerInstanceToZooKeeper(HiveConf hiveConf, Map<String, String> confsToPublish) throws Exception {
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    String instanceURI = getServerInstanceURI();
    setUpZooKeeperAuth(hiveConf);
    int sessionTimeout =
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
            TimeUnit.MILLISECONDS);
    int baseSleepTime =
        (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
            TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .sessionTimeoutMs(sessionTimeout).aclProvider(zooKeeperAclProvider)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    zooKeeperClient.start();
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zooKeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
      LOG.info("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2");
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        LOG.error("Unable to create HiveServer2 namespace: " + rootNamespace + " on ZooKeeper", e);
        throw e;
      }
    }
    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      String pathPrefix =
          ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
              + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "serverUri=" + instanceURI + ";"
              + "version=" + HiveVersionInfo.getVersion() + ";" + "sequence=";
      String znodeData = "";
      if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_PUBLISH_CONFIGS)) {
        // HiveServer2 configs that this instance will publish to ZooKeeper,
        // so that the clients can read these and configure themselves properly.

        addConfsToPublish(hiveConf, confsToPublish, instanceURI);
        // Publish configs for this instance as the data on the node
        znodeData = Joiner.on(';').withKeyValueSeparator("=").join(confsToPublish);
      } else {
        znodeData = instanceURI;
      }
      byte[] znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"));
      znode =
          new PersistentEphemeralNode(zooKeeperClient,
              PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, pathPrefix, znodeDataUTF8);
      znode.start();
      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }
      setDeregisteredWithZooKeeper(false);
      final String znodePath = znode.getActualPath();
      // Set a watch on the znode
      if (zooKeeperClient.checkExists().usingWatcher(new DeRegisterWatcher()).forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.");
      }
      LOG.info("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI);
    } catch (Exception e) {
      LOG.error("Unable to create a znode for this server instance", e);
      if (znode != null) {
        znode.close();
      }
      throw (e);
    }
  }

  /**
   * Add conf keys, values that HiveServer2 will publish to ZooKeeper.
   * @param hiveConf
   */
  private void addConfsToPublish(HiveConf hiveConf, Map<String, String> confsToPublish, String serviceUri) {
    // Hostname
    confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST));
    // Hostname:port
    confsToPublish.put(INSTANCE_URI_CONFIG, serviceUri);
    // Transport mode
    confsToPublish.put(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE));
    // Transport specific confs
    if (isHTTPTransportMode(hiveConf)) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname,
          Integer.toString(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT)));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));
    } else {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname,
          Integer.toString(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT)));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    }
    // Auth specific confs
    confsToPublish.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION));
    if (isKerberosAuthMode(hiveConf)) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL));
    }
    // SSL conf
    confsToPublish.put(ConfVars.HIVE_SERVER2_USE_SSL.varname,
        Boolean.toString(hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)));
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param hiveConf
   * @return
   * @throws Exception
   */
  private void setUpZooKeeperAuth(HiveConf hiveConf) throws Exception {
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
      if (principal.isEmpty()) {
        throw new IOException("HiveServer2 Kerberos principal is empty");
      }
      String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
      if (keyTabFile.isEmpty()) {
        throw new IOException("HiveServer2 Kerberos keytab is empty");
      }
      // Install the JAAS Configuration for the runtime
      Utils.setZookeeperClientKerberosJaasConfig(principal, keyTabFile);
    }
  }

  public boolean isLeader() {
    return isLeader.get();
  }


  /**
   * The watcher class which sets the de-register flag when the znode corresponding to this server
   * instance is deleted. Additionally, it shuts down the server if there are no more active client
   * sessions at the time of receiving a 'NodeDeleted' notification from ZooKeeper.
   */
  private class DeRegisterWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
        if (znode != null) {
          try {
            znode.close();
            LOG.warn("This HiveServer2 instance is now de-registered from ZooKeeper. "
                + "The server will be shut down after the last client session completes.");
          } catch (IOException e) {
            LOG.error("Failed to close the persistent ephemeral znode", e);
          } finally {
            HiveServer2.this.setDeregisteredWithZooKeeper(true);
            // If there are no more active client sessions, stop the server
            if (cliService.getSessionManager().getOpenSessionCount() == 0) {
              LOG.warn("This instance of HiveServer2 has been removed from the list of server "
                  + "instances available for dynamic service discovery. "
                  + "The last client session has ended - will shutdown now.");
              HiveServer2.this.stop();
            }
          }
        }
      }
    }
  }

  private void removeServerInstanceFromZooKeeper() throws Exception {
    setDeregisteredWithZooKeeper(true);

    if (znode != null) {
      znode.close();
    }
    zooKeeperClient.close();
    LOG.info("Server instance removed from ZooKeeper.");
  }

  public boolean isDeregisteredWithZooKeeper() {
    return deregisteredWithZooKeeper;
  }

  private void setDeregisteredWithZooKeeper(boolean deregisteredWithZooKeeper) {
    this.deregisteredWithZooKeeper = deregisteredWithZooKeeper;
  }

  private String getServerInstanceURI() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getHostName() + ":"
        + thriftCLIService.getPortNumber();
  }

  public String getServerHost() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getHostName();
  }

  @Override
  public synchronized void start() {
    super.start();
    // If we're supporting dynamic service discovery, we'll add the service uri for this
    // HiveServer2 instance to Zookeeper as a znode.
    HiveConf hiveConf = getHiveConf();
    if (serviceDiscovery) {
      try {
        if (activePassiveHA) {
          hs2HARegistry.registerLeaderLatchListener(leaderLatchListener, leaderActionsExecutorService);
          hs2HARegistry.start();
          LOG.info("HS2 HA registry started");
        } else {
          addServerInstanceToZooKeeper(hiveConf, confsToPublish);
        }
      } catch (Exception e) {
        LOG.error("Error adding this HiveServer2 instance to ZooKeeper: ", e);
        throw new ServiceException(e);
      }
    }
    if (webServer != null) {
      try {
        webServer.start();
        LOG.info("Web UI has started on port " + webServer.getPort());
      } catch (Exception e) {
        LOG.error("Error starting Web UI: ", e);
        throw new ServiceException(e);
      }
    }

    if (!activePassiveHA) {
      LOG.info("HS2 interactive HA not enabled. Starting tez sessions..");
      startOrReconnectTezSessions();
    } else {
      LOG.info("HS2 interactive HA enabled. Tez sessions will be started/reconnected by the leader.");
    }
  }

  private static class HS2LeaderLatchListener implements LeaderLatchListener {
    private HiveServer2 hiveServer2;
    private SessionState parentSession;

    HS2LeaderLatchListener(final HiveServer2 hs2, final SessionState parentSession) {
      this.hiveServer2 = hs2;
      this.parentSession = parentSession;
    }

    // leadership status change happens inside synchronized methods LeaderLatch.setLeadership().
    // Also we use single threaded executor service for handling notifications which guarantees ordering for
    // notification handling. if a leadership status change happens when tez sessions are getting created,
    // the notLeader notification will get queued in executor service.
    @Override
    public void isLeader() {
      LOG.info("HS2 instance {} became the LEADER. Starting/Reconnecting tez sessions..", hiveServer2.serviceUri);
      hiveServer2.isLeader.set(true);
      if (parentSession != null) {
        SessionState.setCurrentSessionState(parentSession);
      }
      hiveServer2.startOrReconnectTezSessions();
      LOG.info("Started/Reconnected tez sessions.");
    }

    @Override
    public void notLeader() {
      LOG.info("HS2 instance {} LOST LEADERSHIP. Stopping/Disconnecting tez sessions..", hiveServer2.serviceUri);
      hiveServer2.isLeader.set(false);
      hiveServer2.stopOrDisconnectTezSessions();
      LOG.info("Stopped/Disconnected tez sessions.");
    }
  }

  private void startOrReconnectTezSessions() {
    LOG.info("Starting/Reconnecting tez sessions..");
    // TODO: add tez session reconnect after TEZ-3875
    WMFullResourcePlan resourcePlan = null;
    if (!StringUtils.isEmpty(wmQueue)) {
      try {
        resourcePlan = sessionHive.getActiveResourcePlan();
      } catch (HiveException e) {
        if (!HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_IN_TEST_SSL)) {
          throw new RuntimeException(e);
        } else {
          resourcePlan = null; // Ignore errors in SSL tests where the connection is misconfigured.
        }
      }

      if (resourcePlan == null && HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_IN_TEST)) {
        LOG.info("Creating a default resource plan for test");
        resourcePlan = createTestResourcePlan();
      }
    }
    initAndStartTezSessionPoolManager(resourcePlan);
    initAndStartWorkloadManager(resourcePlan);
  }

  private void initAndStartTezSessionPoolManager(final WMFullResourcePlan resourcePlan) {
    // starting Tez session pool in start here to let parent session state initialize on CliService state, to avoid
    // SessionState.get() return null during createTezDir
    try {
      // will be invoked anyway in TezTask. Doing it early to initialize triggers for non-pool tez session.
      LOG.info("Initializing tez session pool manager");
      tezSessionPoolManager = TezSessionPoolManager.getInstance();
      if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
        tezSessionPoolManager.setupPool(hiveConf);
      }
      tezSessionPoolManager.startPool(hiveConf, resourcePlan);
      LOG.info("Tez session pool manager initialized.");
    } catch (Exception e) {
      throw new ServiceException("Unable to setup tez session pool", e);
    }
  }

  private void initAndStartWorkloadManager(final WMFullResourcePlan resourcePlan) {
    if (!StringUtils.isEmpty(wmQueue)) {
      // Initialize workload management.
      LOG.info("Initializing workload management");
      try {
        wm = WorkloadManager.create(wmQueue, hiveConf, resourcePlan);
        wm.start();
        LOG.info("Workload manager initialized.");
      } catch (Exception e) {
        throw new ServiceException("Unable to instantiate and start Workload Manager", e);
      }
    } else {
      LOG.info("Workload management is not enabled.");
    }
  }

  private void stopOrDisconnectTezSessions() {
    LOG.info("Stoppping/Disconnecting tez sessions.");
    // There should already be an instance of the session pool manager.
    // If not, ignoring is fine while stopping HiveServer2.
    if (tezSessionPoolManager != null) {
      try {
        tezSessionPoolManager.stop();
        LOG.info("Stopped tez session pool manager.");
      } catch (Exception e) {
        LOG.error("Tez session pool manager stop had an error during stop of HiveServer2. "
          + "Shutting down HiveServer2 anyway.", e);
      }
    }
    if (wm != null) {
      try {
        wm.stop();
        LOG.info("Stopped workload manager.");
      } catch (Exception e) {
        LOG.error("Workload manager stop had an error during stop of HiveServer2. "
          + "Shutting down HiveServer2 anyway.", e);
      }
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down HiveServer2");
    HiveConf hiveConf = this.getHiveConf();
    super.stop();
    if (hs2HARegistry != null) {
      hs2HARegistry.stop();
      shutdownExecutor(leaderActionsExecutorService);
      LOG.info("HS2 HA registry stopped");
      hs2HARegistry = null;
    }
    if (webServer != null) {
      try {
        webServer.stop();
        LOG.info("Web UI has stopped");
      } catch (Exception e) {
        LOG.error("Error stopping Web UI: ", e);
      }
    }
    // Shutdown Metrics
    if (MetricsFactory.getInstance() != null) {
      try {
        MetricsFactory.close();
      } catch (Exception e) {
        LOG.error("error in Metrics deinit: " + e.getClass().getName() + " "
          + e.getMessage(), e);
      }
    }
    // Remove this server instance from ZooKeeper if dynamic service discovery is set
    if (serviceDiscovery && !activePassiveHA) {
      try {
        removeServerInstanceFromZooKeeper();
      } catch (Exception e) {
        LOG.error("Error removing znode for this HiveServer2 instance from ZooKeeper.", e);
      }
    }

    stopOrDisconnectTezSessions();

    if (hiveConf != null && hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      try {
        SparkSessionManagerImpl.getInstance().shutdown();
      } catch(Exception ex) {
        LOG.error("Spark session pool manager failed to stop during HiveServer2 shutdown.", ex);
      }
    }
  }

  private void shutdownExecutor(final ExecutorService leaderActionsExecutorService) {
    leaderActionsExecutorService.shutdown();
    try {
      if (!leaderActionsExecutorService.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
        LOG.warn("Executor service did not terminate in the specified time {} sec", SHUTDOWN_TIME);
        List<Runnable> droppedTasks = leaderActionsExecutorService.shutdownNow();
        LOG.warn("Executor service was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
      }
    } catch (InterruptedException e) {
      LOG.warn("Executor service did not terminate in the specified time {} sec. Exception: {}", SHUTDOWN_TIME,
        e.getMessage());
      List<Runnable> droppedTasks = leaderActionsExecutorService.shutdownNow();
      LOG.warn("Executor service was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
    }
  }

  @VisibleForTesting
  public static void scheduleClearDanglingScratchDir(HiveConf hiveConf, int initialWaitInSec) {
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_CLEAR_DANGLING_SCRATCH_DIR)) {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder()
          .namingPattern("cleardanglingscratchdir-%d")
          .daemon(true)
          .build());
      executor.scheduleAtFixedRate(new ClearDanglingScratchDir(false, false, false,
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.SCRATCHDIR), hiveConf), initialWaitInSec,
          HiveConf.getTimeVar(hiveConf, ConfVars.HIVE_SERVER2_CLEAR_DANGLING_SCRATCH_DIR_INTERVAL,
          TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
  }

  private static void startHiveServer2() throws Throwable {
    long attempts = 0, maxAttempts = 1;
    while (true) {
      LOG.info("Starting HiveServer2");
      HiveConf hiveConf = new HiveConf();
      maxAttempts = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS);
      long retrySleepIntervalMs = hiveConf
          .getTimeVar(ConfVars.HIVE_SERVER2_SLEEP_INTERVAL_BETWEEN_START_ATTEMPTS,
              TimeUnit.MILLISECONDS);
      HiveServer2 server = null;
      try {
        // Cleanup the scratch dir before starting
        ServerUtils.cleanUpScratchDir(hiveConf);
        // Schedule task to cleanup dangling scratch dir periodically,
        // initial wait for a random time between 0-10 min to
        // avoid intial spike when using multiple HS2
        scheduleClearDanglingScratchDir(hiveConf, new Random().nextInt(600));

        server = new HiveServer2();
        server.init(hiveConf);
        server.start();

        try {
          JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(hiveConf);
          pauseMonitor.start();
        } catch (Throwable t) {
          LOG.warn("Could not initiate the JvmPauseMonitor thread." + " GCs and Pauses may not be " +
            "warned upon.", t);
        }

        if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
          SparkSessionManagerImpl.getInstance().setup(hiveConf);
        }
        break;
      } catch (Throwable throwable) {
        if (server != null) {
          try {
            server.stop();
          } catch (Throwable t) {
            LOG.info("Exception caught when calling stop of HiveServer2 before retrying start", t);
          } finally {
            server = null;
          }
        }
        if (++attempts >= maxAttempts) {
          throw new Error("Max start attempts " + maxAttempts + " exhausted", throwable);
        } else {
          LOG.warn("Error starting HiveServer2 on attempt " + attempts
              + ", will retry in " + retrySleepIntervalMs + "ms", throwable);
          try {
            Thread.sleep(retrySleepIntervalMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  /**
   * Remove all znodes corresponding to the given version number from ZooKeeper
   *
   * @param versionNumber
   * @throws Exception
   */
  static void deleteServerInstancesFromZooKeeper(String versionNumber) throws Exception {
    HiveConf hiveConf = new HiveConf();
    String zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf);
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    int baseSleepTime = (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS);
    int maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    CuratorFramework zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).build();
    zooKeeperClient.start();
    List<String> znodePaths =
        zooKeeperClient.getChildren().forPath(
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
    List<String> znodePathsUpdated;
    // Now for each path that is for the given versionNumber, delete the znode from ZooKeeper
    for (int i = 0; i < znodePaths.size(); i++) {
      String znodePath = znodePaths.get(i);
      deleteSignal = new CountDownLatch(1);
      if (znodePath.contains("version=" + versionNumber + ";")) {
        String fullZnodePath =
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
                + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + znodePath;
        LOG.warn("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        System.out.println("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        zooKeeperClient.delete().guaranteed().inBackground(new DeleteCallBack())
            .forPath(fullZnodePath);
        // Wait for the delete to complete
        deleteSignal.await();
        // Get the updated path list
        znodePathsUpdated =
            zooKeeperClient.getChildren().forPath(
                ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
        // Gives a list of any new paths that may have been created to maintain the persistent ephemeral node
        znodePathsUpdated.removeAll(znodePaths);
        // Add the new paths to the znodes list. We'll try for their removal as well.
        znodePaths.addAll(znodePathsUpdated);
      }
    }
    zooKeeperClient.close();
  }

  private static class DeleteCallBack implements BackgroundCallback {
    @Override
    public void processResult(CuratorFramework zooKeeperClient, CuratorEvent event)
        throws Exception {
      if (event.getType() == CuratorEventType.DELETE) {
        deleteSignal.countDown();
      }
    }
  }

  public static void main(String[] args) {
    HiveConf.setLoadHiveServer2Config(true);
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("hiveserver2");
      ServerOptionsProcessorResponse oprocResponse = oproc.parse(args);

      // NOTE: It is critical to do this here so that log4j is reinitialized
      // before any of the other core hive classes are loaded
      String initLog4jMessage = LogUtils.initHiveLog4j();
      LOG.debug(initLog4jMessage);
      HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);

      // Logger debug message from "oproc" after log4j initialize properly
      LOG.debug(oproc.getDebugMessage().toString());

      // Call the executor which will execute the appropriate command based on the parsed options
      oprocResponse.getServerOptionsExecutor().execute();
    } catch (LogInitializationException e) {
      LOG.error("Error initializing log: " + e.getMessage(), e);
      System.exit(-1);
    }
  }

  /**
   * ServerOptionsProcessor.
   * Process arguments given to HiveServer2 (-hiveconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  static class ServerOptionsProcessor {
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    private final String serverName;
    private final StringBuilder debugMessage = new StringBuilder();

    @SuppressWarnings("static-access")
    ServerOptionsProcessor(String serverName) {
      this.serverName = serverName;
      // -hiveconf x=y
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("hiveconf")
          .withDescription("Use value for given property")
          .create());
      // -deregister <versionNumber>
      options.addOption(OptionBuilder
          .hasArgs(1)
          .withArgName("versionNumber")
          .withLongOpt("deregister")
          .withDescription("Deregister all instances of given version from dynamic service discovery")
          .create());
      options.addOption(new Option("H", "help", false, "Print help information"));
    }

    ServerOptionsProcessorResponse parse(String[] argv) {
      try {
        commandLine = new GnuParser().parse(options, argv);
        // Process --hiveconf
        // Get hiveconf param values and set the System property values
        Properties confProps = commandLine.getOptionProperties("hiveconf");
        for (String propKey : confProps.stringPropertyNames()) {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
          if (propKey.equalsIgnoreCase("hive.root.logger")) {
            CommonCliOptions.splitAndSetLogger(propKey, confProps);
          } else {
            System.setProperty(propKey, confProps.getProperty(propKey));
          }
        }

        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options));
        }

        // Process --deregister
        if (commandLine.hasOption("deregister")) {
          return new ServerOptionsProcessorResponse(new DeregisterOptionExecutor(
              commandLine.getOptionValue("deregister")));
        }
      } catch (ParseException e) {
        // Error out & exit - we were not able to parse the args successfully
        System.err.println("Error starting HiveServer2 with given arguments: ");
        System.err.println(e.getMessage());
        System.exit(-1);
      }
      // Default executor, when no option is specified
      return new ServerOptionsProcessorResponse(new StartOptionExecutor());
    }

    StringBuilder getDebugMessage() {
      return debugMessage;
    }
  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  static class ServerOptionsProcessorResponse {
    private final ServerOptionsExecutor serverOptionsExecutor;

    ServerOptionsProcessorResponse(ServerOptionsExecutor serverOptionsExecutor) {
      this.serverOptionsExecutor = serverOptionsExecutor;
    }

    ServerOptionsExecutor getServerOptionsExecutor() {
      return serverOptionsExecutor;
    }
  }

  /**
   * The executor interface for running the appropriate HiveServer2 command based on parsed options
   */
  static interface ServerOptionsExecutor {
    public void execute();
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  static class HelpOptionExecutor implements ServerOptionsExecutor {
    private final Options options;
    private final String serverName;

    HelpOptionExecutor(String serverName, Options options) {
      this.options = options;
      this.serverName = serverName;
    }

    @Override
    public void execute() {
      new HelpFormatter().printHelp(serverName, options);
      System.exit(0);
    }
  }

  /**
   * StartOptionExecutor: starts HiveServer2.
   * This is the default executor, when no option is specified.
   */
  static class StartOptionExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        startHiveServer2();
      } catch (Throwable t) {
        LOG.error("Error starting HiveServer2", t);
        System.exit(-1);
      }
    }
  }

  /**
   * DeregisterOptionExecutor: executes the --deregister option by deregistering all HiveServer2
   * instances from ZooKeeper of a specific version.
   */
  static class DeregisterOptionExecutor implements ServerOptionsExecutor {
    private final String versionNumber;

    DeregisterOptionExecutor(String versionNumber) {
      this.versionNumber = versionNumber;
    }

    @Override
    public void execute() {
      try {
        deleteServerInstancesFromZooKeeper(versionNumber);
      } catch (Exception e) {
        LOG.error("Error deregistering HiveServer2 instances for version: " + versionNumber
            + " from ZooKeeper", e);
        System.out.println("Error deregistering HiveServer2 instances for version: " + versionNumber
            + " from ZooKeeper." + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }
}
