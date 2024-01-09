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

import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.hive.common.JvmPauseMonitor;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.ZKDeRegisterWatcher;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveServer2TransportMode;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.llap.coordinator.LlapCoordinator;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.events.NotificationEventPoll;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.repl.metric.MetricSink;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.PolicyProviderContainer;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeSynchronizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.session.ClearDanglingScratchDir;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorThread;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.apache.hadoop.hive.registry.impl.ZookeeperUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.hive.http.HttpServer;
import org.apache.hive.http.JdbcJarDownloadServlet;
import org.apache.hive.http.LlapServlet;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.auth.AuthType;
import org.apache.hive.service.auth.saml.HiveSaml2Client;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.servlet.HS2LeadershipStatus;
import org.apache.hive.service.servlet.HS2Peers;
import org.apache.hive.service.servlet.QueriesRESTfulAPIServlet;
import org.apache.hive.service.servlet.QueryProfileServlet;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.servlet.jsp.JspFactory;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Logger LOG = LoggerFactory.getLogger(HiveServer2.class);
  public static final String INSTANCE_URI_CONFIG = "hive.server2.instance.uri";
  private static final int SHUTDOWN_TIME = 60;

  private static CountDownLatch zkDeleteSignal;
  private static volatile KeeperException.Code zkDeleteResultCode;

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private CuratorFramework zKClientForPrivSync = null;
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
  // used for testing
  private SettableFuture<Boolean> isLeaderTestFuture = SettableFuture.create();
  private SettableFuture<Boolean> notLeaderTestFuture = SettableFuture.create();
  private ZooKeeperHiveHelper zooKeeperHelper = null;
  private ScheduledQueryExecutionService scheduledQueryService;

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

  @VisibleForTesting
  public CLIService getCliService() {
    return this.cliService;
  }

  @VisibleForTesting
  public void setPamAuthenticator(PamAuthenticator pamAuthenticator) {
    this.pamAuthenticator = pamAuthenticator;
  }

  @VisibleForTesting
  public SettableFuture<Boolean> getIsLeaderTestFuture() {
    return isLeaderTestFuture;
  }

  @VisibleForTesting
  public SettableFuture<Boolean> getNotLeaderTestFuture() {
    return notLeaderTestFuture;
  }

  private void resetIsLeaderTestFuture() {
    isLeaderTestFuture = SettableFuture.create();
  }

  private void resetNotLeaderTestFuture() {
    notLeaderTestFuture = SettableFuture.create();
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

    // Do not allow sessions - leader election or initialization will allow them for an active HS2.
    cliService = new CLIService(this, false);
    addService(cliService);
    final HiveServer2 hiveServer2 = this;
    boolean isHttpTransportMode = isHttpTransportMode(hiveConf);
    boolean isAllTransportMode = isAllTransportMode(hiveConf);
    if (isHttpTransportMode || isAllTransportMode) {
      thriftCLIService = new ThriftHttpCLIService(cliService);
      addService(thriftCLIService);
    }
    if (!isHttpTransportMode || isAllTransportMode)  {
      thriftCLIService = new ThriftBinaryCLIService(cliService);
      addService(thriftCLIService); //thriftCliService instance is used for zookeeper purposes
    }

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

    // Initialize metadata provider class and trimmer
    CalcitePlanner.warmup();

    try {
      sessionHive = Hive.get(hiveConf);
    } catch (HiveException e) {
      throw new RuntimeException("Failed to get metastore connection", e);
    }

    // Create views registry
    HiveMaterializedViewsRegistry.get().init();

    StatsSources.initialize(hiveConf);

    if (hiveConf.getBoolVar(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED)) {
      scheduledQueryService = ScheduledQueryExecutionService.startScheduledQueryExecutorService(hiveConf);
    }

    // Setup cache if enabled.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED)) {
      try {
        QueryResultsCache.initialize(hiveConf);
      } catch (Exception err) {
        throw new RuntimeException("Error initializing the query results cache", err);
      }
    }

    // setup metastore client cache
    if (hiveConf.getBoolVar(ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(hiveConf);
    }

    try {
      NotificationEventPoll.initialize(hiveConf);
    } catch (Exception err) {
      throw new RuntimeException("Error initializing notification event poll", err);
    }

    wmQueue = hiveConf.get(ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE.varname, "").trim();
    this.zooKeeperAclProvider = getACLProvider(hiveConf);

    this.serviceDiscovery = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY);
    this.activePassiveHA = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE);

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

    try {
      logCompactionParameters(hiveConf);
      maybeStartCompactorThreads(hiveConf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

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
          if (JspFactory.getDefaultFactory() == null) {
            // Set the default JspFactory to avoid NPE while opening the home page
            JspFactory.setDefaultFactory(new org.apache.jasper.runtime.JspFactoryImpl());
          }
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
            if (StringUtils.isBlank(keyStorePath)) {
              throw new IllegalArgumentException(
                ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PATH.varname
                  + " Not configured for SSL connection");
            }
            builder.setKeyStorePassword(ShimLoader.getHadoopShims().getPassword(
              hiveConf, ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_PASSWORD.varname));
            builder.setKeyStorePath(keyStorePath);
            builder.setKeyStoreType(hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYSTORE_TYPE));
            builder.setKeyManagerFactoryAlgorithm(
                hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_KEYMANAGERFACTORY_ALGORITHM));
            builder.setExcludeCiphersuites(
                hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_SSL_EXCLUDE_CIPHERSUITES));
            builder.setUseSSL(true);
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_SPNEGO)) {
            String spnegoPrincipal = hiveConf.getVar(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_PRINCIPAL);
            String spnegoKeytab = hiveConf.getVar(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_KEYTAB);
            if (StringUtils.isBlank(spnegoPrincipal) || StringUtils.isBlank(spnegoKeytab)) {
              throw new IllegalArgumentException(
                ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_PRINCIPAL.varname
                  + "/" + ConfVars.HIVE_SERVER2_WEBUI_SPNEGO_KEYTAB.varname
                  + " Not configured for SPNEGO authentication");
            }
            builder.setSPNEGOPrincipal(spnegoPrincipal);
            builder.setSPNEGOKeytab(spnegoKeytab);
            builder.setUseSPNEGO(true);
          }
          if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_ENABLE_CORS)) {
            builder.setEnableCORS(true);
            String allowedOrigins = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_ORIGINS);
            String allowedMethods = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_METHODS);
            String allowedHeaders = hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_HEADERS);
            if (StringUtils.isBlank(allowedOrigins) || StringUtils.isBlank(allowedMethods) || StringUtils.isBlank(allowedHeaders)) {
              throw new IllegalArgumentException("CORS enabled. But " +
                ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_ORIGINS.varname + "/" +
                ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_METHODS.varname + "/" +
                ConfVars.HIVE_SERVER2_WEBUI_CORS_ALLOWED_HEADERS.varname + "/" +
                " is not configured");
            }
            builder.setAllowedOrigins(allowedOrigins);
            builder.setAllowedMethods(allowedMethods);
            builder.setAllowedHeaders(allowedHeaders);
            LOG.info("CORS enabled - allowed-origins: {} allowed-methods: {} allowed-headers: {}", allowedOrigins,
              allowedMethods, allowedHeaders);
          }
          if(hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_XFRAME_ENABLED)){
            builder.configureXFrame(true).setXFrameOption(hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_XFRAME_VALUE));
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
            builder.setContextAttribute("hs2.failover.callback", new FailoverHandlerCallback(hs2HARegistry));
            builder.setContextAttribute("hiveconf", hiveConf);
            builder.addServlet("leader", HS2LeadershipStatus.class);
            builder.addServlet("peers", HS2Peers.class);
          }
          builder.addServlet("llap", LlapServlet.class);
          builder.addServlet("jdbcjar", JdbcJarDownloadServlet.class);
          builder.setContextRootRewriteTarget("/hiveserver2.jsp");

          webServer = builder.build();
          webServer.addServlet("query_page", "/query_page.html", QueryProfileServlet.class);
          webServer.addServlet("api", "/api/*", QueriesRESTfulAPIServlet.class);
        }
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }

    // Add a shutdown hook for catching SIGTERM & SIGINT
    long timeout = HiveConf.getTimeVar(getHiveConf(),
        HiveConf.ConfVars.HIVE_SERVER2_GRACEFUL_STOP_TIMEOUT, TimeUnit.SECONDS);
    // Extra time for releasing the resources if timeout sets to 0
    ShutdownHookManager.addGracefulShutDownHook(() -> graceful_stop(),  timeout == 0 ? 30 : timeout);
  }

  private void logCompactionParameters(HiveConf hiveConf) {
    LOG.info("Compaction HS2 parameters:");
    String runWorkerIn = MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN);
    LOG.info("hive.metastore.runworker.in = {}", runWorkerIn);
    int numWorkers = MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
    LOG.info("metastore.compactor.worker.threads = {}", numWorkers);
    if ("hs2".equals(runWorkerIn) && numWorkers < 1) {
      LOG.warn("Invalid number of Compactor Worker threads({}) on HS2", numWorkers);
    }
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

  public static boolean isHttpTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if (transportMode != null
        && (transportMode.equalsIgnoreCase(HiveServer2TransportMode.http.toString()))) {
      return true;
    }
    return false;
  }

  public static boolean isAllTransportMode(HiveConf hiveConf) {
    String transportMode = System.getenv("HIVE_SERVER2_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase(HiveServer2TransportMode.all.toString()))) {
      return true;
    }
    return false;
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private ACLProvider zooKeeperAclProvider;

  private ACLProvider getACLProvider(HiveConf hiveConf) {
    final boolean isSecure = ZookeeperUtils.isKerberosEnabled(hiveConf);

    return new ACLProvider() {
      @Override
      public List<ACL> getDefaultAcl() {
        List<ACL> nodeAcls = new ArrayList<ACL>();
        if (isSecure) {
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
    boolean isHttpTransportMode = isHttpTransportMode(hiveConf);
    boolean isAllTransportMode = isAllTransportMode(hiveConf);

    if (isHttpTransportMode || isAllTransportMode) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname,
          Integer.toString(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT)));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));
    }
    if (!isHttpTransportMode || isAllTransportMode) {
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_PORT.varname,
          Integer.toString(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT)));
      confsToPublish.put(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname,
          hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    }
    // Auth specific confs
    confsToPublish.put(ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,
        hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION));
    if (AuthType.isKerberosAuthMode(hiveConf)) {
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
  private static void setUpZooKeeperAuth(HiveConf hiveConf) throws Exception {
    if (ZookeeperUtils.isKerberosEnabled(hiveConf)) {
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

  public int getOpenSessionsCount() {
    return cliService != null ? cliService.getSessionManager().getOpenSessionCount() : 0;
  }

  interface FailoverHandler {
    void failover() throws Exception;
  }

  public static class FailoverHandlerCallback implements FailoverHandler {
    private HS2ActivePassiveHARegistry hs2HARegistry;

    FailoverHandlerCallback(HS2ActivePassiveHARegistry hs2HARegistry) {
      this.hs2HARegistry = hs2HARegistry;
    }

    @Override
    public void failover() throws Exception {
      hs2HARegistry.failover();
    }
  }

  /**
   * The watcher class shuts down the server if there are no more active client
   * sessions at the time of receiving a 'NodeDeleted' notification from ZooKeeper.
   */
  public class DeRegisterWatcher extends ZKDeRegisterWatcher {
    public DeRegisterWatcher(ZooKeeperHiveHelper zooKeeperHiveHelper) {
      super(zooKeeperHiveHelper);
    }

    @Override
    public void process(WatchedEvent event) {
      super.process(event);
      if (event.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
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

  /**
   * @return true if the server instance was deregistered from ZooKeeper, else false. The function might
   * be called even when the instance is not registered with the ZooKeeper (See
   * SessionManage.closeSessionInternal()). In that case, return false since the deregistration has
   * not really happened.
   */
  public boolean isDeregisteredWithZooKeeper() {
    if (serviceDiscovery && !activePassiveHA) {
      if (zooKeeperHelper != null) {
        return zooKeeperHelper.isDeregisteredWithZooKeeper();
      }
    }
    return false;
  }

  public String getServerInstanceURI() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getCanonicalHostName() + ":"
        + thriftCLIService.getPortNumber();
  }

  public String getServerHost() throws Exception {
    if ((thriftCLIService == null) || (thriftCLIService.getServerIPAddress() == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.");
    }
    return thriftCLIService.getServerIPAddress().getCanonicalHostName();
  }

  @Override
  public synchronized void start() {
    super.start();
    // If we're supporting dynamic service discovery, we'll add the service uri for this
    // HiveServer2 instance to Zookeeper as a znode.
    HiveConf hiveConf = getHiveConf();
    if (!serviceDiscovery || !activePassiveHA) {
      allowClientSessions();
    }
    if (serviceDiscovery) {
      try {
        if (activePassiveHA) {
          hs2HARegistry.registerLeaderLatchListener(leaderLatchListener, leaderActionsExecutorService);
          hs2HARegistry.start();
          LOG.info("HS2 HA registry started");
        } else {
          boolean publishConfigs =
                  hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_PUBLISH_CONFIGS);
          String instanceURI = getServerInstanceURI();
          String znodeData;
          if (publishConfigs) {
            // HiveServer2 configs that this instance will publish to ZooKeeper,
            // so that the clients can read these and configure themselves properly.
            addConfsToPublish(hiveConf, confsToPublish, getServerInstanceURI());
            znodeData = Joiner.on(';').withKeyValueSeparator("=").join(confsToPublish);
          } else {
            znodeData = instanceURI;
          }

          // Add a Znode to the specified ZooKeeper with name: serverUri=host:port;
          // version=versionInfo; sequence=sequenceNumber
          zooKeeperHelper = hiveConf.getZKConfig();
          String znodePathPrefix = "serverUri=" + instanceURI + ";" +
                  "version=" + HiveVersionInfo.getVersion() + ";" + "sequence=";
          zooKeeperHelper.addServerInstanceToZooKeeper(znodePathPrefix, znodeData,
                  zooKeeperAclProvider, new DeRegisterWatcher(zooKeeperHelper));
        }
      } catch (Exception e) {
        LOG.error("Error adding this HiveServer2 instance to ZooKeeper: ", e);
        throw new ServiceException(e);
      }
    }

    try {
      startPrivilegeSynchronizer(hiveConf);
    } catch (Exception e) {
      LOG.error("Error starting priviledge synchronizer: ", e);
      throw new ServiceException(e);
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

    if (hiveConf.getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      if (!activePassiveHA) {
        LOG.info("HS2 interactive HA not enabled. Starting tez sessions..");
        try {
          startOrReconnectTezSessions();
        } catch (Exception e) {
          LOG.error("Error starting  Tez sessions: ", e);
          throw new ServiceException(e);
        }
      } else {
        LOG.info("HS2 interactive HA enabled. Tez sessions will be started/reconnected by the leader.");
      }
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
      hiveServer2.allowClientSessions();

      // resolve futures used for testing
      if (HiveConf.getBoolVar(hiveServer2.getHiveConf(), ConfVars.HIVE_IN_TEST)) {
        hiveServer2.isLeaderTestFuture.set(true);
        hiveServer2.resetNotLeaderTestFuture();
      }
    }

    @Override
    public void notLeader() {
      LOG.info("HS2 instance {} LOST LEADERSHIP. Stopping/Disconnecting tez sessions..", hiveServer2.serviceUri);
      hiveServer2.isLeader.set(false);
      hiveServer2.closeAndDisallowHiveSessions();
      hiveServer2.stopOrDisconnectTezSessions();
      LOG.info("Stopped/Disconnected tez sessions.");

      // resolve futures used for testing
      if (HiveConf.getBoolVar(hiveServer2.getHiveConf(), ConfVars.HIVE_IN_TEST)) {
        hiveServer2.notLeaderTestFuture.set(true);
        hiveServer2.resetIsLeaderTestFuture();
      }
    }
  }

  private void startOrReconnectTezSessions() {
    LOG.info("Starting/Reconnecting tez sessions..");
    // TODO: add tez session reconnect after TEZ-3875
    WMFullResourcePlan resourcePlan;
    try {
      resourcePlan = sessionHive.getActiveResourcePlan();
    } catch (HiveException e) {
      if (!HiveConf.getBoolVar(getHiveConf(), ConfVars.HIVE_IN_TEST_SSL)) {
        throw new RuntimeException(e);
      } else {
        resourcePlan = null; // Ignore errors in SSL tests where the connection is misconfigured.
      }
    }

    if (resourcePlan == null && HiveConf.getBoolVar(
      getHiveConf(), ConfVars.HIVE_IN_TEST)) {
      LOG.info("Creating a default resource plan for test");
      resourcePlan = createTestResourcePlan();
    }
    initAndStartTezSessionPoolManager(resourcePlan);
    initAndStartWorkloadManager(resourcePlan);
  }

  private void allowClientSessions() {
    cliService.getSessionManager().allowSessions(true);
  }

  private void initAndStartTezSessionPoolManager(final WMFullResourcePlan resourcePlan) {
    // starting Tez session pool in start here to let parent session state initialize on CliService state, to avoid
    // SessionState.get() return null during createTezDir
    try {
      // will be invoked anyway in TezTask. Doing it early to initialize triggers for non-pool tez session.
      LOG.info("Initializing tez session pool manager. Active resource plan: {}",
        resourcePlan == null || resourcePlan.getPlan() == null ? "null" : resourcePlan.getPlan().getName());
      tezSessionPoolManager = TezSessionPoolManager.getInstance();
      HiveConf hiveConf = getHiveConf();
      if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS)) {
        tezSessionPoolManager.setupPool(hiveConf);
      } else {
        tezSessionPoolManager.setupNonPool(hiveConf);
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
      LOG.info("Initializing workload management. Active resource plan: {}",
        resourcePlan == null || resourcePlan.getPlan() == null ? "null" : resourcePlan.getPlan().getName());
      try {
        wm = WorkloadManager.create(wmQueue, getHiveConf(), resourcePlan);
        wm.start();
        LOG.info("Workload manager initialized.");
      } catch (Exception e) {
        throw new ServiceException("Unable to instantiate and start Workload Manager", e);
      }
    } else {
      LOG.info("Workload management is not enabled as {} config is not set",
        ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE.varname);
    }
  }

  private void closeAndDisallowHiveSessions() {
    LOG.info("Closing all open hive sessions.");
    if (cliService == null) {
      return;
    }
    cliService.getSessionManager().allowSessions(false);
    // No sessions can be opened after the above call. Close the existing ones if any.
    try {
      for (HiveSession session : cliService.getSessionManager().getSessions()) {
        cliService.getSessionManager().closeSession(session.getSessionHandle());
      }
      LOG.info("Closed all open hive sessions");
    } catch (HiveSQLException e) {
      LOG.error("Unable to close all open sessions.", e);
    }
  }

  private void stopOrDisconnectTezSessions() {
    LOG.info("Stopping/Disconnecting tez sessions.");
    // There should already be an instance of the session pool manager.
    // If not, ignoring is fine while stopping HiveServer2.
    if (tezSessionPoolManager != null) {
      try {
        tezSessionPoolManager.stop();
        LOG.info("Stopped tez session pool manager.");
      } catch (Exception e) {
        LOG.error("Error while stopping tez session pool manager.", e);
      }
    }
    if (wm != null) {
      try {
        wm.stop();
        LOG.info("Stopped workload manager.");
      } catch (Exception e) {
        LOG.error("Error while stopping workload manager.", e);
      }
    }
  }

  /**
   * Decommission HiveServer2. As a consequence, SessionManager stops
   * opening new sessions, OperationManager refuses running new queries and
   * HiveServer2 deregisters itself from Zookeeper if service discovery is enabled,
   * but the decommissioning has no effect on the current running queries.
   */
  public synchronized void decommission() {
    LOG.info("Decommissioning HiveServer2");
    // Remove this server instance from ZooKeeper if dynamic service discovery is set
    if (zooKeeperHelper != null && !isDeregisteredWithZooKeeper()) {
      try {
        zooKeeperHelper.removeServerInstanceFromZooKeeper();
      } catch (Exception e) {
        LOG.error("Error removing znode for this HiveServer2 instance from ZooKeeper.", e);
      }
    }
    String pidDir = StringUtils.defaultIfEmpty(System.getenv("HIVESERVER2_PID_DIR"),
        System.getenv("HIVE_CONF_DIR"));
    if (StringUtils.isNotEmpty(pidDir)) {
      File pidFile = new File(pidDir, "hiveserver2.pid");
      LOG.info("Deleting the tmp HiveServer2 pid file: {}", pidFile);
      FileUtils.deleteQuietly(pidFile);
    }
    super.decommission();
  }

  public synchronized void graceful_stop() {
    try {
      decommission();
      // Need 30s for stop() to release server's resources
      long maxTimeForWait = HiveConf.getTimeVar(getHiveConf(),
          HiveConf.ConfVars.HIVE_SERVER2_GRACEFUL_STOP_TIMEOUT, TimeUnit.MILLISECONDS) - 30000;
      long timeout = maxTimeForWait, startTime = System.currentTimeMillis();
      try {
        // The service should be started before when reaches here, as decommissioning would throw
        // IllegalStateException otherwise, so both cliService and sessionManager should not be null.
        while (timeout > 0 && !getCliService().getSessionManager().getOperations().isEmpty()) {
          // For gracefully stopping, sleeping some time while looping does not bring much overhead,
          // that is, at most 100ms are wasted for waiting for OperationManager to be done,
          // and this code path will only be executed when HS2 is being terminated.
          Thread.sleep(Math.min(100, timeout));
          timeout = maxTimeForWait + startTime - System.currentTimeMillis();
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for all live operations to be done");
        Thread.currentThread().interrupt();
      }
      LOG.info("Spent {}ms waiting for live operations to be done, current live operations: {}"
          , System.currentTimeMillis() - startTime
          , getCliService().getSessionManager().getOperations().size());
    } finally {
      stop();
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down HiveServer2");
    HiveConf hiveConf = this.getHiveConf();
    super.stop();
    if (scheduledQueryService != null) {
      try {
        scheduledQueryService.close();
      } catch (Exception e) {
        LOG.error("Error stopping schq", e);
      }
    }
    //Shutdown metric collection
    MetricSink.getInstance().tearDown();
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
        if (zooKeeperHelper != null) {
          zooKeeperHelper.removeServerInstanceFromZooKeeper();
        }
      } catch (Exception e) {
        LOG.error("Error removing znode for this HiveServer2 instance from ZooKeeper.", e);
      }
    }

    stopOrDisconnectTezSessions();

    if (zKClientForPrivSync != null) {
      zKClientForPrivSync.close();
    }
    if (hiveConf != null && AuthType.isSamlAuthMode(hiveConf)) {
      // this is mostly for testing purposes to make sure that SAML client is
      // reinitialized after a HS2 is restarted.
      HiveSaml2Client.shutdown();
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
          HiveConf.getVar(hiveConf, HiveConf.ConfVars.SCRATCH_DIR), hiveConf), initialWaitInSec,
          HiveConf.getTimeVar(hiveConf, ConfVars.HIVE_SERVER2_CLEAR_DANGLING_SCRATCH_DIR_INTERVAL,
          TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
  }

  public void startPrivilegeSynchronizer(HiveConf hiveConf) throws Exception {

    if (!HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_PRIVILEGE_SYNCHRONIZER)) {
      return;
    }
    PolicyProviderContainer policyContainer = new PolicyProviderContainer();
    HiveAuthorizer authorizer = SessionState.get().getAuthorizerV2();
    if (authorizer.getHivePolicyProvider() != null) {
      policyContainer.addAuthorizer(authorizer);
    }
    if (MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS) != null &&
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS).contains(
        "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener") &&
        MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.HIVE_AUTHORIZATION_MANAGER)!= null) {
      List<HiveMetastoreAuthorizationProvider> providers = HiveUtils.getMetaStoreAuthorizeProviderManagers(
          hiveConf, HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER, SessionState.get().getAuthenticator());
      for (HiveMetastoreAuthorizationProvider provider : providers) {
        if (provider.getHivePolicyProvider() != null) {
          policyContainer.addAuthorizationProvider(provider);
        }
      }
    }

    if (policyContainer.size() > 0) {
      setUpZooKeeperAuth(hiveConf);
      zKClientForPrivSync = hiveConf.getZKConfig().startZookeeperClient(zooKeeperAclProvider, true);
      String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
      String path = ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
          + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + "leader";
      LeaderLatch privilegeSynchronizerLatch = new LeaderLatch(zKClientForPrivSync, path);
      privilegeSynchronizerLatch.start();
      LOG.info("Find " + policyContainer.size() + " policy to synchronize, start PrivilegeSynchronizer");
      Thread privilegeSynchronizerThread = new Thread(
          new PrivilegeSynchronizer(privilegeSynchronizerLatch, policyContainer, hiveConf), "PrivilegeSynchronizer");
      privilegeSynchronizerThread.setDaemon(true);
      privilegeSynchronizerThread.start();
    } else {
      LOG.warn(
          "No policy provider found, skip creating PrivilegeSynchronizer");
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

  private void maybeStartCompactorThreads(HiveConf hiveConf) throws Exception {
    if (MetastoreConf.getVar(hiveConf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN).equals("hs2")) {
      int numWorkers = MetastoreConf.getIntVar(hiveConf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
      List<Map.Entry<String, String>> entries = hiveConf.getMatchingEntries(Constants.COMPACTION_POOLS_PATTERN);

      StringBuilder sb = new StringBuilder(2048);
      sb.append("This HS2 instance will act as Compactor with the following worker pool configuration:\n");
      sb.append("Global pool size: ").append(numWorkers).append("\n");

      LOG.info("Initializing the compaction pools with using the global worker limit: {} ", numWorkers);
      while (numWorkers > 0 && entries.size() > 0) {
        Map.Entry<String, String> entry = entries.remove(0);
        String poolName = entry.getValue();
        int poolWorkers = hiveConf.getInt(entry.getKey(), 0);

        if (poolWorkers == 0) {
          LOG.warn("Compaction pool ({}) configured with zero workers. Skipping pool initialization", poolName);
          sb.append("Pool not initialized, 0 size: ").append(poolName).append("\n");
          continue;
        }
        if (poolWorkers > numWorkers) {
          LOG.warn("Global worker pool exhausted, compaction pool ({}) will be configured with less workers than the " +
              "required number. ({} -> {})", poolName, poolWorkers, numWorkers);
          poolWorkers = numWorkers;
        }

        LOG.info("Initializing compaction pool ({}) with {} workers.", poolName, poolWorkers);
        for (int i = 0; i < poolWorkers; i++) {
          Worker w = new Worker();
          w.setPoolName(poolName);
          CompactorThread.initializeAndStartThread(w, hiveConf);
          sb.append("Worker - Name: ").append(w.getName()).append(", Pool: ").append(poolName)
              .append(", Priority: ").append(w.getPriority()).append("\n");
        }
        numWorkers -= poolWorkers;
      }

      if (numWorkers == 0) {
        LOG.warn("No default compaction pool configured, all non-labeled compaction requests will remain unprocessed!");
        if (entries.size() > 0) {
          for (Map.Entry<String, String> entry : entries) {
            String poolName = entry.getValue();
            LOG.warn("There are no available workers for the following compaction pool: {} ", poolName);
            sb.append("Pool not initialized, no remaining free workers: ").append(poolName).append("\n");
          }
          sb.append("Pool not initialized, no remaining free workers: default\n");
        }
      } else {
        LOG.info("Initializing default compaction pool with {} workers.", numWorkers);
        for (int i = 0; i < numWorkers; i++) {
          Worker w = new Worker();
          CompactorThread.initializeAndStartThread(w, hiveConf);
          sb.append("Worker - Name: ").append(w.getName()).append(", Pool: default, Priority: ").append(w.getPriority()).append("\n");
        }
      }
      LOG.info(sb.toString());
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
    setUpZooKeeperAuth(hiveConf);
    CuratorFramework zooKeeperClient = hiveConf.getZKConfig().getNewZookeeperClient();
    zooKeeperClient.start();
    String rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
    List<String> znodePaths =
        zooKeeperClient.getChildren().forPath(
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
    List<String> znodePathsUpdated;
    // Now for each path that is for the given versionNumber, delete the znode from ZooKeeper
    for (int i = 0; i < znodePaths.size(); i++) {
      String znodePath = znodePaths.get(i);
      zkDeleteSignal = new CountDownLatch(1);
      if (znodePath.contains("version=" + versionNumber + ";")) {
        String fullZnodePath =
            ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace
                + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + znodePath;
        LOG.warn("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        System.out.println("Will attempt to remove the znode: " + fullZnodePath + " from ZooKeeper");
        zooKeeperClient.delete().guaranteed().inBackground(new DeleteCallBack())
            .forPath(fullZnodePath);
        // Wait for the delete to complete
        zkDeleteSignal.await();
        final KeeperException.Code rc = HiveServer2.zkDeleteResultCode;
        if (rc != KeeperException.Code.OK) {
          throw KeeperException.create(rc);
        }
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
        zkDeleteResultCode = KeeperException.Code.get(event.getResultCode());
        zkDeleteSignal.countDown();
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
      // --listHAPeers
      options.addOption(OptionBuilder
        .hasArgs(0)
        .withLongOpt("listHAPeers")
        .withDescription("List all HS2 instances when running in Active Passive HA mode")
        .create());
      // --failover <workerIdentity>
      options.addOption(OptionBuilder
        .hasArgs(1)
        .withArgName("workerIdentity")
        .withLongOpt("failover")
        .withDescription("Manually failover Active HS2 instance to passive standby mode")
        .create());
      // --graceful_stop
      options.addOption(OptionBuilder
        .hasArgs(1)
        .isRequired(false)
        .withArgName("pid")
        .withLongOpt("graceful_stop")
        .withDescription("Gracefully stopping HS2 instance of" +
            " 'pid'(default: $HIVE_CONF_DIR/hiveserver2.pid) in 'timeout'(default:1800) seconds.")
        .create());
      // --getHiveConf <key>
      options.addOption(OptionBuilder
        .hasArg(true)
        .withArgName("key")
        .withLongOpt("getHiveConf")
        .withDescription("Get the value of key from HiveConf")
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
          if ("hive.log.file".equals(propKey) ||
              "hive.log.dir".equals(propKey) ||
              "hive.root.logger".equals(propKey)) {
            throw new IllegalArgumentException("Logs will be split in two "
                + "files if the commandline argument " + propKey + " is "
                + "used. To prevent this use to HADOOP_CLIENT_OPTS -D"
                + propKey + "=" + confProps.getProperty(propKey)
                + " or use the set the value in the configuration file"
                + " (see HIVE-19886)");
          }
          HiveConf.overrides.put(propKey, confProps.getProperty(propKey));
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

        // Process --listHAPeers
        if (commandLine.hasOption("listHAPeers")) {
          return new ServerOptionsProcessorResponse(new ListHAPeersExecutor());
        }

        // Process --failover
        if (commandLine.hasOption("failover")) {
          return new ServerOptionsProcessorResponse(new FailoverHS2InstanceExecutor(
            commandLine.getOptionValue("failover")
          ));
        }

        // Process --getHiveConf
        if (commandLine.hasOption("getHiveConf")) {
          return new ServerOptionsProcessorResponse(() -> {
            String key = commandLine.getOptionValue("getHiveConf");
            HiveConf hiveConf = new HiveConf();
            HiveConf.ConfVars confVars = HiveConf.getConfVars(key);
            String value = hiveConf.get(key);
            if (confVars != null && confVars.getValidator() instanceof Validator.TimeValidator) {
              Validator.TimeValidator validator = (Validator.TimeValidator) confVars.getValidator();
              value = HiveConf.getTimeVar(hiveConf, confVars, validator.getTimeUnit()) + "";
            }
            System.out.println(key + "=" + value);
          });
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

  /**
   * Handler for --failover <workerIdentity> command. The way failover works is,
   * - the client gets <workerIdentity> from user input
   * - the client uses HS2 HA registry to get list of HS2 instances and finds the one that matches <workerIdentity>
   * - if there is a match, client makes sure the instance is a leader (only leader can failover)
   * - if the matched instance is a leader, its web endpoint is obtained from service record then http DELETE method
   *   is invoked on /leader endpoint (Yes. Manual failover requires web UI to be enabled)
   * - the webpoint checks if admin ACLs are set, if so will close and restart the leader latch triggering a failover
   */
  static class FailoverHS2InstanceExecutor implements ServerOptionsExecutor {
    private final String workerIdentity;

    FailoverHS2InstanceExecutor(String workerIdentity) {
      this.workerIdentity = workerIdentity;
    }

    @Override
    public void execute() {
      try {
        HiveConf hiveConf = new HiveConf();
        HS2ActivePassiveHARegistry haRegistry = HS2ActivePassiveHARegistryClient.getClient(hiveConf);
        Collection<HiveServer2Instance> hs2Instances = haRegistry.getAll();
        // no HS2 instances are running
        if (hs2Instances.isEmpty()) {
          LOG.error("No HiveServer2 instances are running in HA mode");
          System.err.println("No HiveServer2 instances are running in HA mode");
          System.exit(-1);
        }
        HiveServer2Instance targetInstance = null;
        for (HiveServer2Instance instance : hs2Instances) {
          if (instance.getWorkerIdentity().equals(workerIdentity)) {
            targetInstance = instance;
            break;
          }
        }
        // no match for workerIdentity
        if (targetInstance == null) {
          LOG.error("Cannot find any HiveServer2 instance with workerIdentity: " + workerIdentity);
          System.err.println("Cannot find any HiveServer2 instance with workerIdentity: " + workerIdentity);
          System.exit(-1);
        }
        // only one HS2 instance available (cannot failover)
        if (hs2Instances.size() == 1) {
          LOG.error("Only one HiveServer2 instance running in thefail cluster. Cannot failover: " + workerIdentity);
          System.err.println("Only one HiveServer2 instance running in the cluster. Cannot failover: " + workerIdentity);
          System.exit(-1);
        }
        // matched HS2 instance is not leader
        if (!targetInstance.isLeader()) {
          LOG.error("HiveServer2 instance (workerIdentity: " + workerIdentity + ") is not a leader. Cannot failover");
          System.err.println("HiveServer2 instance (workerIdentity: " + workerIdentity + ") is not a leader. Cannot failover");
          System.exit(-1);
        }

        String webPort = targetInstance.getProperties().get(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname);
        // web port cannot be obtained
        if (StringUtils.isEmpty(webPort)) {
          LOG.error("Unable to determine web port for instance: " + workerIdentity);
          System.err.println("Unable to determine web port for instance: " + workerIdentity);
          System.exit(-1);
        }

        // invoke DELETE /leader endpoint for failover
        String webEndpoint = "http://" + targetInstance.getHost() + ":" + webPort + "/leader";
        HttpDelete httpDelete = new HttpDelete(webEndpoint);
        CloseableHttpResponse httpResponse = null;
        try (
          CloseableHttpClient client = HttpClients.createDefault();
        ) {
          int statusCode = -1;
          String response = "Response unavailable";
          httpResponse = client.execute(httpDelete);
          if (httpResponse != null) {
            StatusLine statusLine = httpResponse.getStatusLine();
            if (statusLine != null) {
              response = httpResponse.getStatusLine().getReasonPhrase();
              statusCode = httpResponse.getStatusLine().getStatusCode();

              if (statusCode == 200) {
                System.out.println(EntityUtils.toString(httpResponse.getEntity()));
              }
            }
          }

          if (statusCode != 200) {
            // Failover didn't succeed - log error and exit
            LOG.error("Unable to failover HiveServer2 instance: " + workerIdentity +
                ". status code: " + statusCode + "error: " + response);
            System.err.println("Unable to failover HiveServer2 instance: " + workerIdentity +
                ". status code: " + statusCode + " error: " + response);
            System.exit(-1);
          }
        } finally {
          if (httpResponse != null) {
            httpResponse.close();
          }
        }
      } catch (IOException e) {
        LOG.error("Error listing HiveServer2 HA instances from ZooKeeper", e);
        System.err.println("Error listing HiveServer2 HA instances from ZooKeeper" + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }

  static class ListHAPeersExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        HiveConf hiveConf = new HiveConf();
        HS2ActivePassiveHARegistry haRegistry = HS2ActivePassiveHARegistryClient.getClient(hiveConf);
        HS2Peers.HS2Instances hs2Instances = new HS2Peers.HS2Instances(haRegistry.getAll());
        String jsonOut = hs2Instances.toJson();
        System.out.println(jsonOut);
      } catch (IOException e) {
        LOG.error("Error listing HiveServer2 HA instances from ZooKeeper", e);
        System.err.println("Error listing HiveServer2 HA instances from ZooKeeper" + e);
        System.exit(-1);
      }
      System.exit(0);
    }
  }
}
