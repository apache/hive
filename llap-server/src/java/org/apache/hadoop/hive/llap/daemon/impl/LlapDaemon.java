/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl;

import org.apache.hadoop.hive.llap.LlapOutputFormatService;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.configuration.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.QueryFailedHandler;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.services.impl.LlapWebServices;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge.UdfWhitelistChecker;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class LlapDaemon extends CompositeService implements ContainerRunner, LlapDaemonMXBean {

  private static final Logger LOG = LoggerFactory.getLogger(LlapDaemon.class);

  public static final String LOG4j2_PROPERTIES_FILE = "llap-daemon-log4j2.properties";
  public static final String LLAP_HADOOP_METRICS2_PROPERTIES_FILE = "hadoop-metrics2-llapdaemon.properties";
  public static final String HADOOP_METRICS2_PROPERTIES_FILE = "hadoop-metrics2.properties";
  private final Configuration shuffleHandlerConf;
  private final LlapProtocolServerImpl server;
  private final ContainerRunnerImpl containerRunner;
  private final AMReporter amReporter;
  private final LlapRegistryService registry;
  private final LlapWebServices webServices;
  private final AtomicLong numSubmissions = new AtomicLong(0);
  private final JvmPauseMonitor pauseMonitor;
  private final ObjectName llapDaemonInfoBean;
  private final LlapDaemonExecutorMetrics metrics;
  private final FunctionLocalizer fnLocalizer;

  // Parameters used for JMX
  private final boolean llapIoEnabled;
  private final long executorMemoryPerInstance;
  private final long ioMemoryPerInstance;
  private final int numExecutors;
  private final long maxJvmMemory;
  private final String[] localDirs;
  private final DaemonId daemonId;

  // TODO Not the best way to share the address
  private final AtomicReference<InetSocketAddress> srvAddress = new AtomicReference<>(),
      mngAddress = new AtomicReference<>();
  private final AtomicReference<Integer> shufflePort = new AtomicReference<>();

  public LlapDaemon(Configuration daemonConf, int numExecutors, long executorMemoryBytes,
      boolean ioEnabled, boolean isDirectCache, long ioMemoryBytes, String[] localDirs, int srvPort,
      int mngPort, int shufflePort, int webPort, String appName) {
    super("LlapDaemon");

    initializeLogging(daemonConf);

    printAsciiArt();

    Preconditions.checkArgument(numExecutors > 0);
    Preconditions.checkArgument(srvPort == 0 || (srvPort > 1024 && srvPort < 65536),
        "Server RPC Port must be between 1025 and 65535, or 0 automatic selection");
    Preconditions.checkArgument(mngPort == 0 || (mngPort > 1024 && mngPort < 65536),
        "Management RPC Port must be between 1025 and 65535, or 0 automatic selection");
    Preconditions.checkArgument(localDirs != null && localDirs.length > 0,
        "Work dirs must be specified");
    Preconditions.checkArgument(shufflePort == 0 || (shufflePort > 1024 && shufflePort < 65536),
        "Shuffle Port must be betwee 1024 and 65535, or 0 for automatic selection");
    int outputFormatServicePort = HiveConf.getIntVar(daemonConf, HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT);
    Preconditions.checkArgument(outputFormatServicePort == 0
        || (outputFormatServicePort > 1024 && outputFormatServicePort < 65536),
        "OutputFormatService Port must be between 1024 and 65535, or 0 for automatic selection");
    String hosts = HiveConf.getTrimmedVar(daemonConf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    if (hosts.startsWith("@")) {
      String zkHosts = HiveConf.getTrimmedVar(daemonConf, ConfVars.HIVE_ZOOKEEPER_QUORUM);
      LOG.info("Zookeeper Quorum: {}", zkHosts);
      Preconditions.checkArgument(zkHosts != null && !zkHosts.trim().isEmpty(),
          "LLAP service hosts startswith '@' but hive.zookeeper.quorum is not set." +
              " hive.zookeeper.quorum must be set.");
    }
    String hostName = MetricsUtils.getHostName();
    try {
      daemonId = new DaemonId(UserGroupInformation.getCurrentUser().getShortUserName(),
          LlapUtil.generateClusterName(daemonConf), hostName, appName, System.currentTimeMillis());
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }


    this.maxJvmMemory = getTotalHeapSize();
    this.llapIoEnabled = ioEnabled;
    this.executorMemoryPerInstance = executorMemoryBytes;
    this.ioMemoryPerInstance = ioMemoryBytes;
    this.numExecutors = numExecutors;
    this.localDirs = localDirs;

    int waitQueueSize = HiveConf.getIntVar(
        daemonConf, ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE);
    boolean enablePreemption = HiveConf.getBoolVar(
        daemonConf, ConfVars.LLAP_DAEMON_TASK_SCHEDULER_ENABLE_PREEMPTION);
    LOG.warn("Attempting to start LlapDaemonConf with the following configuration: " +
        "numExecutors=" + numExecutors +
        ", rpcListenerPort=" + srvPort +
        ", mngListenerPort=" + mngPort +
        ", webPort=" + webPort +
        ", outputFormatSvcPort=" + outputFormatServicePort +
        ", workDirs=" + Arrays.toString(localDirs) +
        ", shufflePort=" + shufflePort +
        ", executorMemory=" + executorMemoryBytes +
        ", llapIoEnabled=" + ioEnabled +
        ", llapIoCacheIsDirect=" + isDirectCache +
        ", llapIoCacheSize=" + ioMemoryBytes +
        ", jvmAvailableMemory=" + maxJvmMemory +
        ", waitQueueSize= " + waitQueueSize +
        ", enablePreemption= " + enablePreemption);

    long memRequired =
        executorMemoryBytes + (ioEnabled && isDirectCache == false ? ioMemoryBytes : 0);
    // TODO: this check is somewhat bogus as the maxJvmMemory != Xmx parameters (see annotation in LlapServiceDriver)
    Preconditions.checkState(maxJvmMemory >= memRequired,
        "Invalid configuration. Xmx value too small. maxAvailable=" + maxJvmMemory +
            ", configured(exec + io if enabled)=" +
            memRequired);

    this.shuffleHandlerConf = new Configuration(daemonConf);
    this.shuffleHandlerConf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, shufflePort);
    this.shuffleHandlerConf.set(ShuffleHandler.SHUFFLE_HANDLER_LOCAL_DIRS,
        StringUtils.arrayToString(localDirs));
    this.shuffleHandlerConf.setBoolean(ShuffleHandler.SHUFFLE_DIR_WATCHER_ENABLED,
        HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_DAEMON_SHUFFLE_DIR_WATCHER_ENABLED));

    // Less frequently set parameter, not passing in as a param.
    int numHandlers = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_RPC_NUM_HANDLERS);

    // Initialize the function localizer.
    ClassLoader executorClassLoader = null;
    if (HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_DAEMON_DOWNLOAD_PERMANENT_FNS)) {
      this.fnLocalizer = new FunctionLocalizer(daemonConf, localDirs[0]);
      executorClassLoader = fnLocalizer.getClassLoader();
      // Set up the hook that will disallow creating non-whitelisted UDFs anywhere in the plan.
      // We are not using a specific hook for GenericUDFBridge - that doesn't work in MiniLlap
      // because the daemon is embedded, so the client also gets this hook and Kryo is brittle.
      SerializationUtilities.setGlobalHook(new LlapGlobalUdfChecker(fnLocalizer));
    } else {
      this.fnLocalizer = null;
      SerializationUtilities.setGlobalHook(new LlapGlobalUdfChecker(new StaticPermanentFunctionChecker(daemonConf)));
      executorClassLoader = Thread.currentThread().getContextClassLoader();
    }

    // Initialize the metrics system
    LlapMetricsSystem.initialize("LlapDaemon");
    this.pauseMonitor = new JvmPauseMonitor(daemonConf);
    pauseMonitor.start();
    String displayName = "LlapDaemonExecutorMetrics-" + hostName;
    String sessionId = MetricsUtils.getUUID();
    daemonConf.set("llap.daemon.metrics.sessionid", sessionId);
    String[] strIntervals = HiveConf.getTrimmedStringsVar(daemonConf,
        HiveConf.ConfVars.LLAP_DAEMON_TASK_PREEMPTION_METRICS_INTERVALS);
    List<Integer> intervalList = new ArrayList<>();
    if (strIntervals != null) {
      for (String strInterval : strIntervals) {
        try {
          intervalList.add(Integer.valueOf(strInterval));
        } catch (NumberFormatException e) {
          LOG.warn("Ignoring task pre-emption metrics interval {} from {} as it is invalid",
              strInterval, Arrays.toString(strIntervals));
        }
      }
    }
    this.metrics = LlapDaemonExecutorMetrics.create(displayName, sessionId, numExecutors,
        Ints.toArray(intervalList));
    this.metrics.setMemoryPerInstance(executorMemoryBytes);
    this.metrics.setCacheMemoryPerInstance(ioMemoryBytes);
    this.metrics.setJvmMaxMemory(maxJvmMemory);
    this.metrics.setWaitQueueSize(waitQueueSize);
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    this.llapDaemonInfoBean = MBeans.register("LlapDaemon", "LlapDaemonInfo", this);
    LOG.info("Started LlapMetricsSystem with displayName: " + displayName +
        " sessionId: " + sessionId);

    this.amReporter = new AMReporter(srvAddress, new QueryFailedHandlerProxy(), daemonConf);

    this.server = new LlapProtocolServerImpl(
        numHandlers, this, srvAddress, mngAddress, srvPort, mngPort, daemonId);

    this.containerRunner = new ContainerRunnerImpl(daemonConf, numExecutors, waitQueueSize,
        enablePreemption, localDirs, this.shufflePort, srvAddress, executorMemoryBytes, metrics,
        amReporter, executorClassLoader, daemonId);
    addIfService(containerRunner);

    // Not adding the registry as a service, since we need to control when it is initialized - conf used to pickup properties.
    this.registry = new LlapRegistryService(true);

    if (HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.HIVE_IN_TEST)) {
      this.webServices = null;
    } else {
      this.webServices = new LlapWebServices(webPort, this, registry);
      addIfService(webServices);
    }
    // Bring up the server only after all other components have started.
    addIfService(server);
    // AMReporter after the server so that it gets the correct address. It knows how to deal with
    // requests before it is started.
    addIfService(amReporter);
  }

  private void initializeLogging(final Configuration conf) {
    long start = System.currentTimeMillis();
    URL llap_l4j2 = LlapDaemon.class.getClassLoader().getResource(LOG4j2_PROPERTIES_FILE);
    if (llap_l4j2 != null) {
      final boolean async = LogUtils.checkAndSetAsyncLogging(conf);
      Configurator.initialize("LlapDaemonLog4j2", llap_l4j2.toString());
      long end = System.currentTimeMillis();
      LOG.warn("LLAP daemon logging initialized from {} in {} ms. Async: {}",
          llap_l4j2, (end - start), async);
    } else {
      throw new RuntimeException("Log initialization failed." +
          " Unable to locate " + LOG4j2_PROPERTIES_FILE + " file in classpath");
    }
  }

  public static long getTotalHeapSize() {
    // runtime.getMax() gives a very different number from the actual Xmx sizing.
    // you can iterate through the
    // http://docs.oracle.com/javase/7/docs/api/java/lang/management/MemoryPoolMXBean.html
    // from java.lang.management to figure this out, but the hard-coded params in the llap run.sh
    // result in 89% usable heap (-XX:NewRatio=8) + a survivor region which is technically not
    // in the usable space.

    long total = 0;
    for (MemoryPoolMXBean mp : ManagementFactory.getMemoryPoolMXBeans()) {
      long sz = mp.getUsage().getMax();
      if (mp.getName().contains("Survivor")) {
        sz *= 2; // there are 2 survivor spaces
      }
      if (mp.getType().equals(MemoryType.HEAP)) {
        total += sz;
      }
    }
    // round up to the next MB
    total += (total % (1024*1024));
    return total;
  }

  private void printAsciiArt() {
    final String asciiArt = "" +
        "$$\\       $$\\        $$$$$$\\  $$$$$$$\\\n" +
        "$$ |      $$ |      $$  __$$\\ $$  __$$\\\n" +
        "$$ |      $$ |      $$ /  $$ |$$ |  $$ |\n" +
        "$$ |      $$ |      $$$$$$$$ |$$$$$$$  |\n" +
        "$$ |      $$ |      $$  __$$ |$$  ____/\n" +
        "$$ |      $$ |      $$ |  $$ |$$ |\n" +
        "$$$$$$$$\\ $$$$$$$$\\ $$ |  $$ |$$ |\n" +
        "\\________|\\________|\\__|  \\__|\\__|\n" +
        "\n";
    LOG.warn("\n\n" + asciiArt);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    LlapProxy.setDaemon(true);
    if (fnLocalizer != null) {
      fnLocalizer.init();
      fnLocalizer.startLocalizeAllFunctions();
    }
    if (isIoEnabled()) {
      LlapProxy.initializeLlapIo(conf);
    }
  }

  @Override
  public void serviceStart() throws Exception {
    // Start the Shuffle service before the listener - until it's a service as well.
    ShuffleHandler.initializeAndStart(shuffleHandlerConf);
    LOG.info("Setting shuffle port to: " + ShuffleHandler.get().getPort());
    this.shufflePort.set(ShuffleHandler.get().getPort());
    getConfig()
        .setInt(ConfVars.LLAP_DAEMON_YARN_SHUFFLE_PORT.varname, ShuffleHandler.get().getPort());
    LlapOutputFormatService.initializeAndStart(getConfig());
    super.serviceStart();

    // Setup the actual ports in the configuration.
    getConfig().setInt(ConfVars.LLAP_DAEMON_RPC_PORT.varname, server.getBindAddress().getPort());
    getConfig().setInt(ConfVars.LLAP_MANAGEMENT_RPC_PORT.varname, server.getManagementBindAddress().getPort());
    if (webServices != null) {
      getConfig().setInt(ConfVars.LLAP_DAEMON_WEB_PORT.varname, webServices.getPort());
    }
    getConfig().setInt(ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT.varname, LlapOutputFormatService.get().getPort());

    this.registry.init(getConfig());
    this.registry.start();
    LOG.info(
        "LlapDaemon serviceStart complete. RPC Port={}, ManagementPort={}, ShuflePort={}, WebPort={}",
        server.getBindAddress().getPort(), server.getManagementBindAddress().getPort(),
        ShuffleHandler.get().getPort(), (webServices == null ? "" : webServices.getPort()));
  }

  public void serviceStop() throws Exception {
    if (registry != null) {
      this.registry.stop();
    }
    super.serviceStop();
    ShuffleHandler.shutdown();
    shutdown();
    LlapOutputFormatService.get().stop();
    LOG.info("LlapDaemon shutdown complete");
  }

  public void shutdown() {
    LOG.info("LlapDaemon shutdown invoked");
    if (llapDaemonInfoBean != null) {
      try {
        MBeans.unregister(llapDaemonInfoBean);
      } catch (Throwable ex) {
        LOG.info("Error unregistering the bean; ignoring", ex);
      }
    }

    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }

    if (metrics != null) {
      LlapMetricsSystem.shutdown();
    }

    LlapProxy.close();

    if (fnLocalizer != null) {
      fnLocalizer.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new LlapDaemonUncaughtExceptionHandler());
    LlapDaemon llapDaemon = null;
    try {
      // Cache settings will need to be setup in llap-daemon-site.xml - since the daemons don't read hive-site.xml
      // Ideally, these properties should be part of LlapDameonConf rather than HiveConf
      LlapDaemonConfiguration daemonConf = new LlapDaemonConfiguration();

      String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
      String appName = null;
      if (containerIdStr != null && !containerIdStr.isEmpty()) {
        daemonConf.set(ConfVars.LLAP_DAEMON_CONTAINER_ID.varname, containerIdStr);
        appName = ConverterUtils.toContainerId(containerIdStr)
            .getApplicationAttemptId().getApplicationId().toString();
      } else {
        daemonConf.unset(ConfVars.LLAP_DAEMON_CONTAINER_ID.varname);
        // Note, we assume production LLAP always runs under YARN.
        LOG.error("Cannot find " + ApplicationConstants.Environment.CONTAINER_ID.toString()
            + "; LLAP tokens may grant access to subsequent instances of the cluster with"
            + " the same name");
        appName = null;
      }

      int numExecutors = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS);

      String localDirList = LlapUtil.getDaemonLocalDirList(daemonConf);
      String[] localDirs = (localDirList == null || localDirList.isEmpty()) ?
          new String[0] : StringUtils.getTrimmedStrings(localDirList);
      int rpcPort = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_RPC_PORT);
      int mngPort = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_MANAGEMENT_RPC_PORT);
      int shufflePort = daemonConf
          .getInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, ShuffleHandler.DEFAULT_SHUFFLE_PORT);
      int webPort = HiveConf.getIntVar(daemonConf, ConfVars.LLAP_DAEMON_WEB_PORT);
      long executorMemoryBytes = HiveConf.getIntVar(
          daemonConf, ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB) * 1024l * 1024l;

      long ioMemoryBytes = HiveConf.getSizeVar(daemonConf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
      boolean isDirectCache = HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_ALLOCATOR_DIRECT);
      boolean isLlapIo = HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_IO_ENABLED, true);
      llapDaemon = new LlapDaemon(daemonConf, numExecutors, executorMemoryBytes, isLlapIo,
          isDirectCache, ioMemoryBytes, localDirs, rpcPort, mngPort, shufflePort, webPort,
          appName);

      LOG.info("Adding shutdown hook for LlapDaemon");
      ShutdownHookManager.addShutdownHook(new CompositeServiceShutdownHook(llapDaemon), 1);

      llapDaemon.init(daemonConf);
      llapDaemon.start();
      LOG.info("Started LlapDaemon");
      // Relying on the RPC threads to keep the service alive.
    } catch (Throwable t) {
      // TODO Replace this with a ExceptionHandler / ShutdownHook
      LOG.warn("Failed to start LLAP Daemon with exception", t);
      if (llapDaemon != null) {
        llapDaemon.shutdown();
      }
      System.exit(-1);
    }
  }

  @Override
  public SubmitWorkResponseProto submitWork(
      SubmitWorkRequestProto request) throws IOException {
    numSubmissions.incrementAndGet();
    return containerRunner.submitWork(request);
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(
      SourceStateUpdatedRequestProto request) throws IOException {
    return containerRunner.sourceStateUpdated(request);
  }

  @Override
  public QueryCompleteResponseProto queryComplete(
      QueryCompleteRequestProto request) throws IOException {
    return containerRunner.queryComplete(request);
  }

  @Override
  public TerminateFragmentResponseProto terminateFragment(
      TerminateFragmentRequestProto request) throws IOException {
    return containerRunner.terminateFragment(request);
  }

  @VisibleForTesting
  public long getNumSubmissions() {
    return numSubmissions.get();
  }

  public InetSocketAddress getListenerAddress() {
    return server.getBindAddress();
  }

  // LlapDaemonMXBean methods. Will be exposed via JMX
  @Override
  public int getRpcPort() {
    return server.getBindAddress().getPort();
  }

  @Override
  public int getNumExecutors() {
    return numExecutors;
  }

  @Override
  public int getShufflePort() {
    return ShuffleHandler.get().getPort();
  }

  @Override
  public String getLocalDirs() {
    return Joiner.on(",").skipNulls().join(localDirs);
  }

  @Override
  public Set<String> getExecutorsStatus() {
    return containerRunner.getExecutorStatus();
  }

  @Override
  public long getExecutorMemoryPerInstance() {
    return executorMemoryPerInstance;
  }

  @Override
  public long getIoMemoryPerInstance() {
    return ioMemoryPerInstance;
  }

  @Override
  public boolean isIoEnabled() {
    return llapIoEnabled;
  }

  @Override
  public long getMaxJvmMemory() {
    return maxJvmMemory;
  }

  /**
   * A global hook that checks all subclasses of GenericUDF against the whitelist. It also injects
   * us into GenericUDFBridge-s, to check with the whitelist before instantiating a UDF.
   */
  private static final class LlapGlobalUdfChecker extends SerializationUtilities.Hook {
    private UdfWhitelistChecker fnCheckerImpl;
    public LlapGlobalUdfChecker(UdfWhitelistChecker fnCheckerImpl) {
      this.fnCheckerImpl = fnCheckerImpl;
    }

    @Override
    public boolean preRead(Class<?> type) {
      // 1) Don't call postRead - we will have checked everything here.
      // 2) Ignore GenericUDFBridge, it's checked separately in LlapUdfBridgeChecker.
      if (GenericUDFBridge.class == type) return true; // Run post-hook.
      if (!(GenericUDF.class.isAssignableFrom(type) || UDF.class.isAssignableFrom(type))
          || fnCheckerImpl.isUdfAllowed(type)) return false;
      throw new SecurityException("UDF " + type.getCanonicalName() + " is not allowed");
    }

    @Override
    public Object postRead(Object o) {
      if (o == null) return o;
      Class<?> type = o.getClass();
      if (GenericUDFBridge.class == type)  {
        ((GenericUDFBridge)o).setUdfChecker(fnCheckerImpl);
      }
      // This won't usually be called otherwise.
      preRead(type);
      return o;
    }
  }

  private static class LlapDaemonUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.info("UncaughtExceptionHandler invoked");
      if(ShutdownHookManager.isShutdownInProgress()) {
        LOG.warn("Thread {} threw a Throwable, but we are shutting down, so ignoring this", t, e);
      } else if(e instanceof Error) {
        try {
          LOG.error("Thread {} threw an Error.  Shutting down now...", t, e);
        } catch (Throwable err) {
          //We don't want to not exit because of an issue with logging
        }
        if(e instanceof OutOfMemoryError) {
          //After catching an OOM java says it is undefined behavior, so don't
          //even try to clean up or we can get stuck on shutdown.
          try {
            System.err.println("Halting due to Out Of Memory Error...");
            e.printStackTrace();
          } catch (Throwable err) {
            //Again we done want to exit because of logging issues.
          }
          ExitUtil.halt(-1);
        } else {
          ExitUtil.terminate(-1);
        }
      } else {
        LOG.error("Thread {} threw an Exception. Shutting down now...", t, e);
        ExitUtil.terminate(-1);
      }
    }
  }

  private class QueryFailedHandlerProxy implements QueryFailedHandler {

    @Override
    public void queryFailed(QueryIdentifier queryIdentifier) {
      containerRunner.queryFailed(queryIdentifier);
    }
  }
}
