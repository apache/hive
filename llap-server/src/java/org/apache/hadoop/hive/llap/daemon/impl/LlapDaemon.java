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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.api.LlapIoProxy;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class LlapDaemon extends AbstractService implements ContainerRunner, LlapDaemonMXBean {

  private static final Logger LOG = Logger.getLogger(LlapDaemon.class);

  private final Configuration shuffleHandlerConf;
  private final LlapDaemonProtocolServerImpl server;
  private final ContainerRunnerImpl containerRunner;
  private final LlapRegistryService registry;
  private final AtomicLong numSubmissions = new AtomicLong(0);
  private JvmPauseMonitor pauseMonitor;
  private final ObjectName llapDaemonInfoBean;
  private final LlapDaemonExecutorMetrics metrics;

  // Parameters used for JMX
  private final boolean llapIoEnabled;
  private final long executorMemoryPerInstance;
  private final long ioMemoryPerInstance;
  private final int numExecutors;
  private final long maxJvmMemory;
  private final String[] localDirs;

  // TODO Not the best way to share the address
  private final AtomicReference<InetSocketAddress> address = new AtomicReference<InetSocketAddress>();

  public LlapDaemon(Configuration daemonConf, int numExecutors, long executorMemoryBytes,
                    boolean ioEnabled, long ioMemoryBytes, String[] localDirs, int rpcPort,
                    int shufflePort) {
    super("LlapDaemon");

    printAsciiArt();

    Preconditions.checkArgument(numExecutors > 0);
    Preconditions.checkArgument(rpcPort == 0 || (rpcPort > 1024 && rpcPort < 65536),
        "RPC Port must be between 1025 and 65535, or 0 automatic selection");
    Preconditions.checkArgument(localDirs != null && localDirs.length > 0,
        "Work dirs must be specified");
    Preconditions.checkArgument(shufflePort == 0 || (shufflePort > 1024 && shufflePort < 65536),
        "Shuffle Port must be betwee 1024 and 65535, or 0 for automatic selection");

    this.maxJvmMemory = Runtime.getRuntime().maxMemory();
    this.llapIoEnabled = ioEnabled;
    this.executorMemoryPerInstance = executorMemoryBytes;
    this.ioMemoryPerInstance = ioMemoryBytes;
    this.numExecutors = numExecutors;
    this.localDirs = localDirs;

    LOG.info("Attempting to start LlapDaemonConf with the following configuration: " +
        "numExecutors=" + numExecutors +
        ", rpcListenerPort=" + rpcPort +
        ", workDirs=" + Arrays.toString(localDirs) +
        ", shufflePort=" + shufflePort +
        ", executorMemory=" + executorMemoryBytes +
        ", llapIoEnabled=" + ioEnabled +
        ", llapIoCacheSize=" + ioMemoryBytes +
        ", jvmAvailableMemory=" + maxJvmMemory);

    long memRequired = executorMemoryBytes + (ioEnabled ? ioMemoryBytes : 0);
    Preconditions.checkState(maxJvmMemory >= memRequired,
        "Invalid configuration. Xmx value too small. maxAvailable=" + maxJvmMemory +
            ", configured(exec + io if enabled)=" +
            memRequired);

    this.shuffleHandlerConf = new Configuration(daemonConf);
    this.shuffleHandlerConf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, shufflePort);
    this.shuffleHandlerConf.set(ShuffleHandler.SHUFFLE_HANDLER_LOCAL_DIRS,
        StringUtils.arrayToString(localDirs));
    this.shuffleHandlerConf.setBoolean(ShuffleHandler.SHUFFLE_DIR_WATCHER_ENABLED, daemonConf
        .getBoolean(LlapConfiguration.LLAP_DAEMON_SHUFFLE_DIR_WATCHER_ENABLED,
            LlapConfiguration.LLAP_DAEMON_SHUFFLE_DIR_WATCHER_ENABLED_DEFAULT));

    // Less frequently set parameter, not passing in as a param.
    int numHandlers = daemonConf.getInt(LlapConfiguration.LLAP_DAEMON_RPC_NUM_HANDLERS,
        LlapConfiguration.LLAP_DAEMON_RPC_NUM_HANDLERS_DEFAULT);
    this.server = new LlapDaemonProtocolServerImpl(numHandlers, this, address, rpcPort);

    // Initialize the metric system
    LlapMetricsSystem.initialize("LlapDaemon");
    this.pauseMonitor = new JvmPauseMonitor(daemonConf);
    pauseMonitor.start();
    String displayName = "LlapDaemonExecutorMetrics-" + MetricsUtils.getHostName();
    String sessionId = MetricsUtils.getUUID();
    daemonConf.set("llap.daemon.metrics.sessionid", sessionId);
    this.metrics = LlapDaemonExecutorMetrics.create(displayName, sessionId, numExecutors);
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    this.llapDaemonInfoBean = MBeans.register("LlapDaemon", "LlapDaemonInfo", this);
    LOG.info("Started LlapMetricsSystem with displayName: " + displayName +
        " sessionId: " + sessionId);

    this.containerRunner = new ContainerRunnerImpl(daemonConf, numExecutors, localDirs, shufflePort, address,
        executorMemoryBytes, metrics);
    
    this.registry = new LlapRegistryService();
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
    LOG.info("\n\n" + asciiArt);
  }

  @Override
  public void serviceInit(Configuration conf) {
    server.init(conf);
    containerRunner.init(conf);
    registry.init(conf);
    LlapIoProxy.setDaemon(true);
    LlapIoProxy.initializeLlapIo(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    ShuffleHandler.initializeAndStart(shuffleHandlerConf);
    server.start();
    containerRunner.start();
    registry.start();
    registry.registerWorker();
  }

  public void serviceStop() throws Exception {
    // TODO Shutdown LlapIO
    shutdown();
    containerRunner.stop();
    server.stop();
    registry.unregisterWorker();
    registry.stop();
    ShuffleHandler.shutdown();
  }

  public void shutdown() {
    LOG.info("LlapDaemon shutdown invoked");
    if (llapDaemonInfoBean != null) {
      MBeans.unregister(llapDaemonInfoBean);
    }

    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }

    if (metrics != null) {
      LlapMetricsSystem.shutdown();
    }

    LlapIoProxy.close();
  }

  public static void main(String[] args) throws Exception {
    LlapDaemon llapDaemon = null;
    try {
      // Cache settings will need to be setup in llap-daemon-site.xml - since the daemons don't read hive-site.xml
      // Ideally, these properties should be part of LlapDameonConf rather than HiveConf
      LlapConfiguration daemonConf = new LlapConfiguration();
       int numExecutors = daemonConf.getInt(LlapConfiguration.LLAP_DAEMON_NUM_EXECUTORS,
           LlapConfiguration.LLAP_DAEMON_NUM_EXECUTORS_DEFAULT);
       String[] localDirs =
           daemonConf.getTrimmedStrings(LlapConfiguration.LLAP_DAEMON_WORK_DIRS);
       int rpcPort = daemonConf.getInt(LlapConfiguration.LLAP_DAEMON_RPC_PORT,
           LlapConfiguration.LLAP_DAEMON_RPC_PORT_DEFAULT);
       int shufflePort = daemonConf
           .getInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, ShuffleHandler.DEFAULT_SHUFFLE_PORT);
       long executorMemoryBytes = daemonConf
           .getInt(LlapConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB,
               LlapConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT) * 1024l * 1024l;
       long cacheMemoryBytes =
           HiveConf.getLongVar(daemonConf, HiveConf.ConfVars.LLAP_ORC_CACHE_MAX_SIZE);
       boolean llapIoEnabled = HiveConf.getBoolVar(daemonConf, HiveConf.ConfVars.LLAP_IO_ENABLED);
       llapDaemon =
           new LlapDaemon(daemonConf, numExecutors, executorMemoryBytes, llapIoEnabled,
               cacheMemoryBytes, localDirs,
               rpcPort, shufflePort);

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
  public void submitWork(LlapDaemonProtocolProtos.SubmitWorkRequestProto request) throws
      IOException {
    numSubmissions.incrementAndGet();
    containerRunner.submitWork(request);
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


}
