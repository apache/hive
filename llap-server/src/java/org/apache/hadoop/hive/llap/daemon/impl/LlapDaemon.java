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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.Logger;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.RunContainerRequestProto;
import org.apache.hadoop.hive.llap.io.api.LlapIoProxy;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;

public class LlapDaemon extends AbstractService implements ContainerRunner {

  private static final Logger LOG = Logger.getLogger(LlapDaemon.class);

  private final LlapDaemonConfiguration daemonConf;
  private final int numExecutors;
  private final int rpcPort;
  private final LlapDaemonProtocolServerImpl server;
  private final ContainerRunnerImpl containerRunner;
  private final String[] localDirs;
  private final int shufflePort;
  // TODO Not the best way to share the address
  private final AtomicReference<InetSocketAddress> address = new AtomicReference<InetSocketAddress>();

  public LlapDaemon(LlapDaemonConfiguration daemonConf) {
    super("LlapDaemon");
    // TODO This needs to read TezConfiguration to pick up things like the heartbeat interval from config.
    // Ideally, this would be part of llap-daemon-configuration
    this.numExecutors = daemonConf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS,
        LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS_DEFAULT);
    this.rpcPort = daemonConf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT,
        LlapDaemonConfiguration.LLAP_DAEMON_RPC_PORT_DEFAULT);
    this.daemonConf = daemonConf;
    this.localDirs = daemonConf.getTrimmedStrings(LlapDaemonConfiguration.LLAP_DAEMON_WORK_DIRS);
    this.shufflePort = daemonConf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_YARN_SHUFFLE_PORT, -1);

    long memoryAvailableBytes = this.daemonConf
        .getInt(LlapDaemonConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB,
            LlapDaemonConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT) * 1024l * 1024l;
    long jvmMax = Runtime.getRuntime().maxMemory();

    LOG.info("LlapDaemon started with the following configuration: " +
        "numExecutors=" + numExecutors +
        ", rpcListenerPort=" + rpcPort +
        ", workDirs=" + Arrays.toString(localDirs) +
        ", shufflePort=" + shufflePort +
        ", memoryConfigured=" + memoryAvailableBytes +
        ", jvmAvailableMemory=" + jvmMax);

    Preconditions.checkArgument(this.numExecutors > 0);
    Preconditions.checkArgument(this.rpcPort > 1024 && this.rpcPort < 65536,
        "RPC Port must be between 1025 and 65534");
    Preconditions.checkArgument(this.localDirs != null && this.localDirs.length > 0,
        "Work dirs must be specified");
    Preconditions.checkArgument(this.shufflePort > 0, "ShufflePort must be specified");
    Preconditions.checkState(jvmMax >= memoryAvailableBytes,
        "Invalid configuration. Xmx value too small. maxAvailable=" + jvmMax + ", configured=" +
            memoryAvailableBytes);

    this.server = new LlapDaemonProtocolServerImpl(daemonConf, this, address);
    this.containerRunner = new ContainerRunnerImpl(numExecutors, localDirs, shufflePort, address,
        memoryAvailableBytes);
  }

  @Override
  public void serviceInit(Configuration conf) {
    server.init(conf);
    containerRunner.init(conf);
    LlapIoProxy.initializeLlapIo(conf);
  }

  @Override
  public void serviceStart() {
    server.start();
    containerRunner.start();

  }

  public void serviceStop() {
    containerRunner.stop();
    server.stop();
  }


  public static void main(String[] args) throws Exception {
    try {
      LlapDaemonConfiguration daemonConf = new LlapDaemonConfiguration();

      Configuration shuffleHandlerConf = new Configuration(daemonConf);
      shuffleHandlerConf.set(ShuffleHandler.SHUFFLE_HANDLER_LOCAL_DIRS,
          daemonConf.get(LlapDaemonConfiguration.LLAP_DAEMON_WORK_DIRS));
      ShuffleHandler.initializeAndStart(shuffleHandlerConf);

      LlapDaemon llapDaemon = new LlapDaemon(daemonConf);
      llapDaemon.init(new Configuration());
      llapDaemon.start();
      LOG.info("Started LlapDaemon");
      // Relying on the RPC threads to keep the service alive.
    } catch (Throwable t) {
      // TODO Replace this with a ExceptionHandler / ShutdownHook
      LOG.warn("Failed to start LLAP Daemon with exception", t);
      System.exit(-1);
    }
  }


  @Override
  public void queueContainer(RunContainerRequestProto request) throws IOException {
    containerRunner.queueContainer(request);
  }
}
