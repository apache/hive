/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapDaemonInfo;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

import com.google.common.base.Preconditions;

public class MiniLlapCluster extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(MiniLlapCluster.class);

  private final File testWorkDir;
  private final String clusterNameTrimmed;
  private final long numInstances;
  private final long execBytesPerService;
  private final boolean llapIoEnabled;
  private final boolean ioIsDirect;
  private final long ioBytesPerService;
  private final int numExecutorsPerService;
  private final File zkWorkDir;
  private final String[] localDirs;
  private final Configuration clusterSpecificConfiguration = new Configuration(false);

  private final LlapDaemon [] llapDaemons;
  private MiniZooKeeperCluster miniZooKeeperCluster;
  private final boolean ownZkCluster;


  public static MiniLlapCluster create(String clusterName,
                                       @Nullable MiniZooKeeperCluster miniZkCluster,
                                       int numInstances,
                                       int numExecutorsPerService,
                                       long execBytePerService, boolean llapIoEnabled,
                                       boolean ioIsDirect, long ioBytesPerService,
                                       int numLocalDirs) {
    return new MiniLlapCluster(clusterName, miniZkCluster, numInstances, numExecutorsPerService,
        execBytePerService,
        llapIoEnabled, ioIsDirect, ioBytesPerService, numLocalDirs);
  }

  public static MiniLlapCluster create(String clusterName,
                                       @Nullable MiniZooKeeperCluster miniZkCluster,
                                       int numExecutorsPerService,
                                       long execBytePerService, boolean llapIoEnabled,
                                       boolean ioIsDirect, long ioBytesPerService,
                                       int numLocalDirs) {
    return create(clusterName, miniZkCluster, 1, numExecutorsPerService, execBytePerService,
        llapIoEnabled,
        ioIsDirect, ioBytesPerService, numLocalDirs);
  }

  private MiniLlapCluster(String clusterName, @Nullable MiniZooKeeperCluster miniZkCluster,
                          int numInstances, int numExecutorsPerService, long execMemoryPerService,
                          boolean llapIoEnabled, boolean ioIsDirect, long ioBytesPerService,
                          int numLocalDirs) {
    super(clusterName + "_" + MiniLlapCluster.class.getSimpleName());
    Preconditions.checkArgument(numExecutorsPerService > 0);
    Preconditions.checkArgument(execMemoryPerService > 0);
    Preconditions.checkArgument(numLocalDirs > 0);
    this.numInstances = numInstances;

    this.clusterNameTrimmed = clusterName.replace("$", "") + "_" + MiniLlapCluster.class.getSimpleName();
    this.llapDaemons = new LlapDaemon[numInstances];
    File targetWorkDir = new File("target", clusterNameTrimmed);
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(targetWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      LOG.warn("Could not cleanup test workDir: " + targetWorkDir, e);
      throw new RuntimeException("Could not cleanup test workDir: " + targetWorkDir, e);
    }

    targetWorkDir.mkdir();
    this.testWorkDir = targetWorkDir;

    if (miniZkCluster == null) {
      ownZkCluster = true;
      this.zkWorkDir = new File(testWorkDir, "mini-zk-cluster");
      zkWorkDir.mkdir();
    } else {
      miniZooKeeperCluster = miniZkCluster;
      ownZkCluster = false;
      this.zkWorkDir = null;
    }
    this.numExecutorsPerService = numExecutorsPerService;
    this.execBytesPerService = execMemoryPerService;
    this.ioIsDirect = ioIsDirect;
    this.llapIoEnabled = llapIoEnabled;
    this.ioBytesPerService = ioBytesPerService;

    LlapDaemonInfo.initialize("mini-llap-cluster", numExecutorsPerService, execMemoryPerService,
        ioBytesPerService, ioIsDirect, llapIoEnabled, "-1");

    // Setup Local Dirs
    localDirs = new String[numLocalDirs];
    for (int i = 0 ; i < numLocalDirs ; i++) {
      File f = new File(testWorkDir, "localDir");
      f.mkdirs();
      LOG.info("Created localDir: " + f.getAbsolutePath());
      localDirs[i] = f.getAbsolutePath();
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws IOException, InterruptedException {
    int rpcPort = 0;
    int mngPort = 0;
    int shufflePort = 0;
    int webPort = 0;
    int outputFormatServicePort = 0;
    boolean usePortsFromConf = conf.getBoolean("minillap.usePortsFromConf", false);
    LOG.info("MiniLlap configured to use ports from conf: {}", usePortsFromConf);
    if (usePortsFromConf) {
      rpcPort = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_RPC_PORT);
      mngPort = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_MANAGEMENT_RPC_PORT);
      shufflePort = conf.getInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, ShuffleHandler.DEFAULT_SHUFFLE_PORT);
      webPort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_WEB_PORT);
      outputFormatServicePort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT);
    }
    HiveConf.setIntVar(conf, ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT, outputFormatServicePort);

    if (ownZkCluster) {
      miniZooKeeperCluster = new MiniZooKeeperCluster();
      miniZooKeeperCluster.startup(zkWorkDir);
    } else {
      // Already setup in the create method
    } 

    conf.set(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + clusterNameTrimmed);
    conf.set(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    conf.setInt(ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname, miniZooKeeperCluster.getClientPort());
    // Also add ZK settings to clusterSpecificConf to make sure these get picked up by whoever started this.
    clusterSpecificConfiguration.set(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, "@" + clusterNameTrimmed);
    clusterSpecificConfiguration.set(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    clusterSpecificConfiguration.setInt(ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname, miniZooKeeperCluster.getClientPort());
  
    LOG.info("Initializing {} llap instances for MiniLlapCluster with name={}", numInstances, clusterNameTrimmed);
    for (int i = 0 ;i < numInstances ; i++) {
      llapDaemons[i] = new LlapDaemon(conf, numExecutorsPerService, execBytesPerService, llapIoEnabled,
          ioIsDirect, ioBytesPerService, localDirs, rpcPort, mngPort, shufflePort, webPort, clusterNameTrimmed);
      llapDaemons[i].init(new Configuration(conf));
    }
    LOG.info("Initialized {} llap instances for MiniLlapCluster with name={}", numInstances, clusterNameTrimmed);
  }

  @Override
  public void serviceStart() {
    LOG.info("Starting {} llap instances for MiniLlapCluster with name={}", numInstances, clusterNameTrimmed);
    for (int i = 0 ;i < numInstances ; i++) {
      llapDaemons[i].start();
    }
    LOG.info("Started {} llap instances for MiniLlapCluster with name={}", numInstances, clusterNameTrimmed);

    // Optimize local fetch does not work with LLAP due to different local directories
    // used by containers and LLAP
    clusterSpecificConfiguration
        .setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
  }

  @Override
  public void serviceStop() throws IOException {
    for (int i = 0 ; i < numInstances ; i++) {
      if (llapDaemons[i] != null) {
        llapDaemons[i].stop();
        llapDaemons[i] = null;
      }
    }
    if (ownZkCluster) {
      if (miniZooKeeperCluster != null) {
        LOG.info("Stopping MiniZooKeeper cluster");
        miniZooKeeperCluster.shutdown();
        miniZooKeeperCluster = null;
        LOG.info("Stopped MiniZooKeeper cluster");
      }
    } else {
      LOG.info("Not stopping MiniZK cluster since it is now owned by us"); 
    }
  }


  public Configuration getClusterSpecificConfiguration() {
    Preconditions.checkState(getServiceState() == Service.STATE.STARTED);
    return clusterSpecificConfiguration;
  }

  // Mainly for verification
  public long getNumSubmissions() {
    int numSubmissions = 0;
    for (int i = 0 ; i < numInstances ; i++) {
      numSubmissions += llapDaemons[i].getNumSubmissions();
    }
    return numSubmissions;
  }
}
