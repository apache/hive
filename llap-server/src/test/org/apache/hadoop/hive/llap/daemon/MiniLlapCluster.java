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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

import com.google.common.base.Preconditions;

public class MiniLlapCluster extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(MiniLlapCluster.class);

  private final File testWorkDir;
  private final long execBytesPerService;
  private final boolean llapIoEnabled;
  private final boolean ioIsDirect;
  private final long ioBytesPerService;
  private final int numExecutorsPerService;
  private final String[] localDirs;
  private final Configuration clusterSpecificConfiguration = new Configuration(false);

  private LlapDaemon llapDaemon;

  public static MiniLlapCluster create(String clusterName, int numExecutorsPerService,
      long execBytePerService, boolean llapIoEnabled, boolean ioIsDirect, long ioBytesPerService,
      int numLocalDirs) {
    return new MiniLlapCluster(clusterName, numExecutorsPerService, execBytePerService,
        llapIoEnabled, ioIsDirect, ioBytesPerService, numLocalDirs);
  }

  public static MiniLlapCluster createAndLaunch(Configuration conf, String clusterName,
      int numExecutorsPerService, long execBytePerService, boolean llapIoEnabled,
      boolean ioIsDirect, long ioBytesPerService, int numLocalDirs) {
    MiniLlapCluster miniLlapCluster = create(clusterName, numExecutorsPerService,
        execBytePerService, llapIoEnabled, ioIsDirect, ioBytesPerService, numLocalDirs);
    miniLlapCluster.init(conf);
    miniLlapCluster.start();
    Configuration llapConf = miniLlapCluster.getClusterSpecificConfiguration();
    Iterator<Map.Entry<String, String>> confIter = llapConf.iterator();
    while (confIter.hasNext()) {
      Map.Entry<String, String> entry = confIter.next();
      conf.set(entry.getKey(), entry.getValue());
    }
    return miniLlapCluster;
  }

  // TODO Add support for multiple instances
  private MiniLlapCluster(String clusterName, int numExecutorsPerService, long execMemoryPerService,
                          boolean llapIoEnabled, boolean ioIsDirect, long ioBytesPerService, int numLocalDirs) {
    super(clusterName + "_" + MiniLlapCluster.class.getSimpleName());
    Preconditions.checkArgument(numExecutorsPerService > 0);
    Preconditions.checkArgument(execMemoryPerService > 0);
    Preconditions.checkArgument(numLocalDirs > 0);
    String clusterNameTrimmed = clusterName.replace("$", "") + "_" + MiniLlapCluster.class.getSimpleName();
    File targetWorkDir = new File("target", clusterNameTrimmed);
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(targetWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      LOG.warn("Could not cleanup test workDir: " + targetWorkDir, e);
      throw new RuntimeException("Could not cleanup test workDir: " + targetWorkDir, e);
    }

    if (Shell.WINDOWS) {
      // The test working directory can exceed the maximum path length supported
      // by some Windows APIs and cmd.exe (260 characters).  To work around this,
      // create a symlink in temporary storage with a much shorter path,
      // targeting the full path to the test working directory.  Then, use the
      // symlink as the test working directory.
      String targetPath = targetWorkDir.getAbsolutePath();
      File link = new File(System.getProperty("java.io.tmpdir"),
          String.valueOf(System.currentTimeMillis()));
      String linkPath = link.getAbsolutePath();

      try {
        FileContext.getLocalFSFileContext().delete(new Path(linkPath), true);
      } catch (IOException e) {
        throw new YarnRuntimeException("could not cleanup symlink: " + linkPath, e);
      }

      // Guarantee target exists before creating symlink.
      targetWorkDir.mkdirs();

      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
          Shell.getSymlinkCommand(targetPath, linkPath));
      try {
        shexec.execute();
      } catch (IOException e) {
        throw new YarnRuntimeException(String.format(
            "failed to create symlink from %s to %s, shell output: %s", linkPath,
            targetPath, shexec.getOutput()), e);
      }

      this.testWorkDir = link;
    } else {
      this.testWorkDir = targetWorkDir;
    }
    this.numExecutorsPerService = numExecutorsPerService;
    this.execBytesPerService = execMemoryPerService;
    this.ioIsDirect = ioIsDirect;
    this.llapIoEnabled = llapIoEnabled;
    this.ioBytesPerService = ioBytesPerService;

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
  public void serviceInit(Configuration conf) {
    llapDaemon = new LlapDaemon(conf, numExecutorsPerService, execBytesPerService, llapIoEnabled,
        ioIsDirect, ioBytesPerService, localDirs, 0, 0, 0);
    llapDaemon.init(conf);
  }

  @Override
  public void serviceStart() {
    llapDaemon.start();

    clusterSpecificConfiguration.set(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname,
        getServiceAddress().getHostName());
    clusterSpecificConfiguration.setInt(ConfVars.LLAP_DAEMON_RPC_PORT.varname,
        getServiceAddress().getPort());

    clusterSpecificConfiguration.setInt(
        ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname,
        numExecutorsPerService);
    clusterSpecificConfiguration.setLong(
        ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, execBytesPerService);
    // Optimize local fetch does not work with LLAP due to different local directories
    // used by containers and LLAP
    clusterSpecificConfiguration
        .setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
  }

  @Override
  public void serviceStop() {
    if (llapDaemon != null) {
      llapDaemon.stop();
      llapDaemon = null;
    }
  }

  private InetSocketAddress getServiceAddress() {
    Preconditions.checkState(getServiceState() == Service.STATE.STARTED);
    return llapDaemon.getListenerAddress();
  }

  public Configuration getClusterSpecificConfiguration() {
    Preconditions.checkState(getServiceState() == Service.STATE.STARTED);
    return clusterSpecificConfiguration;
  }

  // Mainly for verification
  public long getNumSubmissions() {
    return llapDaemon.getNumSubmissions();
  }

}
