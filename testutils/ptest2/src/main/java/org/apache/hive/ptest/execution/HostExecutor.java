/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.ssh.RSyncCommand;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.apache.hive.ptest.execution.ssh.RSyncResult;
import org.apache.hive.ptest.execution.ssh.RemoteCommandResult;
import org.apache.hive.ptest.execution.ssh.SSHCommand;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.apache.hive.ptest.execution.ssh.SSHExecutionException;
import org.apache.hive.ptest.execution.ssh.SSHResult;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

class HostExecutor {
  private static final int MAX_SOURCE_DIRS = 5;
  private final Host mHost;
  private final List<Drone> mDrones;
  private final ListeningExecutorService mExecutor;
  private final SSHCommandExecutor mSSHCommandExecutor;
  private final RSyncCommandExecutor mRSyncCommandExecutor;
  private final ImmutableMap<String, String> mTemplateDefaults;
  private final Logger mLogger;
  private final File mLocalScratchDirectory;
  private final File mSuccessfulTestLogDir;
  private final File mFailedTestLogDir;
  private final long mNumPollSeconds;
  private volatile boolean mShutdown;
  
  HostExecutor(Host host, String privateKey, ListeningExecutorService executor,
      SSHCommandExecutor sshCommandExecutor,
      RSyncCommandExecutor rsyncCommandExecutor,
      ImmutableMap<String, String> templateDefaults, File scratchDir,
      File succeededLogDir, File failedLogDir, long numPollSeconds,
      Logger logger) {
    List<Drone> drones = Lists.newArrayList();
    String[] localDirs = host.getLocalDirectories();
    for (int index = 0; index < host.getThreads(); index++) {
      drones.add(new Drone(privateKey, host.getUser(), host.getName(),
          index, localDirs[index % localDirs.length]));
    }
    mShutdown = false;
    mHost = host;
    mDrones = new CopyOnWriteArrayList<Drone>(drones);
    mExecutor = executor;
    mSSHCommandExecutor = sshCommandExecutor;
    mRSyncCommandExecutor = rsyncCommandExecutor;
    mTemplateDefaults = templateDefaults;
    mLocalScratchDirectory = scratchDir;
    mSuccessfulTestLogDir = succeededLogDir;
    mFailedTestLogDir = failedLogDir;
    mNumPollSeconds = numPollSeconds;
    mLogger = logger;
  }

  /**
   * @return failed tests
   */
  ListenableFuture<Void> submitTests(final BlockingQueue<TestBatch> parallelWorkQueue,
      final BlockingQueue<TestBatch> isolatedWorkQueue, final Set<TestBatch> failedTestResults) {
    return mExecutor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        executeTests(parallelWorkQueue, isolatedWorkQueue, failedTestResults);
        return null;
      }

    });
  }
  @VisibleForTesting
  int remainingDrones() {
    return mDrones.size();
  }
  boolean isBad() {
    return mDrones.isEmpty();
  }
  Host getHost() {
    return mHost;
  }
  void shutdownNow() {
    this.mShutdown = true;
  }
  boolean isShutdown() {
    return mShutdown;
  }
  /**
   * Executes parallel test until the parallel work queue is empty. Then
   * executes the isolated tests on the host. During each phase if a
   * AbortDroneException is thrown the drone is removed possibly
   * leaving this host with zero functioning drones. If all drones
   * are removed the host will be replaced before the next run.
   */
  private void executeTests(final BlockingQueue<TestBatch> parallelWorkQueue,
      final BlockingQueue<TestBatch> isolatedWorkQueue, final Set<TestBatch> failedTestResults)
          throws Exception {
    if(mShutdown) {
      mLogger.warn("Shutting down host " + mHost.getName());
      return;
    }
    mLogger.info("Starting parallel execution on " + mHost.getName());
    List<ListenableFuture<Void>> droneResults = Lists.newArrayList();
    for(final Drone drone : ImmutableList.copyOf(mDrones)) {
      droneResults.add(mExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          TestBatch batch = null;
          try {
            do {
              batch = parallelWorkQueue.poll(mNumPollSeconds, TimeUnit.SECONDS);
              if(mShutdown) {
                mLogger.warn("Shutting down host " + mHost.getName());
                return null;
              }
              if(batch != null) {
                if(!executeTestBatch(drone, batch, failedTestResults)) {
                  failedTestResults.add(batch);
                }
              }
            } while(!mShutdown && !parallelWorkQueue.isEmpty());
          } catch(AbortDroneException ex) {
            mDrones.remove(drone); // return value not checked due to concurrent access
            mLogger.error("Aborting drone during parallel execution", ex);
            if(batch != null) {
              Preconditions.checkState(parallelWorkQueue.add(batch),
                  "Could not add batch to parallel queue " + batch);
            }
          }
          return null;
        }
      }));
    }
    if(mShutdown) {
      mLogger.warn("Shutting down host " + mHost.getName());
      return;
    }
    Futures.allAsList(droneResults).get();
    mLogger.info("Starting isolated execution on " + mHost.getName());
    for(Drone drone : ImmutableList.copyOf(mDrones)) {
      TestBatch batch = null;
      try {
        do {
          batch = isolatedWorkQueue.poll(mNumPollSeconds, TimeUnit.SECONDS);
          if(batch != null) {
            if(!executeTestBatch(drone, batch, failedTestResults)) {
              failedTestResults.add(batch);
            }
          }
        } while(!mShutdown && !isolatedWorkQueue.isEmpty());
      } catch(AbortDroneException ex) {
        mDrones.remove(drone); // return value not checked due to concurrent access
        mLogger.error("Aborting drone during isolated execution", ex);
        if(batch != null) {
          Preconditions.checkState(isolatedWorkQueue.add(batch),
              "Could not add batch to isolated queue " + batch);
        }
      }
    }
  }
  /**
   * Executes the test batch on the drone in question. If the command
   * exits with a status code of 255 throw an AbortDroneException.
   */
  private boolean executeTestBatch(Drone drone, TestBatch batch, Set<TestBatch> failedTestResults)
      throws IOException, SSHExecutionException, AbortDroneException {
    String scriptName = "hiveptest-" + batch.getName() + ".sh";
    File script = new File(mLocalScratchDirectory, scriptName);
    Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
    templateVariables.put("instanceName", drone.getInstanceName());
    templateVariables.put("batchName", batch.getName());
    templateVariables.put("testClass", batch.getTestClass());
    templateVariables.put("testArguments", batch.getTestArguments());
    templateVariables.put("localDir", drone.getLocalDirectory());
    templateVariables.put("logDir", drone.getLocalLogDirectory());
    templateVariables.put("maxSourceDirs", String.valueOf(MAX_SOURCE_DIRS));
    templateVariables.put("numOfFailedTests", String.valueOf(failedTestResults.size()));
    String command = Templates.getTemplateResult("bash $localDir/$instanceName/scratch/" + script.getName(),
        templateVariables);
    Templates.writeTemplateResult("batch-exec.vm", script, templateVariables);
    copyToDroneFromLocal(drone, script.getAbsolutePath(), "$localDir/$instanceName/scratch/" + scriptName);
    script.delete();
    mLogger.info(drone + " executing " + batch + " with " + command);
    RemoteCommandResult sshResult = new SSHCommand(mSSHCommandExecutor, drone.getPrivateKey(), drone.getUser(),
        drone.getHost(), drone.getInstance(), command).
        call();
    File batchLogDir = null;
    if(sshResult.getExitCode() == Constants.EXIT_CODE_UNKNOWN) {
      throw new AbortDroneException("Drone " + drone.toString() + " exited with " +
          Constants.EXIT_CODE_UNKNOWN + ": " + sshResult);
    }
    if(mShutdown) {
      mLogger.warn("Shutting down host " + mHost.getName());
      return false;
    }
    boolean result;
    if(sshResult.getExitCode() != 0 || sshResult.getException() != null) {
      result = false;
      batchLogDir = Dirs.create(new File(mFailedTestLogDir, batch.getName()));
    } else {
      result = true;
      batchLogDir = Dirs.create(new File(mSuccessfulTestLogDir, batch.getName()));
    }
    copyFromDroneToLocal(drone, batchLogDir.getAbsolutePath(),
        drone.getLocalLogDirectory() + "/");
    if(failedTestResults.size() > MAX_SOURCE_DIRS) {
      File sourceDir = new File(batchLogDir, "source");
      if(sourceDir.isDirectory()) {
        mLogger.info("Max source directories exceeded, deleting " + sourceDir.getAbsolutePath()
            + ":" + FileUtils.deleteQuietly(sourceDir));
      }
    }
    File logFile = new File(batchLogDir, String.format("%s.txt", batch.getName()));
    PrintWriter writer = new PrintWriter(logFile);
    writer.write(String.format("result = '%s'\n", sshResult.toString()));
    writer.write(String.format("output = '%s'\n", sshResult.getOutput()));
    if(sshResult.getException() != null) {
      sshResult.getException().printStackTrace(writer);
    }
    writer.close();
    return result;
  }
  /**
   * RSync from a single drone. If the command exits with a status of not 0
   * throw an AbortDroneException.
   */
  RSyncResult copyToDroneFromLocal(Drone drone, String localFile, String remoteFile)
      throws AbortDroneException, SSHExecutionException, IOException {
    Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
    templateVariables.put("instanceName", drone.getInstanceName());
    templateVariables.put("localDir", drone.getLocalDirectory());
    RSyncResult result = new RSyncCommand(mRSyncCommandExecutor, drone.getPrivateKey(), drone.getUser(),
        drone.getHost(), drone.getInstance(),
        Templates.getTemplateResult(localFile, templateVariables),
        Templates.getTemplateResult(remoteFile, templateVariables),
        RSyncCommand.Type.FROM_LOCAL).call();
    if(result.getExitCode() != Constants.EXIT_CODE_SUCCESS) {
      throw new AbortDroneException("Drone " + drone + " exited with " +
          result.getExitCode() + ": " + result);
    }
    if(result.getException() != null || result.getExitCode() != 0) {
      throw new SSHExecutionException(result);
    }
    return result;
  }
  /**
   * RSync file to all drones. If any drones exit with a status of not 0
   * they will be removed from use possibly leaving this host with zero
   * functioning drones.
   */
  ListenableFuture<List<ListenableFuture<RemoteCommandResult>>> rsyncFromLocalToRemoteInstances(final String localFile, final String remoteFile)
      throws InterruptedException, IOException {
    // the basic premise here is that we will rsync the directory to first working drone
    // then execute a local rsync on the node to the other drones. This keeps
    // us from executing tons of rsyncs on the master node conserving CPU
    return mExecutor.submit(new Callable<List<ListenableFuture<RemoteCommandResult>>>() {
      @Override
      public List<ListenableFuture<RemoteCommandResult>> call()
          throws Exception {
        List<Drone> drones = Lists.newArrayList(mDrones);
        List<ListenableFuture<RemoteCommandResult>> results = Lists.newArrayList();
        // local path doesn't depend on drone variables
        String resolvedLocalLocation = Files.simplifyPath(Templates.getTemplateResult(localFile, mTemplateDefaults));
        String remoteStagingLocation = null;
        for(final Drone drone : ImmutableList.copyOf(mDrones)) {
          Preconditions.checkState(remoteStagingLocation == null, "Remote staging location must be null at the start of the loop");
          final Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
          templateVariables.put("instanceName", drone.getInstanceName());
          templateVariables.put("localDir", drone.getLocalDirectory());
          String resolvedRemoteLocation = Files.simplifyPath(Templates.getTemplateResult(remoteFile, templateVariables));
          RSyncResult result = new RSyncCommand(mRSyncCommandExecutor, drone.getPrivateKey(), drone.getUser(),
              drone.getHost(), drone.getInstance(),
              resolvedLocalLocation,
              resolvedRemoteLocation,
              RSyncCommand.Type.FROM_LOCAL).call();
          if(result.getExitCode() == Constants.EXIT_CODE_SUCCESS) {
            remoteStagingLocation = resolvedRemoteLocation;
            drones.remove(drone);
            mLogger.info("Successfully staged " + resolvedLocalLocation + " on " + remoteStagingLocation);
            break;
          } else {
            mDrones.remove(drone);
            mLogger.error("Aborting drone during rsync",
                new AbortDroneException("Drone " + drone + " exited with "
                    + result.getExitCode() + ": " + result));
          }
        }
        if(remoteStagingLocation == null) {
          Preconditions.checkState(mDrones.isEmpty(), "If remote staging location is not set all drones should be bad");
          mLogger.warn("Unable to stage directory on remote host, all drones must be bad");
        } else {
          String name = (new File(resolvedLocalLocation)).getName();
          remoteStagingLocation = Files.simplifyPath(remoteStagingLocation + "/" + name);
          results.addAll(execInstances(drones, "rsync -qaPe --delete --delete-during --force " + remoteStagingLocation + " " + remoteFile));
        }
        return results;
      }
    });
  }
  RSyncResult copyFromDroneToLocal(Drone drone, String localFile, String remoteFile)
      throws SSHExecutionException, IOException {
    Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
    templateVariables.put("instanceName", drone.getInstanceName());
    templateVariables.put("localDir", drone.getLocalDirectory());
    RSyncResult result = new RSyncCommand(mRSyncCommandExecutor, drone.getPrivateKey(), drone.getUser(),
        drone.getHost(), drone.getInstance(),
        Templates.getTemplateResult(localFile, templateVariables),
        Templates.getTemplateResult(remoteFile, templateVariables),
        RSyncCommand.Type.TO_LOCAL).call();
    if(result.getException() != null || result.getExitCode() != Constants.EXIT_CODE_SUCCESS) {
      throw new SSHExecutionException(result);
    }
    return result;
  }
  /**
   * Execute command on at least one drone. The method will retry when the command
   * exits with a status code of 255 until all drones have been utilized, possibly
   * excluding the host from future use.
   */
  ListenableFuture<SSHResult> exec(final String cmd)
      throws Exception {
    return mExecutor.submit(new Callable<SSHResult>() {
      @Override
      public SSHResult call() throws Exception {
        for(final Drone drone : ImmutableList.copyOf(mDrones)) {
          Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
          templateVariables.put("instanceName", drone.getInstanceName());
          templateVariables.put("localDir", drone.getLocalDirectory());
          String command = Templates.getTemplateResult(cmd, templateVariables);
          SSHResult result = new SSHCommand(mSSHCommandExecutor, drone.getPrivateKey(), drone.getUser(),
              drone.getHost(), drone.getInstance(), command).call();
          if(result.getExitCode() == Constants.EXIT_CODE_UNKNOWN) {
            mDrones.remove(drone); // return value not checked due to concurrent access
            mLogger.error("Aborting drone during exec " + command,
                new AbortDroneException("Drone " + drone + " exited with "
                    + Constants.EXIT_CODE_UNKNOWN + ": " + result));
          } else {
            return result;
          }
        }
        return null;
      }
    });

  }
  List<ListenableFuture<RemoteCommandResult>> execInstances(final String cmd)
      throws InterruptedException, IOException {
    return execInstances(mDrones, cmd);
  }
  private List<ListenableFuture<RemoteCommandResult>> execInstances(List<Drone> drones, final String cmd)
      throws InterruptedException, IOException {
    List<ListenableFuture<RemoteCommandResult>> result = Lists.newArrayList();
    for(final Drone drone : ImmutableList.copyOf(drones)) {
      result.add(mExecutor.submit(new Callable<RemoteCommandResult>() {
        @Override
        public RemoteCommandResult call() throws Exception {
          Map<String, String> templateVariables = Maps.newHashMap(mTemplateDefaults);
          templateVariables.put("instanceName", drone.getInstanceName());
          templateVariables.put("localDir", drone.getLocalDirectory());
          String command = Templates.getTemplateResult(cmd, templateVariables);
          SSHResult result = new SSHCommand(mSSHCommandExecutor, drone.getPrivateKey(), drone.getUser(),
              drone.getHost(), drone.getInstance(), command).call();
          if(result.getExitCode() != Constants.EXIT_CODE_SUCCESS) {
            mDrones.remove(drone); // return value not checked due to concurrent access
            mLogger.error("Aborting drone during exec " + command,
                new AbortDroneException("Drone " + drone + " exited with "
                    + result.getExitCode() + ": " + result));
            return null;
          } else {
            return result;
          }
        }
      }));
    }
    return result;
  }
}
