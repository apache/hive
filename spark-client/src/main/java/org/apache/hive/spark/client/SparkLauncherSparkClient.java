/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.rpc.RpcServer;

import org.apache.spark.launcher.AbstractLauncher;
import org.apache.spark.launcher.InProcessLauncher;
import org.apache.spark.launcher.SparkAppHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;


/**
 * Extends the {@link AbstractSparkClient} and uses Spark's
 * {@link org.apache.spark.launcher.SparkLauncher} to submit the HoS application. Specifically,
 * it uses the {@link InProcessLauncher} to avoid spawning a sub-process to submit the Spark app.
 * It uses a {@link Thread} to monitor when the Spark app has been successfully submitted. The
 * thread can be interrupted, in which case the {@link RpcServer} client will be cancelled and
 * the Spark app will be stopped.
 */
public class SparkLauncherSparkClient extends AbstractSparkClient {

  private static final Logger LOG = LoggerFactory.getLogger(
          SparkLauncherSparkClient.class.getName());

  private static final long serialVersionUID = 2153000661341457380L;

  private static final Set<SparkAppHandle.State> FAILED_SPARK_STATES = Sets.newHashSet(
          SparkAppHandle.State.FAILED,
          SparkAppHandle.State.KILLED,
          SparkAppHandle.State.LOST);

  private transient AbstractLauncher<InProcessLauncher> sparkLauncher;

  SparkLauncherSparkClient(RpcServer rpcServer,
                                   Map<String, String> conf,
                                   HiveConf hiveConf,
                                   String sessionid) throws IOException {
    super(rpcServer, conf, hiveConf, sessionid);
  }

  @Override
  protected Future<Void> launchDriver(String isTesting, RpcServer rpcServer,
                                      String clientId) throws IOException {
    if (isTesting != null) {
      System.setProperty("spark.testing", "true");
    }

    // Only allow the spark.master to be local in unit tests
    if (isTesting == null) {
      Preconditions.checkArgument(SparkClientUtilities.isYarnClusterMode(
              this.conf.get("spark.master"), this.conf.get("spark.submit.deployMode")),
              getClass().getName() + " is only supported in yarn-cluster mode");
    }

    // Monitors when the Spark app has been successfully started
    CountDownLatch shutdownLatch = new CountDownLatch(1);

    // Submit the app
    SparkAppHandle sparkAppHandle = getSparkLauncher().startApplication(
            new SparkAppListener(shutdownLatch, rpcServer, clientId));

    return createSparkLauncherFuture(shutdownLatch, sparkAppHandle, rpcServer, clientId);
  }

  @VisibleForTesting
  static Future<Void> createSparkLauncherFuture(CountDownLatch shutdownLatch,
                                                SparkAppHandle sparkAppHandle, RpcServer rpcServer,
                                                String clientId) {
    // Monitor the countdown latch
    Callable<Void> runnable = () -> {
      try {
        shutdownLatch.await();
      } catch (InterruptedException e) {
        rpcServer.cancelClient(clientId, "Spark app launcher interrupted");
        sparkAppHandle.stop();
      }
      return null;
    };

    FutureTask<Void> futureTask = new FutureTask<>(runnable);

    Thread driverThread = new Thread(futureTask);
    driverThread.setDaemon(true);
    driverThread.setName("SparkLauncherMonitor");
    driverThread.start();

    return futureTask;
  }

  @Override
  protected String getSparkHome() {
    return null;
  }

  @Override
  protected void addAppArg(String arg) {
    getSparkLauncher().addAppArgs(arg);
  }

  @Override
  protected void addExecutableJar(String jar) {
    getSparkLauncher().setAppResource(jar);
  }

  @Override
  protected void addPropertiesFile(String absolutePath) {
    getSparkLauncher().setPropertiesFile(absolutePath);
  }

  @Override
  protected void addClass(String name) {
    getSparkLauncher().setMainClass(name);
  }

  @Override
  protected void addJars(String jars) {
    getSparkLauncher().addJar(jars);
  }

  @Override
  protected void addProxyUser(String proxyUser) {
    throw new UnsupportedOperationException();
//    getSparkLauncher().addSparkArg("--proxy-user", proxyUser);
  }

  @Override
  protected void addKeytabAndPrincipal(boolean isDoAsEnabled, String keyTabFile, String principal) {
    throw new UnsupportedOperationException();
//    getSparkLauncher().addSparkArg("--principal", principal);
//    getSparkLauncher().addSparkArg("--keytab", keyTabFile);
  }

  @Override
  protected void addNumExecutors(String numOfExecutors) {
    getSparkLauncher().addSparkArg("--num-executors", numOfExecutors);
  }

  @Override
  protected void addExecutorMemory(String executorMemory) {
    getSparkLauncher().addSparkArg("--executor-memory", executorMemory);
  }

  @Override
  protected void addExecutorCores(String executorCores) {
    getSparkLauncher().addSparkArg("--executor-cores", executorCores);
  }

  private AbstractLauncher<InProcessLauncher> getSparkLauncher() {
    if (this.sparkLauncher == null) {
      this.sparkLauncher = new InProcessLauncher();
    }
    return this.sparkLauncher;
  }

  @VisibleForTesting
  static final class SparkAppListener implements SparkAppHandle.Listener {

    private final CountDownLatch shutdownLatch;
    private final RpcServer rpcServer;
    private final String clientId;

    SparkAppListener(CountDownLatch shutdownLatch, RpcServer rpcServer, String clientId) {
      this.shutdownLatch = shutdownLatch;
      this.rpcServer = rpcServer;
      this.clientId = clientId;
    }

    @Override
    public void stateChanged(SparkAppHandle sparkAppHandle) {
      LOG.info("Spark app transitioned to state = " + sparkAppHandle.getState());
      if (sparkAppHandle.getState().isFinal() || sparkAppHandle.getState().equals(
              SparkAppHandle.State.RUNNING)) {
        this.shutdownLatch.countDown();
        sparkAppHandle.disconnect();
        LOG.info("Successfully disconnected from Spark app handle");
      }
      if (FAILED_SPARK_STATES.contains(sparkAppHandle.getState())) {
        this.rpcServer.cancelClient(this.clientId, "Spark app launcher failed," +
                " transitioned to state " + sparkAppHandle.getState());
      }
    }

    @Override
    public void infoChanged(SparkAppHandle sparkAppHandle) {
      // Do nothing
    }
  }
}
