/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hive.spark.client.SparkClientUtilities.HIVE_KRYO_REG_NAME;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hive.spark.client.rpc.Rpc;
import org.apache.hive.spark.client.rpc.RpcConfiguration;
import org.apache.hive.spark.client.rpc.RpcServer;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract implementation of {@link SparkClient} that allows sub-classes to override how the
 * spark application is launched. It provides the following functionality: (1) creating the client
 * connection to the {@link RemoteDriver} and managing its lifecycle, (2) monitoring the thread
 * used to submit the Spark application, (3) safe shutdown of the {@link RemoteDriver}, and (4)
 * configuration handling for submitting the Spark application.
 *
 * <p>
 *   This class contains the client protocol used to communicate with the {@link RemoteDriver}.
 *   It uses this protocol to submit {@link Job}s to the {@link RemoteDriver}.
 * </p>
 */
abstract class AbstractSparkClient implements SparkClient {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkClient.class);

  private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds

  private static final String OSX_TEST_OPTS = "SPARK_OSX_TEST_OPTS";
  private static final String DRIVER_OPTS_KEY = "spark.driver.extraJavaOptions";
  private static final String EXECUTOR_OPTS_KEY = "spark.executor.extraJavaOptions";
  private static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
  private static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";
  private static final String SPARK_DEPLOY_MODE = "spark.submit.deployMode";

  protected final Map<String, String> conf;
  private final HiveConf hiveConf;
  private final Future<Void> driverFuture;
  private final Map<String, JobHandleImpl<?>> jobs;
  private final Rpc driverRpc;
  private final ClientProtocol protocol;
  protected volatile boolean isAlive;

  protected AbstractSparkClient(RpcServer rpcServer, Map<String, String> conf, HiveConf hiveConf,
                  String sessionid) throws IOException {
    this.conf = conf;
    this.hiveConf = hiveConf;
    this.jobs = Maps.newConcurrentMap();

    String secret = rpcServer.createSecret();
    this.driverFuture = startDriver(rpcServer, sessionid, secret);
    this.protocol = new ClientProtocol();

    try {
      // The RPC server will take care of timeouts here.
      this.driverRpc = rpcServer.registerClient(sessionid, secret, protocol).get();
    } catch (Throwable e) {
      String errorMsg;
      if (e.getCause() instanceof TimeoutException) {
        errorMsg = "Timed out waiting for Remote Spark Driver to connect to HiveServer2.\nPossible reasons " +
            "include network issues, errors in remote driver, cluster has no available resources, etc." +
            "\nPlease check YARN or Spark driver's logs for further information.";
      } else if (e.getCause() instanceof InterruptedException) {
        errorMsg = "Interrupted while waiting for Remote Spark Driver to connect to HiveServer2.\nIt is possible " +
            "that the query was cancelled which would cause the Spark Session to close.";
      } else {
        errorMsg = "Error while waiting for Remote Spark Driver to connect back to HiveServer2.";
      }
      if (driverFuture.isDone()) {
        try {
          driverFuture.get();
        } catch (InterruptedException ie) {
          // Give up.
          LOG.warn("Interrupted before driver thread was finished.", ie);
        } catch (ExecutionException ee) {
          LOG.error("Driver thread failed", ee);
        }
      } else {
        driverFuture.cancel(true);
      }
      throw new RuntimeException(errorMsg, e);
    }

    LOG.info("Successfully connected to Remote Spark Driver at: " + this.driverRpc.getRemoteAddress());

    driverRpc.addListener(new Rpc.Listener() {
        @Override
        public void rpcClosed(Rpc rpc) {
          if (isAlive) {
            LOG.warn("Connection to Remote Spark Driver {} closed unexpectedly", driverRpc.getRemoteAddress());
            isAlive = false;
          }
        }

        @Override
        public String toString() {
          return "Connection to Remote Spark Driver Closed Unexpectedly";
        }
    });
    isAlive = true;
  }

  @Override
  public <T extends Serializable> JobHandle<T> submit(Job<T> job) {
    return protocol.submit(job, Collections.<JobHandle.Listener<T>>emptyList());
  }

  @Override
  public <T extends Serializable> JobHandle<T> submit(Job<T> job, List<JobHandle.Listener<T>> listeners) {
    return protocol.submit(job, listeners);
  }

  @Override
  public <T extends Serializable> Future<T> run(Job<T> job) {
    return protocol.run(job);
  }

  @Override
  public void stop() {
    if (isAlive) {
      isAlive = false;
      try {
        protocol.endSession();
      } catch (Exception e) {
        LOG.warn("Exception while waiting for end session reply.", e);
      } finally {
        driverRpc.close();
      }
    }

    try {
      driverFuture.get(DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      LOG.error("Exception while waiting for driver future to complete", e);
    } catch (TimeoutException e) {
      LOG.warn("Timed out shutting down remote driver, cancelling...");
      driverFuture.cancel(true);
    } catch (InterruptedException ie) {
      LOG.debug("Interrupted before driver thread was finished.");
      driverFuture.cancel(true);
    }
  }

  @Override
  public Future<?> addJar(URI uri) {
    return run(new AddJarJob(uri.toString()));
  }

  @Override
  public Future<?> addFile(URI uri) {
    return run(new AddFileJob(uri.toString()));
  }

  @Override
  public Future<Integer> getExecutorCount() {
    return run(new GetExecutorCountJob());
  }

  @Override
  public Future<Integer> getDefaultParallelism() {
    return run(new GetDefaultParallelismJob());
  }

  @Override
  public boolean isActive() {
    return isAlive && driverRpc.isActive();
  }

  @Override
  public void cancel(String jobId) {
    protocol.cancel(jobId);
  }

  private Future<Void> startDriver(final RpcServer rpcServer, final String clientId,
                                   final String secret) throws IOException {
    final String serverAddress = rpcServer.getAddress();
    final String serverPort = String.valueOf(rpcServer.getPort());

    String sparkHome = getSparkHome();

    String sparkLogDir = conf.get("hive.spark.log.dir");
    if (sparkLogDir == null) {
      if (sparkHome == null) {
        sparkLogDir = "./target/";
      } else {
        sparkLogDir = sparkHome + "/logs/";
      }
    }

    String osxTestOpts = "";
    if (Strings.nullToEmpty(System.getProperty("os.name")).toLowerCase().contains("mac")) {
      osxTestOpts = Strings.nullToEmpty(System.getenv(OSX_TEST_OPTS));
    }

    String driverJavaOpts = Joiner.on(" ").skipNulls().join(
        "-Dhive.spark.log.dir=" + sparkLogDir, osxTestOpts, conf.get(DRIVER_OPTS_KEY));
    String executorJavaOpts = Joiner.on(" ").skipNulls().join(
        "-Dhive.spark.log.dir=" + sparkLogDir, osxTestOpts, conf.get(EXECUTOR_OPTS_KEY));

    // Create a file with all the job properties to be read by spark-submit. Change the
    // file's permissions so that only the owner can read it. This avoid having the
    // connection secret show up in the child process's command line.
    File properties = File.createTempFile("spark-submit.", ".properties");
    if (!properties.setReadable(false) || !properties.setReadable(true, true)) {
      throw new IOException("Cannot change permissions of job properties file.");
    }
    properties.deleteOnExit();

    Properties allProps = new Properties();
    // first load the defaults from spark-defaults.conf if available
    try {
      URL sparkDefaultsUrl = Thread.currentThread().getContextClassLoader().getResource("spark-defaults.conf");
      if (sparkDefaultsUrl != null) {
        LOG.info("Loading spark defaults configs from: " + sparkDefaultsUrl);
        allProps.load(new ByteArrayInputStream(Resources.toByteArray(sparkDefaultsUrl)));
      }
    } catch (Exception e) {
      String msg = "Exception trying to load spark-defaults.conf: " + e;
      throw new IOException(msg, e);
    }
    // then load the SparkClientImpl config
    for (Map.Entry<String, String> e : conf.entrySet()) {
      allProps.put(e.getKey(), conf.get(e.getKey()));
    }
    allProps.put(SparkClientFactory.CONF_CLIENT_ID, clientId);
    allProps.put(SparkClientFactory.CONF_KEY_SECRET, secret);
    allProps.put(DRIVER_OPTS_KEY, driverJavaOpts);
    allProps.put(EXECUTOR_OPTS_KEY, executorJavaOpts);

    String isTesting = conf.get("spark.testing");
    if (isTesting != null && isTesting.equalsIgnoreCase("true")) {
      String hiveHadoopTestClasspath = Strings.nullToEmpty(System.getenv("HIVE_HADOOP_TEST_CLASSPATH"));
      if (!hiveHadoopTestClasspath.isEmpty()) {
        String extraDriverClasspath = Strings.nullToEmpty((String)allProps.get(DRIVER_EXTRA_CLASSPATH));
        if (extraDriverClasspath.isEmpty()) {
          allProps.put(DRIVER_EXTRA_CLASSPATH, hiveHadoopTestClasspath);
        } else {
          extraDriverClasspath = extraDriverClasspath.endsWith(File.pathSeparator) ? extraDriverClasspath : extraDriverClasspath + File.pathSeparator;
          allProps.put(DRIVER_EXTRA_CLASSPATH, extraDriverClasspath + hiveHadoopTestClasspath);
        }

        String extraExecutorClasspath = Strings.nullToEmpty((String)allProps.get(EXECUTOR_EXTRA_CLASSPATH));
        if (extraExecutorClasspath.isEmpty()) {
          allProps.put(EXECUTOR_EXTRA_CLASSPATH, hiveHadoopTestClasspath);
        } else {
          extraExecutorClasspath = extraExecutorClasspath.endsWith(File.pathSeparator) ? extraExecutorClasspath : extraExecutorClasspath + File.pathSeparator;
          allProps.put(EXECUTOR_EXTRA_CLASSPATH, extraExecutorClasspath + hiveHadoopTestClasspath);
        }
      }
    }

    Writer writer = new OutputStreamWriter(new FileOutputStream(properties), Charsets.UTF_8);
    try {
      allProps.store(writer, "Spark Context configuration");
    } finally {
      writer.close();
    }

    // Define how to pass options to the child process. If launching in client (or local)
    // mode, the driver options need to be passed directly on the command line. Otherwise,
    // SparkSubmit will take care of that for us.
    String master = conf.get("spark.master");
    Preconditions.checkArgument(master != null, "spark.master is not defined.");
    String deployMode = conf.get(SPARK_DEPLOY_MODE);

    if (SparkClientUtilities.isYarnClusterMode(master, deployMode)) {
      String executorCores = conf.get("spark.executor.cores");
      if (executorCores != null) {
        addExecutorCores(executorCores);
      }

      String executorMemory = conf.get("spark.executor.memory");
      if (executorMemory != null) {
        addExecutorMemory(executorMemory);
      }

      String numOfExecutors = conf.get("spark.executor.instances");
      if (numOfExecutors != null) {
        addNumExecutors(numOfExecutors);
      }
    }
    // The options --principal/--keypad do not work with --proxy-user in spark-submit.sh
    // (see HIVE-15485, SPARK-5493, SPARK-19143), so Hive could only support doAs or
    // delegation token renewal, but not both. Since doAs is a more common case, if both
    // are needed, we choose to favor doAs. So when doAs is enabled, we use kinit command,
    // otherwise, we pass the principal/keypad to spark to support the token renewal for
    // long-running application.
    if ("kerberos".equals(hiveConf.get(HADOOP_SECURITY_AUTHENTICATION))) {
      String principal = SecurityUtil.getServerPrincipal(hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL),
          "0.0.0.0");
      String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
      boolean isDoAsEnabled = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
      if (StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(keyTabFile)) {
        addKeytabAndPrincipal(isDoAsEnabled, keyTabFile, principal);
      }
    }
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      try {
        String currentUser = Utils.getUGI().getShortUserName();
        // do not do impersonation in CLI mode
        if (!currentUser.equals(System.getProperty("user.name"))) {
          LOG.info("Attempting impersonation of " + currentUser);
          addProxyUser(currentUser);
        }
      } catch (Exception e) {
        String msg = "Cannot obtain username: " + e;
        throw new IllegalStateException(msg, e);
      }
    }

    String regStr = conf.get("spark.kryo.registrator");
    if (HIVE_KRYO_REG_NAME.equals(regStr)) {
      addJars(SparkClientUtilities.findKryoRegistratorJar(hiveConf));
    }

    addPropertiesFile(properties.getAbsolutePath());
    addClass(RemoteDriver.class.getName());

    String jar = "spark-internal";
    if (SparkContext.jarOfClass(this.getClass()).isDefined()) {
      jar = SparkContext.jarOfClass(this.getClass()).get();
    }
    addExecutableJar(jar);


    addAppArg(RemoteDriver.REMOTE_DRIVER_HOST_CONF);
    addAppArg(serverAddress);
    addAppArg(RemoteDriver.REMOTE_DRIVER_PORT_CONF);
    addAppArg(serverPort);

    //hive.spark.* keys are passed down to the RemoteDriver via REMOTE_DRIVER_CONF
    // so that they are not used in sparkContext but only in remote driver,
    //as --properties-file contains the spark.* keys that are meant for SparkConf object.
    for (String hiveSparkConfKey : RpcConfiguration.HIVE_SPARK_RSC_CONFIGS) {
      String value = RpcConfiguration.getValue(hiveConf, hiveSparkConfKey);
      addAppArg(RemoteDriver.REMOTE_DRIVER_CONF);
      addAppArg(String.format("%s=%s", hiveSparkConfKey, value));
    }

    return launchDriver(isTesting, rpcServer, clientId);
  }

  protected abstract Future<Void> launchDriver(String isTesting, RpcServer rpcServer, String
          clientId) throws IOException;

  protected abstract String getSparkHome();

  protected abstract void addAppArg(String arg);

  protected abstract void addExecutableJar(String jar);

  protected abstract void addPropertiesFile(String absolutePath);

  protected abstract void addClass(String name);

  protected abstract void addJars(String jars);

  protected abstract void addProxyUser(String proxyUser);

  protected abstract void addKeytabAndPrincipal(boolean isDoAsEnabled, String keyTabFile,
                                                String principal);

  protected abstract void addNumExecutors(String numOfExecutors);

  protected abstract void addExecutorMemory(String executorMemory);

  protected abstract void addExecutorCores(String executorCores);

  private class ClientProtocol extends BaseProtocol {

    <T extends Serializable> JobHandleImpl<T> submit(Job<T> job, List<JobHandle.Listener<T>> listeners) {
      final String jobId = UUID.randomUUID().toString();
      final Promise<T> promise = driverRpc.createPromise();
      final JobHandleImpl<T> handle =
          new JobHandleImpl<T>(AbstractSparkClient.this, promise, jobId, listeners);
      jobs.put(jobId, handle);

      final io.netty.util.concurrent.Future<Void> rpc = driverRpc.call(new JobRequest(jobId, job));
      LOG.debug("Send JobRequest[{}].", jobId);

      // Link the RPC and the promise so that events from one are propagated to the other as
      // needed.
      rpc.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
        @Override
        public void operationComplete(io.netty.util.concurrent.Future<Void> f) {
          if (f.isSuccess()) {
            // If the spark job finishes before this listener is called, the QUEUED status will not be set
            handle.changeState(JobHandle.State.QUEUED);
          } else if (!promise.isDone()) {
            promise.setFailure(f.cause());
          }
        }
      });
      promise.addListener(new GenericFutureListener<Promise<T>>() {
        @Override
        public void operationComplete(Promise<T> p) {
          if (jobId != null) {
            jobs.remove(jobId);
          }
          if (p.isCancelled() && !rpc.isDone()) {
            rpc.cancel(true);
          }
        }
      });
      return handle;
    }

    <T extends Serializable> Future<T> run(Job<T> job) {
      @SuppressWarnings("unchecked")
      final io.netty.util.concurrent.Future<T> rpc = (io.netty.util.concurrent.Future<T>)
        driverRpc.call(new SyncJobRequest(job), Serializable.class);
      return rpc;
    }

    void cancel(String jobId) {
      driverRpc.call(new CancelJob(jobId));
    }

    Future<?> endSession() {
      return driverRpc.call(new EndSession());
    }

    private void handle(ChannelHandlerContext ctx, Error msg) {
      LOG.warn("Error reported from Remote Spark Driver: {}", msg.cause);
    }

    private void handle(ChannelHandlerContext ctx, JobMetrics msg) {
      JobHandleImpl<?> handle = jobs.get(msg.jobId);
      if (handle != null) {
        handle.getMetrics().addMetrics(msg.sparkJobId, msg.stageId, msg.taskId, msg.metrics);
      } else {
        LOG.warn("Received metrics for unknown Spark job {}", msg.sparkJobId);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobResult msg) {
      JobHandleImpl<?> handle = jobs.remove(msg.id);
      if (handle != null) {
        LOG.debug("Received result for client job {}", msg.id);
        handle.setSparkCounters(msg.sparkCounters);
        Throwable error = msg.error;
        if (error == null) {
          handle.setSuccess(msg.result);
        } else {
          handle.setFailure(error);
        }
      } else {
        LOG.warn("Received result for unknown client job {}", msg.id);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobStarted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.id);
      if (handle != null) {
        handle.changeState(JobHandle.State.STARTED);
      } else {
        LOG.warn("Received event for unknown client job {}", msg.id);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.clientJobId);
      if (handle != null) {
        LOG.info("Received Spark job ID: {} for client job {}", msg.sparkJobId, msg.clientJobId);
        handle.addSparkJobId(msg.sparkJobId);
      } else {
        LOG.warn("Received Spark job ID: {} for unknown client job {}", msg.sparkJobId, msg.clientJobId);
      }
    }

    @Override
    protected String name() {
      return "HiveServer2 to Remote Spark Driver Connection";
    }
  }

  private static class AddJarJob implements Job<Serializable> {
    private static final long serialVersionUID = 1L;

    private final String path;

    AddJarJob() {
      this(null);
    }

    AddJarJob(String path) {
      this.path = path;
    }

    @Override
    public Serializable call(JobContext jc) throws Exception {
      jc.sc().addJar(path);
      // Following remote job may refer to classes in this jar, and the remote job would be executed
      // in a different thread, so we add this jar path to JobContext for further usage.
      jc.getAddedJars().put(path, System.currentTimeMillis());
      return null;
    }

  }

  private static class AddFileJob implements Job<Serializable> {
    private static final long serialVersionUID = 1L;

    private final String path;

    AddFileJob() {
      this(null);
    }

    AddFileJob(String path) {
      this.path = path;
    }

    @Override
    public Serializable call(JobContext jc) throws Exception {
      jc.sc().addFile(path);
      return null;
    }

  }

  private static class GetExecutorCountJob implements Job<Integer> {
      private static final long serialVersionUID = 1L;

      @Override
      public Integer call(JobContext jc) throws Exception {
        // minus 1 here otherwise driver is also counted as an executor
        int count = jc.sc().sc().getExecutorMemoryStatus().size() - 1;
        return Integer.valueOf(count);
      }

  }

  private static class GetDefaultParallelismJob implements Job<Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public Integer call(JobContext jc) throws Exception {
      return jc.sc().sc().defaultParallelism();
    }
  }

}
