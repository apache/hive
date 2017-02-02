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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hive.spark.client.rpc.Rpc;
import org.apache.hive.spark.client.rpc.RpcConfiguration;
import org.apache.hive.spark.client.rpc.RpcServer;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkClientImpl implements SparkClient {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SparkClientImpl.class);

  private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds
  private static final long MAX_ERR_LOG_LINES_FOR_RPC = 1000;

  private static final String OSX_TEST_OPTS = "SPARK_OSX_TEST_OPTS";
  private static final String SPARK_HOME_ENV = "SPARK_HOME";
  private static final String SPARK_HOME_KEY = "spark.home";
  private static final String DRIVER_OPTS_KEY = "spark.driver.extraJavaOptions";
  private static final String EXECUTOR_OPTS_KEY = "spark.executor.extraJavaOptions";
  private static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
  private static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";

  private final Map<String, String> conf;
  private final HiveConf hiveConf;
  private final AtomicInteger childIdGenerator;
  private final Thread driverThread;
  private final Map<String, JobHandleImpl<?>> jobs;
  private final Rpc driverRpc;
  private final ClientProtocol protocol;
  private volatile boolean isAlive;

  SparkClientImpl(RpcServer rpcServer, Map<String, String> conf, HiveConf hiveConf) throws IOException, SparkException {
    this.conf = conf;
    this.hiveConf = hiveConf;
    this.childIdGenerator = new AtomicInteger();
    this.jobs = Maps.newConcurrentMap();

    String clientId = UUID.randomUUID().toString();
    String secret = rpcServer.createSecret();
    this.driverThread = startDriver(rpcServer, clientId, secret);
    this.protocol = new ClientProtocol();

    try {
      // The RPC server will take care of timeouts here.
      this.driverRpc = rpcServer.registerClient(clientId, secret, protocol).get();
    } catch (Throwable e) {
      if (e.getCause() instanceof TimeoutException) {
        LOG.error("Timed out waiting for client to connect.\nPossible reasons include network " +
            "issues, errors in remote driver or the cluster has no available resources, etc." +
            "\nPlease check YARN or Spark driver's logs for further information.", e);
      } else {
        LOG.error("Error while waiting for client to connect.", e);
      }
      driverThread.interrupt();
      try {
        driverThread.join();
      } catch (InterruptedException ie) {
        // Give up.
        LOG.debug("Interrupted before driver thread was finished.");
      }
      throw Throwables.propagate(e);
    }

    driverRpc.addListener(new Rpc.Listener() {
        @Override
        public void rpcClosed(Rpc rpc) {
          if (isAlive) {
            LOG.warn("Client RPC channel closed unexpectedly.");
            isAlive = false;
          }
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

    long endTime = System.currentTimeMillis() + DEFAULT_SHUTDOWN_TIMEOUT;
    try {
      driverThread.join(DEFAULT_SHUTDOWN_TIMEOUT);
    } catch (InterruptedException ie) {
      LOG.debug("Interrupted before driver thread was finished.");
    }
    if (endTime - System.currentTimeMillis() <= 0) {
      LOG.warn("Timed out shutting down remote driver, interrupting...");
      driverThread.interrupt();
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

  void cancel(String jobId) {
    protocol.cancel(jobId);
  }

  private Thread startDriver(final RpcServer rpcServer, final String clientId, final String secret)
      throws IOException {
    Runnable runnable;
    final String serverAddress = rpcServer.getAddress();
    final String serverPort = String.valueOf(rpcServer.getPort());

    if (conf.containsKey(SparkClientFactory.CONF_KEY_IN_PROCESS)) {
      // Mostly for testing things quickly. Do not do this in production.
      // when invoked in-process it inherits the environment variables of the parent
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      runnable = new Runnable() {
        @Override
        public void run() {
          List<String> args = Lists.newArrayList();
          args.add("--remote-host");
          args.add(serverAddress);
          args.add("--remote-port");
          args.add(serverPort);
          args.add("--client-id");
          args.add(clientId);
          args.add("--secret");
          args.add(secret);

          for (Map.Entry<String, String> e : conf.entrySet()) {
            args.add("--conf");
            args.add(String.format("%s=%s", e.getKey(), conf.get(e.getKey())));
          }
          try {
            RemoteDriver.main(args.toArray(new String[args.size()]));
          } catch (Exception e) {
            LOG.error("Error running driver.", e);
          }
        }
      };
    } else {
      // If a Spark installation is provided, use the spark-submit script. Otherwise, call the
      // SparkSubmit class directly, which has some caveats (like having to provide a proper
      // version of Guava on the classpath depending on the deploy mode).
      String sparkHome = Strings.emptyToNull(conf.get(SPARK_HOME_KEY));
      if (sparkHome == null) {
        sparkHome = Strings.emptyToNull(System.getenv(SPARK_HOME_ENV));
      }
      if (sparkHome == null) {
        sparkHome = Strings.emptyToNull(System.getProperty(SPARK_HOME_KEY));
      }
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
          LOG.info("Loading spark defaults: " + sparkDefaultsUrl);
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
      String deployMode = conf.get("spark.submit.deployMode");

      List<String> argv = Lists.newLinkedList();

      if (sparkHome != null) {
        argv.add(new File(sparkHome, "bin/spark-submit").getAbsolutePath());
      } else {
        LOG.info("No spark.home provided, calling SparkSubmit directly.");
        argv.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());

        if (master.startsWith("local") || master.startsWith("mesos") ||
            SparkClientUtilities.isYarnClientMode(master, deployMode) ||
            master.startsWith("spark")) {
          String mem = conf.get("spark.driver.memory");
          if (mem != null) {
            argv.add("-Xms" + mem);
            argv.add("-Xmx" + mem);
          }

          String cp = conf.get("spark.driver.extraClassPath");
          if (cp != null) {
            argv.add("-classpath");
            argv.add(cp);
          }

          String libPath = conf.get("spark.driver.extraLibPath");
          if (libPath != null) {
            argv.add("-Djava.library.path=" + libPath);
          }

          String extra = conf.get(DRIVER_OPTS_KEY);
          if (extra != null) {
            for (String opt : extra.split("[ ]")) {
              if (!opt.trim().isEmpty()) {
                argv.add(opt.trim());
              }
            }
          }
        }

        argv.add("org.apache.spark.deploy.SparkSubmit");
      }

      if (SparkClientUtilities.isYarnClusterMode(master, deployMode)) {
        String executorCores = conf.get("spark.executor.cores");
        if (executorCores != null) {
          argv.add("--executor-cores");
          argv.add(executorCores);
        }

        String executorMemory = conf.get("spark.executor.memory");
        if (executorMemory != null) {
          argv.add("--executor-memory");
          argv.add(executorMemory);
        }

        String numOfExecutors = conf.get("spark.executor.instances");
        if (numOfExecutors != null) {
          argv.add("--num-executors");
          argv.add(numOfExecutors);
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
        if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
          List<String> kinitArgv = Lists.newLinkedList();
          kinitArgv.add("kinit");
          kinitArgv.add(principal);
          kinitArgv.add("-k");
          kinitArgv.add("-t");
          kinitArgv.add(keyTabFile + ";");
          kinitArgv.addAll(argv);
          argv = kinitArgv;
        } else {
          // if doAs is not enabled, we pass the principal/keypad to spark-submit in order to
          // support the possible delegation token renewal in Spark
          argv.add("--principal");
          argv.add(principal);
          argv.add("--keytab");
          argv.add(keyTabFile);
        }
      }
      if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
        try {
          String currentUser = Utils.getUGI().getShortUserName();
          // do not do impersonation in CLI mode
          if (!currentUser.equals(System.getProperty("user.name"))) {
            LOG.info("Attempting impersonation of " + currentUser);
            argv.add("--proxy-user");
            argv.add(currentUser);
          }
        } catch (Exception e) {
          String msg = "Cannot obtain username: " + e;
          throw new IllegalStateException(msg, e);
        }
      }

      argv.add("--properties-file");
      argv.add(properties.getAbsolutePath());
      argv.add("--class");
      argv.add(RemoteDriver.class.getName());

      String jar = "spark-internal";
      if (SparkContext.jarOfClass(this.getClass()).isDefined()) {
        jar = SparkContext.jarOfClass(this.getClass()).get();
      }
      argv.add(jar);

      argv.add("--remote-host");
      argv.add(serverAddress);
      argv.add("--remote-port");
      argv.add(serverPort);

      //hive.spark.* keys are passed down to the RemoteDriver via --conf,
      //as --properties-file contains the spark.* keys that are meant for SparkConf object.
      for (String hiveSparkConfKey : RpcConfiguration.HIVE_SPARK_RSC_CONFIGS) {
        String value = RpcConfiguration.getValue(hiveConf, hiveSparkConfKey);
        argv.add("--conf");
        argv.add(String.format("%s=%s", hiveSparkConfKey, value));
      }

      String cmd = Joiner.on(" ").join(argv);
      LOG.info("Running client driver with argv: {}", cmd);
      ProcessBuilder pb = new ProcessBuilder("sh", "-c", cmd);

      // Prevent hive configurations from being visible in Spark.
      pb.environment().remove("HIVE_HOME");
      pb.environment().remove("HIVE_CONF_DIR");
      // Add credential provider password to the child process's environment
      // In case of Spark the credential provider location is provided in the jobConf when the job is submitted
      String password = getSparkJobCredentialProviderPassword();
      if(password != null) {
        pb.environment().put(Constants.HADOOP_CREDENTIAL_PASSWORD_ENVVAR, password);
      }
      if (isTesting != null) {
        pb.environment().put("SPARK_TESTING", isTesting);
      }

      final Process child = pb.start();
      int childId = childIdGenerator.incrementAndGet();
      final List<String> childErrorLog = new ArrayList<String>();
      redirect("stdout-redir-" + childId, new Redirector(child.getInputStream()));
      redirect("stderr-redir-" + childId, new Redirector(child.getErrorStream(), childErrorLog));

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              StringBuilder errStr = new StringBuilder();
              for (String s : childErrorLog) {
                errStr.append(s);
                errStr.append('\n');
              }

              rpcServer.cancelClient(clientId,
                  "Child process exited before connecting back with error log " + errStr.toString());
              LOG.warn("Child process exited with code {}", exitCode);
            }
          } catch (InterruptedException ie) {
            LOG.warn("Waiting thread interrupted, killing child process.");
            Thread.interrupted();
            child.destroy();
          } catch (Exception e) {
            LOG.warn("Exception while waiting for child process.", e);
          }
        }
      };
    }

    Thread thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.setName("Driver");
    thread.start();
    return thread;
  }

  private String getSparkJobCredentialProviderPassword() {
    if (conf.containsKey("spark.yarn.appMasterEnv.HADOOP_CREDSTORE_PASSWORD")) {
      return conf.get("spark.yarn.appMasterEnv.HADOOP_CREDSTORE_PASSWORD");
    } else if (conf.containsKey("spark.executorEnv.HADOOP_CREDSTORE_PASSWORD")) {
      return conf.get("spark.executorEnv.HADOOP_CREDSTORE_PASSWORD");
    }
    return null;
  }

  private void redirect(String name, Redirector redirector) {
    Thread thread = new Thread(redirector);
    thread.setName(name);
    thread.setDaemon(true);
    thread.start();
  }

  private class ClientProtocol extends BaseProtocol {

    <T extends Serializable> JobHandleImpl<T> submit(Job<T> job, List<JobHandle.Listener<T>> listeners) {
      final String jobId = UUID.randomUUID().toString();
      final Promise<T> promise = driverRpc.createPromise();
      final JobHandleImpl<T> handle =
          new JobHandleImpl<T>(SparkClientImpl.this, promise, jobId, listeners);
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
      LOG.warn("Error reported from remote driver.", msg.cause);
    }

    private void handle(ChannelHandlerContext ctx, JobMetrics msg) {
      JobHandleImpl<?> handle = jobs.get(msg.jobId);
      if (handle != null) {
        handle.getMetrics().addMetrics(msg.sparkJobId, msg.stageId, msg.taskId, msg.metrics);
      } else {
        LOG.warn("Received metrics for unknown job {}", msg.jobId);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobResult msg) {
      JobHandleImpl<?> handle = jobs.remove(msg.id);
      if (handle != null) {
        LOG.info("Received result for {}", msg.id);
        handle.setSparkCounters(msg.sparkCounters);
        Throwable error = msg.error != null ? new SparkException(msg.error) : null;
        if (error == null) {
          handle.setSuccess(msg.result);
        } else {
          handle.setFailure(error);
        }
      } else {
        LOG.warn("Received result for unknown job {}", msg.id);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobStarted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.id);
      if (handle != null) {
        handle.changeState(JobHandle.State.STARTED);
      } else {
        LOG.warn("Received event for unknown job {}", msg.id);
      }
    }

    private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.clientJobId);
      if (handle != null) {
        LOG.info("Received spark job ID: {} for {}", msg.sparkJobId, msg.clientJobId);
        handle.addSparkJobId(msg.sparkJobId);
      } else {
        LOG.warn("Received spark job ID: {} for unknown job {}", msg.sparkJobId, msg.clientJobId);
      }
    }

  }

  private class Redirector implements Runnable {

    private final BufferedReader in;
    private List<String> errLogs;
    private int numErrLogLines = 0;

    Redirector(InputStream in) {
      this.in = new BufferedReader(new InputStreamReader(in));
    }

    Redirector(InputStream in, List<String> errLogs) {
      this.in = new BufferedReader(new InputStreamReader(in));
      this.errLogs = errLogs;
    }

    @Override
    public void run() {
      try {
        String line = null;
        while ((line = in.readLine()) != null) {
          LOG.info(line);
          if (errLogs != null) {
            if (numErrLogLines++ < MAX_ERR_LOG_LINES_FOR_RPC) {
              errLogs.add(line);
            }
          }
        }
      } catch (IOException e) {
        if (isAlive) {
          LOG.warn("I/O error in redirector thread.", e);
        } else {
          // When stopping the remote driver the process might be destroyed during reading from the stream.
          // We should not log the related exceptions in a visible level as they might mislead the user.
          LOG.debug("I/O error in redirector thread while stopping the remote driver", e);
        }
      } catch (Exception e) {
        LOG.warn("Error in redirector thread.", e);
      }
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
