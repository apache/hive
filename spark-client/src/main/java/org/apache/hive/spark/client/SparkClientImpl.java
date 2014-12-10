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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.spark.client.rpc.Rpc;
import org.apache.hive.spark.client.rpc.RpcServer;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;

class SparkClientImpl implements SparkClient {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SparkClientImpl.class);

  private static final String DEFAULT_CONNECTION_TIMEOUT = "60"; // In seconds
  private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds

  private static final String DRIVER_OPTS_KEY = "spark.driver.extraJavaOptions";
  private static final String EXECUTOR_OPTS_KEY = "spark.executor.extraJavaOptions";

  private final Map<String, String> conf;
  private final AtomicInteger childIdGenerator;
  private final Thread driverThread;
  private final Map<String, JobHandleImpl<?>> jobs;
  private final Rpc driverRpc;
  private final ClientProtocol protocol;
  private volatile boolean isAlive;

  SparkClientImpl(RpcServer rpcServer, Map<String, String> conf) throws IOException, SparkException {
    this.conf = conf;
    this.childIdGenerator = new AtomicInteger();
    this.jobs = Maps.newConcurrentMap();

    String secret = rpcServer.createSecret();
    this.driverThread = startDriver(rpcServer, secret);
    this.protocol = new ClientProtocol();

    try {
      // The RPC server will take care of timeouts here.
      this.driverRpc = rpcServer.registerClient(secret, protocol).get();
    } catch (Exception e) {
      LOG.warn("Error while waiting for client to connect.", e);
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
    return protocol.submit(job);
  }

  @Override
  public void stop() {
    if (isAlive) {
      isAlive = false;
      try {
        protocol.endSession().get(10, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        LOG.warn("Timed out waiting for driver to respond to stop request.");
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
      LOG.debug("Shut down time out.");
      driverThread.interrupt();
    }
  }

  @Override
  public Future<?> addJar(URL url) {
    return submit(new AddJarJob(url.toString()));
  }

  @Override
  public Future<?> addFile(URL url) {
    return submit(new AddFileJob(url.toString()));
  }

  @Override
  public Future<Integer> getExecutorCount() {
    return submit(new GetExecutorCountJob());
  }

  void cancel(String jobId) {
    protocol.cancel(jobId);
  }

  private Thread startDriver(RpcServer rpcServer, final String secret) throws IOException {
    Runnable runnable;
    final String serverAddress = rpcServer.getAddress();
    final String serverPort = String.valueOf(rpcServer.getPort());

    if (conf.containsKey(SparkClientFactory.CONF_KEY_IN_PROCESS)) {
      // Mostly for testing things quickly. Do not do this in production.
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      runnable = new Runnable() {
        @Override
        public void run() {
          List<String> args = Lists.newArrayList();
          args.add("--remote-host");
          args.add(serverAddress);
          args.add("--remote-port");
          args.add(serverPort);
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
      String sparkHome = conf.get("spark.home");
      if (sparkHome == null) {
        sparkHome = System.getProperty("spark.home");
      }
      String sparkLogDir = conf.get("hive.spark.log.dir");
      if (sparkLogDir == null) {
        if (sparkHome == null) {
          sparkLogDir = "./target/";
        } else {
          sparkLogDir = sparkHome + "/logs/";
        }
      }
      String driverJavaOpts = Joiner.on(" ").skipNulls().join(
          "-Dhive.spark.log.dir=" + sparkLogDir,conf.get(DRIVER_OPTS_KEY));
      String executorJavaOpts = Joiner.on(" ").skipNulls().join(
          "-Dhive.spark.log.dir=" + sparkLogDir, conf.get(EXECUTOR_OPTS_KEY));

      // Create a file with all the job properties to be read by spark-submit. Change the
      // file's permissions so that only the owner can read it. This avoid having the
      // connection secret show up in the child process's command line.
      File properties = File.createTempFile("spark-submit.", ".properties");
      if (!properties.setReadable(false) || !properties.setReadable(true, true)) {
        throw new IOException("Cannot change permissions of job properties file.");
      }

      Properties allProps = new Properties();
      for (Map.Entry<String, String> e : conf.entrySet()) {
        allProps.put(e.getKey(), conf.get(e.getKey()));
      }
      allProps.put(SparkClientFactory.CONF_KEY_SECRET, secret);
      allProps.put(DRIVER_OPTS_KEY, driverJavaOpts);
      allProps.put(EXECUTOR_OPTS_KEY, executorJavaOpts);

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

      List<String> argv = Lists.newArrayList();

      if (sparkHome != null) {
        argv.add(new File(sparkHome, "bin/spark-submit").getAbsolutePath());
      } else {
        LOG.info("No spark.home provided, calling SparkSubmit directly.");
        argv.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());

        if (master.startsWith("local") || master.startsWith("mesos") || master.endsWith("-client") || master.startsWith("spark")) {
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

      LOG.debug("Running client driver with argv: {}", Joiner.on(" ").join(argv));

      ProcessBuilder pb = new ProcessBuilder(argv.toArray(new String[argv.size()]));
      final Process child = pb.start();

      int childId = childIdGenerator.incrementAndGet();
      redirect("stdout-redir-" + childId, child.getInputStream());
      redirect("stderr-redir-" + childId, child.getErrorStream());

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              LOG.warn("Child process exited with code {}.", exitCode);
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

  private void redirect(String name, InputStream in) {
    Thread thread = new Thread(new Redirector(in));
    thread.setName(name);
    thread.setDaemon(true);
    thread.start();
  }

  private class ClientProtocol extends BaseProtocol {

    <T extends Serializable> JobHandleImpl<T> submit(Job<T> job) {
      final String jobId = UUID.randomUUID().toString();
      final Promise<T> promise = driverRpc.createPromise();
      JobHandleImpl<T> handle = new JobHandleImpl<T>(SparkClientImpl.this, promise, jobId);
      jobs.put(jobId, handle);

      final io.netty.util.concurrent.Future<Void> rpc = driverRpc.call(new JobRequest(jobId, job));

      // Link the RPC and the promise so that events from one are propagated to the other as
      // needed.
      rpc.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
        @Override
        public void operationComplete(io.netty.util.concurrent.Future<Void> f) {
          if (!f.isSuccess() && !promise.isDone()) {
            promise.setFailure(f.cause());
          }
        }
      });
      promise.addListener(new GenericFutureListener<Promise<T>>() {
        @Override
        public void operationComplete(Promise<T> p) {
          jobs.remove(jobId);
          if (p.isCancelled() && !rpc.isDone()) {
            rpc.cancel(true);
          }
        }
      });

      return handle;
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

    private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
      JobHandleImpl<?> handle = jobs.get(msg.clientJobId);
      if (handle != null) {
        LOG.info("Received spark job ID: {} for {}", msg.sparkJobId, msg.clientJobId);
        handle.getSparkJobIds().add(msg.sparkJobId);
      } else {
        LOG.warn("Received spark job ID: {} for unknown job {}", msg.sparkJobId, msg.clientJobId);
      }
    }

  }

  private class Redirector implements Runnable {

    private final BufferedReader in;

    Redirector(InputStream in) {
      this.in = new BufferedReader(new InputStreamReader(in));
    }

    @Override
    public void run() {
      try {
        String line = null;
        while ((line = in.readLine()) != null) {
          LOG.info(line);
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
        int count = jc.sc().sc().getExecutorMemoryStatus().size();
        return Integer.valueOf(count);
      }

  }

}
