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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkClientImpl implements SparkClient {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SparkClientImpl.class);
  
  private static final String DEFAULT_CONNECTION_TIMEOUT = "60"; // In seconds

  private final Map<String, String> conf;
  private final AtomicInteger childIdGenerator;
  private final String name;
  private final ActorRef clientRef;
  private final Thread driverThread;
  private final Map<String, JobHandleImpl<?>> jobs;

  private volatile ActorSelection remoteRef;

  SparkClientImpl(Map<String, String> conf) throws IOException, SparkException {
    this.conf = conf;
    this.childIdGenerator = new AtomicInteger();
    this.name = "SparkClient-" + ClientUtils.randomName();
    this.clientRef = bind(Props.create(ClientActor.class, this), name);
    this.jobs = Maps.newConcurrentMap();
    this.driverThread = startDriver();

    long connectTimeout = 1000 * Integer.parseInt(
        Optional.fromNullable(conf.get("spark.client.connectTimeout")).or(DEFAULT_CONNECTION_TIMEOUT));
    long endTime = System.currentTimeMillis() + connectTimeout;

    synchronized (this) {
      while (remoteRef == null) {
        try {
          wait(connectTimeout);
        } catch (InterruptedException ie) {
          throw new SparkException("Interrupted.", ie);
        }

        connectTimeout = endTime - System.currentTimeMillis();
        if (remoteRef == null && connectTimeout <= 0) {
          throw new SparkException("Timed out waiting for remote driver to connect.");
        }
      }
    }
  }

  @Override
  public <T extends Serializable> JobHandle<T> submit(Job<T> job) {
    String jobId = ClientUtils.randomName();
    remoteRef.tell(new Protocol.JobRequest(jobId, job), clientRef);

    JobHandleImpl<T> handle = new JobHandleImpl<T>(this, jobId);
    jobs.put(jobId, handle);
    return handle;
  }

  @Override
  public void stop() {
    if (remoteRef != null) {
      LOG.info("Sending EndSession to remote actor.");
      remoteRef.tell(new Protocol.EndSession(), clientRef);
    }
    unbind(clientRef);
    try {
      driverThread.join(); // TODO: timeout?
    } catch (InterruptedException ie) {
      LOG.debug("Interrupted before driver thread was finished.");
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
    remoteRef.tell(new Protocol.CancelJob(jobId), clientRef);
  }

  private Thread startDriver() throws IOException {
    Runnable runnable;
    if (conf.containsKey(ClientUtils.CONF_KEY_IN_PROCESS)) {
      // Mostly for testing things quickly. Do not do this in production.
      LOG.warn("!!!! Running remote driver in-process. !!!!");
      runnable = new Runnable() {
        @Override
        public void run() {
          List<String> args = Lists.newArrayList();
          args.add("--remote");
          args.add(String.format("%s/%s", SparkClientFactory.akkaUrl, name));
          args.add("--secret");
          args.add(SparkClientFactory.secret);

          for (Map.Entry<String, String> e : conf.entrySet()) {
            args.add("--conf");
            args.add(String.format("%s=%s", e.getKey(), e.getValue()));
          }
          try {
            RemoteDriver.main(args.toArray(new String[args.size()]));
          } catch (Exception e) {
            LOG.error("Error running driver.", e);
          }
        }
      };
    } else {
      // Create a file with all the job properties to be read by spark-submit. Change the
      // file's permissions so that only the owner can read it. This avoid having the
      // connection secret show up in the child process's command line.
      File properties = File.createTempFile("spark-submit.", ".properties");
      if (!properties.setReadable(false) || !properties.setReadable(true, true)) {
        throw new IOException("Cannot change permissions of job properties file.");
      }

      Properties allProps = new Properties();
      for (Map.Entry<String, String> e : conf.entrySet()) {
        allProps.put(e.getKey(), e.getValue());
      }
      allProps.put(ClientUtils.CONF_KEY_SECRET, SparkClientFactory.secret);

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

      // If a Spark installation is provided, use the spark-submit script. Otherwise, call the
      // SparkSubmit class directly, which has some caveats (like having to provide a proper
      // version of Guava on the classpath depending on the deploy mode).
      if (conf.get("spark.home") != null) {
        argv.add(new File(conf.get("spark.home"), "bin/spark-submit").getAbsolutePath());
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

          String extra = conf.get("spark.driver.extraJavaOptions");
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


      argv.add("--remote");
      argv.add(String.format("%s/%s", SparkClientFactory.akkaUrl, name));

      LOG.debug("Running client driver with argv: {}", Joiner.on(" ").join(argv));

      ProcessBuilder pb = new ProcessBuilder(argv.toArray(new String[argv.size()]));
      pb.environment().clear();
      final Process child = pb.start();

      int childId = childIdGenerator.incrementAndGet();
      redirect("stdout-redir-" + childId, child.getInputStream(), System.out);
      redirect("stderr-redir-" + childId, child.getErrorStream(), System.err);

      runnable = new Runnable() {
        @Override
        public void run() {
          try {
            int exitCode = child.waitFor();
            if (exitCode != 0) {
              LOG.warn("Child process exited with code {}.", exitCode);
            }
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

  private void redirect(String name, InputStream in, OutputStream out) {
    Thread thread = new Thread(new Redirector(in, out));
    thread.setName(name);
    thread.setDaemon(true);
    thread.start();
  }

  private ActorRef bind(Props props, String name) {
    return SparkClientFactory.actorSystem.actorOf(props, name);
  }

  private void unbind(ActorRef actor) {
    SparkClientFactory.actorSystem.stop(actor);
  }

  private ActorSelection select(String url) {
    return SparkClientFactory.actorSystem.actorSelection(url);
  }

  private class ClientActor extends UntypedActor {

    @Override
    public void onReceive(Object message) throws Exception {
      if (message instanceof Protocol.Error) {
        Protocol.Error e = (Protocol.Error) message;
        LOG.error("Error report from remote driver.", e.cause);
      } else if (message instanceof Protocol.Hello) {
        Protocol.Hello hello = (Protocol.Hello) message;
        LOG.info("Received hello from {}", hello.remoteUrl);
        remoteRef = select(hello.remoteUrl);
        synchronized (SparkClientImpl.this) {
          SparkClientImpl.this.notifyAll();
        }
      } else if (message instanceof Protocol.JobMetrics) {
        Protocol.JobMetrics jm = (Protocol.JobMetrics) message;
        JobHandleImpl<?> handle = jobs.get(jm.jobId);
        if (handle != null) {
          handle.getMetrics().addMetrics(jm.sparkJobId, jm.stageId, jm.taskId, jm.metrics);
        } else {
          LOG.warn("Received metrics for unknown job {}", jm.jobId);
        }
      } else if (message instanceof Protocol.JobResult) {
        Protocol.JobResult jr = (Protocol.JobResult) message;
        JobHandleImpl<?> handle = jobs.remove(jr.id);
        if (handle != null) {
          LOG.info("Received result for {}", jr.id);
          handle.setSparkCounters(jr.sparkCounters);
          handle.complete(jr.result, jr.error);
        } else {
          LOG.warn("Received result for unknown job {}", jr.id);
        }
      } else if (message instanceof Protocol.JobSubmitted) {
        Protocol.JobSubmitted jobSubmitted = (Protocol.JobSubmitted) message;
        JobHandleImpl<?> handle = jobs.get(jobSubmitted.clientJobId);
        if (handle != null) {
          LOG.info("Received spark job ID: {} for {}",
              jobSubmitted.sparkJobId, jobSubmitted.clientJobId);
          handle.getSparkJobIds().add(jobSubmitted.sparkJobId);
        } else {
          LOG.warn("Received spark job ID: {} for unknown job {}",
              jobSubmitted.sparkJobId, jobSubmitted.clientJobId);
        }
      }
    }

  }

  private class Redirector implements Runnable {

    private final InputStream in;
    private final OutputStream out;

    Redirector(InputStream in, OutputStream out) {
      this.in = in;
      this.out = out;
    }

    @Override
    public void run() {
      try {
        byte[] buf = new byte[1024];
        int len = in.read(buf);
        while (len != -1) {
          out.write(buf, 0, len);
          out.flush();
          len = in.read(buf);
        }
      } catch (Exception e) {
        LOG.warn("Error in redirector thread.", e);
      }
    }

  }

  private static class AddJarJob implements Job<Serializable> {
    private static final long serialVersionUID = 1L;

    private final String path;

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
