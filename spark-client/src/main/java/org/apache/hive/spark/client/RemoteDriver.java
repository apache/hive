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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple2;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.remote.DisassociatedEvent;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.*;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.spark.client.metrics.Metrics;

/**
 * Driver code for the Spark client library.
 */
public class RemoteDriver {

  private final static Logger LOG = LoggerFactory.getLogger(RemoteDriver.class);

  private final Map<String, JobWrapper<?>> activeJobs;
  private final Object shutdownLock;
  private final ActorSystem system;
  private final ActorRef actor;
  private final ActorSelection client;
  private final ExecutorService executor;
  private final JobContextImpl jc;

  private boolean running;

  private RemoteDriver(String[] args) throws Exception {
    this.activeJobs = Maps.newConcurrentMap();
    this.shutdownLock = new Object();

    SparkConf conf = new SparkConf();
    String remote = null;
    for (int idx = 0; idx < args.length; idx += 2) {
      String key = args[idx];
      if (key.equals("--remote")) {
        remote = getArg(args, idx);
      } else if (key.equals("--secret")) {
        conf.set(ClientUtils.CONF_KEY_SECRET, getArg(args, idx));
      } else if (key.equals("--conf")) {
        String[] val = getArg(args, idx).split("[=]", 2);
        conf.set(val[0], val[1]);
      } else {
        throw new IllegalArgumentException("Invalid command line: " +
            Joiner.on(" ").join(args));
      }
    }

    executor = Executors.newCachedThreadPool();

    LOG.info("Connecting to: {}", remote);

    Map<String, String> mapConf = Maps.newHashMap();
    for (Tuple2<String, String> e : conf.getAll()) {
      mapConf.put(e._1(), e._2());
    }

    ClientUtils.ActorSystemInfo info = ClientUtils.createActorSystem(mapConf);
    this.system = info.system;
    this.actor = system.actorOf(Props.create(ServerActor.class, this), "RemoteDriver");
    this.client = system.actorSelection(remote);

    try {
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.sc().addSparkListener(new ClientListener());
      jc = new JobContextImpl(sc);
    } catch (Exception e) {
      LOG.error("Failed to start SparkContext.", e);
      shutdown(new Protocol.Error(e));
      throw e;
    }

    client.tell(new Protocol.Hello(info.url + "/RemoteDriver"), actor);
    running = true;
  }

  private void run() throws InterruptedException {
    synchronized (shutdownLock) {
      while (running) {
        shutdownLock.wait();
      }
    }
    executor.shutdownNow();
  }

  private synchronized void shutdown(Object msg) {
    if (running) {
      LOG.info("Shutting down remote driver.");
      running = false;
      for (JobWrapper<?> job : activeJobs.values()) {
        cancelJob(job);
      }

      if (msg != null) {
        client.tell(msg, actor);
      }
      if (jc != null) {
        jc.stop();
      }
      system.shutdown();
      synchronized (shutdownLock) {
        shutdownLock.notifyAll();
      }
    }
  }

  private boolean cancelJob(JobWrapper<?> job) {
    boolean cancelled = false;
    for (JavaFutureAction<?> action : job.jobs) {
      cancelled |= action.cancel(true);
    }
    return cancelled | job.future.cancel(true);
  }

  private String getArg(String[] args, int keyIdx) {
    int valIdx = keyIdx + 1;
    if (args.length <= valIdx) {
      throw new IllegalArgumentException("Invalid command line: " +
          Joiner.on(" ").join(args));
    }
    return args[valIdx];
  }

  private class ServerActor extends UntypedActor {

    @Override
    public void onReceive(Object message) throws Exception {
      if (message instanceof Protocol.CancelJob) {
        Protocol.CancelJob cj = (Protocol.CancelJob) message;
        JobWrapper<?> job = activeJobs.get(cj.id);
        if (job == null || !cancelJob(job)) {
          LOG.info("Requested to cancel an already finished job.");
        }
      } else if (message instanceof DisassociatedEvent) {
        LOG.debug("Shutting down due to DisassociatedEvent.");
        shutdown(null);
      } else if (message instanceof Protocol.EndSession) {
        LOG.debug("Shutting down due to EndSession request.");
        shutdown(null);
      } else if (message instanceof Protocol.JobRequest) {
        Protocol.JobRequest req = (Protocol.JobRequest) message;
        LOG.info("Received job request {}", req.id);
        JobWrapper<?> wrapper = new JobWrapper<Serializable>(req);
        activeJobs.put(req.id, wrapper);
        wrapper.submit();
      }
    }

  }

  private class JobWrapper<T extends Serializable> implements Callable<Void> {

    private final Protocol.JobRequest<T> req;
    private final List<JavaFutureAction<?>> jobs;
    private final AtomicInteger completed;

    private Future<?> future;

    JobWrapper(Protocol.JobRequest<T> req) {
      this.req = req;
      this.jobs = Lists.newArrayList();
      this.completed = new AtomicInteger();
    }

    @Override
    public Void call() throws Exception {
      try {
        jc.setMonitorCb(new MonitorCallback() {
          @Override
          public void call(JavaFutureAction<?> future) {
            monitorJob(future);
          }
        });

        T result = req.job.call(jc);
        synchronized (completed) {
          while (completed.get() != jobs.size()) {
            LOG.debug("Client job {} finished, {} of {} Spark jobs finished.",
                req.id, completed.get(), jobs.size());
            completed.wait();
          }
        }
        client.tell(new Protocol.JobResult(req.id, result, null), actor);
      } catch (Throwable t) {
          // Catch throwables in a best-effort to report job status back to the client. It's
          // re-thrown so that the executor can destroy the affected thread (or the JVM can
          // die or whatever would happen if the throwable bubbled up).
          client.tell(new Protocol.JobResult(req.id, null, t), actor);
          throw new ExecutionException(t);
      } finally {
        jc.setMonitorCb(null);
        activeJobs.remove(req.id);
      }
      return null;
    }

    void submit() {
      this.future = executor.submit(this);
    }

    void jobDone() {
      synchronized (completed) {
        completed.incrementAndGet();
        completed.notifyAll();
      }
    }

    private void monitorJob(JavaFutureAction<?> job) {
      jobs.add(job);
    }

  }

  private class ClientListener implements SparkListener {

    private final Map<Integer, Integer> stageToJobId = Maps.newHashMap();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
      // TODO: are stage IDs unique? Otherwise this won't work.
      synchronized (stageToJobId) {
        for (int i = 0; i < jobStart.stageIds().length(); i++) {
          stageToJobId.put((Integer) jobStart.stageIds().apply(i), jobStart.jobId());
        }
      }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
      synchronized (stageToJobId) {
        for (Iterator<Map.Entry<Integer, Integer>> it = stageToJobId.entrySet().iterator();
            it.hasNext(); ) {
          Map.Entry<Integer, Integer> e = it.next();
          if (e.getValue() == jobEnd.jobId()) {
            it.remove();
          }
        }
      }

      String clientId = getClientId(jobEnd.jobId());
      if (clientId != null) {
        activeJobs.get(clientId).jobDone();
      }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.reason() instanceof org.apache.spark.Success$ &&
          !taskEnd.taskInfo().speculative()) {
        Metrics metrics = new Metrics(taskEnd.taskMetrics());
        Integer jobId;
        synchronized (stageToJobId) {
          jobId = stageToJobId.get(taskEnd.stageId());
        }

        // TODO: implement implicit AsyncRDDActions conversion instead of jc.monitor()?
        // TODO: how to handle stage failures?

        String clientId = getClientId(jobId);
        if (clientId != null) {
          client.tell(new Protocol.JobMetrics(clientId, jobId, taskEnd.stageId(),
              taskEnd.taskInfo().taskId(), metrics), actor);
        }
      }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) { }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) { }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) { }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) { }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) { }

    @Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) { }

    @Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) { }

    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) { }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) { }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) { }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) { }

    /**
     * Returns the client job ID for the given Spark job ID.
     *
     * This will only work for jobs monitored via JobContext#monitor(). Other jobs won't be
     * matched, and this method will return `None`.
     */
    private String getClientId(Integer jobId) {
      for (Map.Entry<String, JobWrapper<?>> e : activeJobs.entrySet()) {
        for (JavaFutureAction<?> future : e.getValue().jobs) {
          if (future.jobIds().contains(jobId)) {
            return e.getKey();
          }
        }
      }
      return null;
    }

  }

  public static void main(String[] args) throws Exception {
    new RemoteDriver(args).run();
  }

}

