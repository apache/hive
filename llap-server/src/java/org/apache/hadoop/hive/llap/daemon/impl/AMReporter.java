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

package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.CallableWithNdc;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends status updates to various AMs.
 */
public class AMReporter extends AbstractService {

    /*
  registrations and un-registrations will happen as and when tasks are submitted or are removed.
  reference counting is likely required.

  A connection needs to be established to each app master.

  Ignore exceptions when communicating with the AM.
  At a later point, report back saying the AM is dead so that tasks can be removed from the running queue.

  Use a cachedThreadPool so that a few AMs going down does not affect other AppMasters.

  Race: When a task completes - it sends out it's message via the regular TaskReporter. The AM after this may run another DAG,
  or may die. This may need to be consolidated with the LlapTaskReporter. Try ensuring there's no race between the two.

  Single thread which sends heartbeats to AppMasters as events drain off a queue.
   */

  private static final Logger LOG = LoggerFactory.getLogger(AMReporter.class);

  private final LlapNodeId nodeId;
  private final Configuration conf;
  private final ListeningExecutorService queueLookupExecutor;
  private final ListeningExecutorService executor;
  private final DelayQueue<AMNodeInfo> pendingHeartbeatQueeu = new DelayQueue();
  private final long heartbeatInterval;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final Map<LlapNodeId, AMNodeInfo> knownAppMasters = new HashMap<>();
  volatile ListenableFuture<Void> queueLookupFuture;

  public AMReporter(LlapNodeId nodeId, Configuration conf) {
    super(AMReporter.class.getName());
    this.nodeId = nodeId;
    this.conf = conf;
    ExecutorService rawExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AMReporter %d").build());
    this.executor = MoreExecutors.listeningDecorator(rawExecutor);
    ExecutorService rawExecutor2 = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AMReporterQueueDrainer").build());
    this.queueLookupExecutor = MoreExecutors.listeningDecorator(rawExecutor2);
    this.heartbeatInterval =
        conf.getLong(LlapConfiguration.LLAP_DAEMON_LIVENESS_HEARTBEAT_INTERVAL_MS,
            LlapConfiguration.LLAP_DAEMON_LIVENESS_HEARTBEAT_INTERVAL_MS_DEFAULT);
  }

  @Override
  public void serviceStart() {
    QueueLookupCallable queueDrainerCallable = new QueueLookupCallable();
    queueLookupFuture = queueLookupExecutor.submit(queueDrainerCallable);
    Futures.addCallback(queueLookupFuture, new FutureCallback<Void>() {
      @Override
      public void onSuccess(Void result) {
        LOG.info("AMReporter QueueDrainer exited");
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("AMReporter QueueDrainer exited with error", t);
      }
    });
    LOG.info("Started service: " + getName());
  }

  @Override
  public void serviceStop() {
    if (!isShutdown.getAndSet(true)) {
      if (queueLookupFuture != null) {
        queueLookupFuture.cancel(true);
      }
      queueLookupExecutor.shutdownNow();
      executor.shutdownNow();
      LOG.info("Stopped service: " + getName());
    }
  }


  public void registerTask(String amLocation, int port, String user, Token<JobTokenIdentifier> jobToken) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Registering for heartbeat: " + amLocation + ":" + port);
    }
    AMNodeInfo amNodeInfo;
    synchronized (knownAppMasters) {
      LlapNodeId amNodeId = LlapNodeId.getInstance(amLocation, port);
      amNodeInfo = knownAppMasters.get(amNodeId);
      if (amNodeInfo == null) {
        amNodeInfo = new AMNodeInfo(amNodeId, user, jobToken, conf);
        knownAppMasters.put(amNodeId, amNodeInfo);
        // Add to the queue only the first time this is registered, and on
        // subsequent instances when it's taken off the queue.
        amNodeInfo.setNextHeartbeatTime(System.currentTimeMillis() + heartbeatInterval);
        pendingHeartbeatQueeu.add(amNodeInfo);
      }
      amNodeInfo.incrementAndGetTaskCount();
    }
  }

  public void unregisterTask(String amLocation, int port) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Un-registering for heartbeat: " + amLocation + ":" + port);
    }
    AMNodeInfo amNodeInfo;
    LlapNodeId amNodeId = LlapNodeId.getInstance(amLocation, port);
    synchronized (knownAppMasters) {
      amNodeInfo = knownAppMasters.get(amNodeId);
      if (amNodeInfo == null) {
        LOG.error(("Ignoring unexpected unregisterRequest for am at: " + amLocation + ":" + port));
      }
      amNodeInfo.decrementAndGetTaskCount();
      // Not removing this here. Will be removed when taken off the queue and discovered to have 0
      // pending tasks.
    }
  }

  private class QueueLookupCallable extends CallableWithNdc<Void> {

    @Override
    protected Void callInternal() {
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        try {
          AMNodeInfo amNodeInfo = pendingHeartbeatQueeu.take();
          if (amNodeInfo.getTaskCount() == 0) {
            synchronized (knownAppMasters) {
              knownAppMasters.remove(amNodeInfo.amNodeId);
            }
            amNodeInfo.stopUmbilical();
          } else {
            // Add back to the queue for the next heartbeat, and schedule the actual heartbeat
            amNodeInfo.setNextHeartbeatTime(System.currentTimeMillis() + heartbeatInterval);
            pendingHeartbeatQueeu.add(amNodeInfo);
            executor.submit(new AMHeartbeatCallable(amNodeInfo));
          }
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info("QueueLookup thread interrupted after shutdown");
          } else {
            LOG.warn("Received unexpected interrupt while waiting on heartbeat queue");
          }
        }

      }
      return null;
    }
  }

  private class AMHeartbeatCallable extends CallableWithNdc<Void> {

    final AMNodeInfo amNodeInfo;

    public AMHeartbeatCallable(AMNodeInfo amNodeInfo) {
      this.amNodeInfo = amNodeInfo;
    }

    @Override
    protected Void callInternal() {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Attempting to heartbeat to AM: " + amNodeInfo);
      }
      if (amNodeInfo.getTaskCount() > 0) {
        try {
          if (LOG.isTraceEnabled()) {
            LOG.trace("NodeHeartbeat to: " + amNodeInfo);
          }
          amNodeInfo.getUmbilical().nodeHeartbeat(new Text(nodeId.getHostname()),
              nodeId.getPort());
        } catch (IOException e) {
          // TODO Ideally, this could be used to avoid running a task - AM down / unreachable, so there's no point running it.
          LOG.warn("Failed to communicate with AM. May retry later: " + amNodeInfo.amNodeId, e);
        } catch (InterruptedException e) {
          if (!isShutdown.get()) {
            LOG.warn("Failed to communicate with AM: " + amNodeInfo.amNodeId, e);
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping node heartbeat to AM: " + amNodeInfo + ", since ref count is 0");
        }
      }
      return null;
    }
  }



  private static class AMNodeInfo implements Delayed {
    private final AtomicInteger taskCount = new AtomicInteger(0);
    private final String user;
    private final Token<JobTokenIdentifier> jobToken;
    private final Configuration conf;
    private final LlapNodeId amNodeId;
    private LlapTaskUmbilicalProtocol umbilical;
    private long nextHeartbeatTime;


    public AMNodeInfo(LlapNodeId amNodeId, String user,
                      Token<JobTokenIdentifier> jobToken,
                      Configuration conf) {
      this.user = user;
      this.jobToken = jobToken;
      this.conf = conf;
      this.amNodeId = amNodeId;
    }

    synchronized LlapTaskUmbilicalProtocol getUmbilical() throws IOException, InterruptedException {
      if (umbilical == null) {
        final InetSocketAddress address =
            NetUtils.createSocketAddrForHost(amNodeId.getHostname(), amNodeId.getPort());
        SecurityUtil.setTokenService(this.jobToken, address);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        ugi.addToken(jobToken);
        umbilical = ugi.doAs(new PrivilegedExceptionAction<LlapTaskUmbilicalProtocol>() {
          @Override
          public LlapTaskUmbilicalProtocol run() throws Exception {
            return RPC.getProxy(LlapTaskUmbilicalProtocol.class,
                LlapTaskUmbilicalProtocol.versionID, address, conf);
          }
        });
      }
      return umbilical;
    }

    synchronized void stopUmbilical() {
      if (umbilical != null) {
        RPC.stopProxy(umbilical);
      }
      umbilical = null;
    }

    int incrementAndGetTaskCount() {
      return taskCount.incrementAndGet();
    }

    int decrementAndGetTaskCount() {
      return taskCount.decrementAndGet();
    }

    int getTaskCount() {
      return taskCount.get();
    }

    synchronized void setNextHeartbeatTime(long nextTime) {
      nextHeartbeatTime = nextTime;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed o) {
      AMNodeInfo other = (AMNodeInfo)o;
      if (this.nextHeartbeatTime > other.nextHeartbeatTime) {
        return 1;
      } else if (this.nextHeartbeatTime < other.nextHeartbeatTime) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public String toString() {
      return "AMInfo: " + amNodeId + ", taskCount=" + getTaskCount();
    }
  }
}
