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

import javax.net.SocketFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.QueryFailedHandler;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for communicating with various AMs.
 */
public class AMReporter extends AbstractService {

  // TODO In case of a failure to heartbeat, tasks for the specific DAG should ideally be KILLED

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

  private volatile LlapNodeId nodeId;
  private final QueryFailedHandler queryFailedHandler;
  private final Configuration conf;
  private final ListeningExecutorService queueLookupExecutor;
  private final ListeningExecutorService executor;
  private final RetryPolicy retryPolicy;
  private final long retryTimeout;
  private final SocketFactory socketFactory;
  private final DelayQueue<AMNodeInfo> pendingHeartbeatQueeu = new DelayQueue<>();
  private final AtomicReference<InetSocketAddress> localAddress;
  private final long heartbeatInterval;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  // Tracks appMasters to which heartbeats are being sent. This should not be used for any other
  // messages like taskKilled, etc.
  private final Map<LlapNodeId, AMNodeInfo> knownAppMasters = new HashMap<>();
  volatile ListenableFuture<Void> queueLookupFuture;

  public AMReporter(AtomicReference<InetSocketAddress> localAddress,
                    QueryFailedHandler queryFailedHandler, Configuration conf) {
    super(AMReporter.class.getName());
    this.localAddress = localAddress;
    this.queryFailedHandler = queryFailedHandler;
    this.conf = conf;
    ExecutorService rawExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AMReporter %d").build());
    this.executor = MoreExecutors.listeningDecorator(rawExecutor);
    ExecutorService rawExecutor2 = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AMReporterQueueDrainer").build());
    this.queueLookupExecutor = MoreExecutors.listeningDecorator(rawExecutor2);
    this.heartbeatInterval = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_DAEMON_AM_LIVENESS_HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

    this.retryTimeout = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    long retrySleep = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_SLEEP_BETWEEN_RETRIES_MS,
        TimeUnit.MILLISECONDS);
    this.retryPolicy = RetryPolicies
        .retryUpToMaximumTimeWithFixedSleep(retryTimeout, retrySleep,
            TimeUnit.MILLISECONDS);

    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);

    LOG.info("Setting up AMReporter with " +
        "heartbeatInterval(ms)=" + heartbeatInterval +
        ", retryTime(ms)=" + retryTimeout +
        ", retrySleep(ms)=" + retrySleep);
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
        if (t instanceof CancellationException && isShutdown.get()) {
          LOG.info("AMReporter QueueDrainer exited as a result of a cancellation after shutdown");
        } else {
          LOG.error("AMReporter QueueDrainer exited with error", t);
          Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
        }
      }
    });
    nodeId = LlapNodeId.getInstance(localAddress.get().getHostName(), localAddress.get().getPort());
    LOG.info("AMReporter running with NodeId: {}", nodeId);
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

  public void registerTask(String amLocation, int port, String user,
                           Token<JobTokenIdentifier> jobToken, QueryIdentifier queryIdentifier) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Registering for heartbeat: " + amLocation + ":" + port + " for queryIdentifier=" + queryIdentifier);
    }
    AMNodeInfo amNodeInfo;
    synchronized (knownAppMasters) {
      LlapNodeId amNodeId = LlapNodeId.getInstance(amLocation, port);
      amNodeInfo = knownAppMasters.get(amNodeId);
      if (amNodeInfo == null) {
        amNodeInfo =
            new AMNodeInfo(amNodeId, user, jobToken, queryIdentifier, retryPolicy, retryTimeout, socketFactory,
                conf);
        knownAppMasters.put(amNodeId, amNodeInfo);
        // Add to the queue only the first time this is registered, and on
        // subsequent instances when it's taken off the queue.
        amNodeInfo.setNextHeartbeatTime(System.currentTimeMillis() + heartbeatInterval);
        pendingHeartbeatQueeu.add(amNodeInfo);
      }
      amNodeInfo.setCurrentQueryIdentifier(queryIdentifier);
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
        LOG.info(("Ignoring duplicate unregisterRequest for am at: " + amLocation + ":" + port));
      } else {
        amNodeInfo.decrementAndGetTaskCount();
      }
      // Not removing this here. Will be removed when taken off the queue and discovered to have 0
      // pending tasks.
    }
  }

  public void taskKilled(String amLocation, int port, String user, Token<JobTokenIdentifier> jobToken,
                         final QueryIdentifier queryIdentifier, final TezTaskAttemptID taskAttemptId) {
    // Not re-using the connection for the AM heartbeat - which may or may not be open by this point.
    // knownAppMasters is used for sending heartbeats for queued tasks. Killed messages use a new connection.
    LlapNodeId amNodeId = LlapNodeId.getInstance(amLocation, port);
    AMNodeInfo amNodeInfo =
        new AMNodeInfo(amNodeId, user, jobToken, queryIdentifier, retryPolicy, retryTimeout, socketFactory,
            conf);

    // Even if the service hasn't started up. It's OK to make this invocation since this will
    // only happen after the AtomicReference address has been populated. Not adding an additional check.
    ListenableFuture<Void> future =
        executor.submit(new KillTaskCallable(taskAttemptId, amNodeInfo));
    Futures.addCallback(future, new FutureCallback<Void>() {
      @Override
      public void onSuccess(Void result) {
        LOG.info("Sent taskKilled for {}", taskAttemptId);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Failed to send taskKilled for {}. The attempt will likely time out.",
            taskAttemptId);
      }
    });
  }

  private class QueueLookupCallable extends CallableWithNdc<Void> {

    @Override
    protected Void callInternal() {
      while (!isShutdown.get() && !Thread.currentThread().isInterrupted()) {
        try {
          final AMNodeInfo amNodeInfo = pendingHeartbeatQueeu.take();
          if (amNodeInfo.getTaskCount() == 0 || amNodeInfo.hasAmFailed()) {
            synchronized (knownAppMasters) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Removing am {} with last associated dag {} from heartbeat with taskCount={}, amFailed={}",
                    amNodeInfo.amNodeId, amNodeInfo.getCurrentQueryIdentifier(), amNodeInfo.getTaskCount(),
                    amNodeInfo.hasAmFailed(), amNodeInfo);
              }
              knownAppMasters.remove(amNodeInfo.amNodeId);
            }
            amNodeInfo.stopUmbilical();
          } else {
            // Add back to the queue for the next heartbeat, and schedule the actual heartbeat
            long next = System.currentTimeMillis() + heartbeatInterval;
            amNodeInfo.setNextHeartbeatTime(next);
            pendingHeartbeatQueeu.add(amNodeInfo);
            ListenableFuture<Void> future = executor.submit(new AMHeartbeatCallable(amNodeInfo));
            Futures.addCallback(future, new FutureCallback<Void>() {
              @Override
              public void onSuccess(Void result) {
                // Nothing to do.
              }

              @Override
              public void onFailure(Throwable t) {
                QueryIdentifier currentQueryIdentifier = amNodeInfo.getCurrentQueryIdentifier();
                amNodeInfo.setAmFailed(true);
                LOG.warn("Heartbeat failed to AM {}. Killing all other tasks for the query={}",
                    amNodeInfo.amNodeId, currentQueryIdentifier, t);
                queryFailedHandler.queryFailed(currentQueryIdentifier);
              }
            });
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

  private class KillTaskCallable extends CallableWithNdc<Void> {
    final AMNodeInfo amNodeInfo;
    final TezTaskAttemptID taskAttemptId;

    public KillTaskCallable(TezTaskAttemptID taskAttemptId,
                            AMNodeInfo amNodeInfo) {
      this.taskAttemptId = taskAttemptId;
      this.amNodeInfo = amNodeInfo;
    }

    @Override
    protected Void callInternal() {
      try {
        amNodeInfo.getUmbilical().taskKilled(taskAttemptId);
      } catch (IOException e) {
        LOG.warn("Failed to send taskKilled message for task {}. Will re-run after it times out", taskAttemptId);
      } catch (InterruptedException e) {
        if (!isShutdown.get()) {
          LOG.info("Interrupted while trying to send taskKilled message for task {}", taskAttemptId);
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
          QueryIdentifier currentQueryIdentifier = amNodeInfo.getCurrentQueryIdentifier();
          amNodeInfo.setAmFailed(true);
          LOG.warn("Failed to communicated with AM at {}. Killing remaining fragments for query {}",
              amNodeInfo.amNodeId, currentQueryIdentifier, e);
          queryFailedHandler.queryFailed(currentQueryIdentifier);
        } catch (InterruptedException e) {
          if (!isShutdown.get()) {
            LOG.warn("Interrupted while trying to send heartbeat to AM {}", amNodeInfo.amNodeId, e);
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
    private final RetryPolicy retryPolicy;
    private final long timeout;
    private final SocketFactory socketFactory;
    private final AtomicBoolean amFailed = new AtomicBoolean(false);
    private QueryIdentifier currentQueryIdentifier;
    private LlapTaskUmbilicalProtocol umbilical;
    private long nextHeartbeatTime;


    public AMNodeInfo(LlapNodeId amNodeId, String user,
                      Token<JobTokenIdentifier> jobToken,
                      QueryIdentifier currentQueryIdentifier,
                      RetryPolicy retryPolicy,
                      long timeout,
                      SocketFactory socketFactory,
                      Configuration conf) {
      this.user = user;
      this.jobToken = jobToken;
      this.currentQueryIdentifier = currentQueryIdentifier;
      this.retryPolicy = retryPolicy;
      this.timeout = timeout;
      this.socketFactory = socketFactory;
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
            return RPC
                .getProxy(LlapTaskUmbilicalProtocol.class, LlapTaskUmbilicalProtocol.versionID,
                    address, UserGroupInformation.getCurrentUser(), conf, socketFactory,
                    (int) timeout);
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

    void setAmFailed(boolean val) {
      amFailed.set(val);
    }

    boolean hasAmFailed() {
      return amFailed.get();
    }

    int getTaskCount() {
      return taskCount.get();
    }

    public synchronized QueryIdentifier getCurrentQueryIdentifier() {
      return currentQueryIdentifier;
    }

    public synchronized void setCurrentQueryIdentifier(QueryIdentifier queryIdentifier) {
      this.currentQueryIdentifier = queryIdentifier;
    }

    synchronized void setNextHeartbeatTime(long nextTime) {
      nextHeartbeatTime = nextTime;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(nextHeartbeatTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
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
