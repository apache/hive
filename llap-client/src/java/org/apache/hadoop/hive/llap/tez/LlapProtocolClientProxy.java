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

package org.apache.hadoop.hive.llap.tez;

import javax.net.SocketFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB;
import org.apache.hadoop.hive.llap.impl.LlapProtocolClientImpl;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapProtocolClientProxy extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapProtocolClientProxy.class);

  private final ConcurrentMap<String, LlapProtocolBlockingPB> hostProxies;

  private final RequestManager requestManager;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;

  private final ListeningExecutorService requestManagerExecutor;
  private volatile ListenableFuture<Void> requestManagerFuture;
  private final Token<LlapTokenIdentifier> llapToken;
  private final String llapTokenUser;

  public LlapProtocolClientProxy(
      int numThreads, Configuration conf, Token<LlapTokenIdentifier> llapToken) {
    super(LlapProtocolClientProxy.class.getSimpleName());
    this.hostProxies = new ConcurrentHashMap<>();
    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
    this.llapToken = llapToken;
    if (llapToken != null) {
      try {
        llapTokenUser = llapToken.decodeIdentifier().getOwner().toString();
      } catch (IOException e) {
        throw new RuntimeException("Cannot determine the user from token " + llapToken, e);
      }
    } else {
      llapTokenUser = null;
    }

    long connectionTimeout = HiveConf.getTimeVar(conf,
        ConfVars.LLAP_TASK_COMMUNICATOR_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    long retrySleep = HiveConf.getTimeVar(conf,
        ConfVars.LLAP_TASK_COMMUNICATOR_CONNECTION_SLEEP_BETWEEN_RETRIES_MS,
        TimeUnit.MILLISECONDS);
    this.retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(
        connectionTimeout, retrySleep, TimeUnit.MILLISECONDS);

    this.requestManager = new RequestManager(numThreads);
    ExecutorService localExecutor = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("RequestManagerExecutor").build());
    this.requestManagerExecutor = MoreExecutors.listeningDecorator(localExecutor);

    LOG.info("Setting up taskCommunicator with" +
        "numThreads=" + numThreads +
        "retryTime(millis)=" + connectionTimeout +
        "retrySleep(millis)=" + retrySleep);
  }

  @Override
  public void serviceStart() {
    requestManagerFuture = requestManagerExecutor.submit(requestManager);
    Futures.addCallback(requestManagerFuture, new FutureCallback<Void>() {
      @Override
      public void onSuccess(Void result) {
        LOG.info("RequestManager shutdown");
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("RequestManager shutdown with error", t);
      }
    });
  }

  @Override
  public void serviceStop() {
    if (requestManagerFuture != null) {
      requestManager.shutdown();
      requestManagerFuture.cancel(true);
    }
    requestManagerExecutor.shutdown();
  }

  public void sendSubmitWork(SubmitWorkRequestProto request, String host, int port,
                         final ExecuteRequestCallback<SubmitWorkResponseProto> callback) {
    LlapNodeId nodeId = LlapNodeId.getInstance(host, port);
    requestManager.queueRequest(new SubmitWorkCallable(nodeId, request, callback));
  }

  public void sendSourceStateUpdate(final SourceStateUpdatedRequestProto request, final LlapNodeId nodeId,
                                    final ExecuteRequestCallback<SourceStateUpdatedResponseProto> callback) {
    requestManager.queueRequest(
        new SendSourceStateUpdateCallable(nodeId, request, callback));
  }

  public void sendQueryComplete(final QueryCompleteRequestProto request, final String host,
                                final int port,
                                final ExecuteRequestCallback<QueryCompleteResponseProto> callback) {
    LlapNodeId nodeId = LlapNodeId.getInstance(host, port);
    requestManager.queueRequest(new SendQueryCompleteCallable(nodeId, request, callback));
  }

  public void sendTerminateFragment(final TerminateFragmentRequestProto request, final String host,
                                    final int port,
                                    final ExecuteRequestCallback<TerminateFragmentResponseProto> callback) {
    LlapNodeId nodeId = LlapNodeId.getInstance(host, port);
    requestManager.queueRequest(new SendTerminateFragmentCallable(nodeId, request, callback));
  }

  @VisibleForTesting
  static class RequestManager implements Callable<Void> {

    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final Condition queueCondition = lock.newCondition();
    private final AtomicBoolean shouldRun = new AtomicBoolean(false);

    private final int maxConcurrentRequestsPerNode = 1;
    private final ListeningExecutorService executor;


    // Tracks new additions via add, while the loop is processing existing ones.
    private final LinkedList<CallableRequest> newRequestList = new LinkedList<>();

    // Tracks existing requests which are cycled through.
    private final LinkedList<CallableRequest> pendingRequests = new LinkedList<>();

    // Tracks requests executing per node
    private final ConcurrentMap<LlapNodeId, AtomicInteger> runningRequests = new ConcurrentHashMap<>();

    // Tracks completed requests pre node
    private final LinkedList<LlapNodeId> completedNodes = new LinkedList<>();

    public RequestManager(int numThreads) {
      ExecutorService localExecutor = Executors.newFixedThreadPool(numThreads,
          new ThreadFactoryBuilder().setNameFormat("TaskCommunicator #%2d").build());
      executor = MoreExecutors.listeningDecorator(localExecutor);
    }


    @VisibleForTesting
    Set<LlapNodeId> currentLoopDisabledNodes = new HashSet<>();
    @VisibleForTesting
    List<CallableRequest> currentLoopSkippedRequests = new LinkedList<>();
    @Override
    public Void call() {
      // Caches disabled nodes for quicker lookups and ensures a request on a node which was skipped
      // does not go out of order.
      while (!isShutdown.get()) {
        lock.lock();
        try {
          while (!shouldRun.get()) {
            queueCondition.await();
            break; // Break out and try executing.
          }
          boolean shouldBreak = process();
          if (shouldBreak) {
            break;
          }
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            break;
          } else {
            LOG.warn("RunLoop interrupted without being shutdown first");
            throw new RuntimeException(e);
          }
        } finally {
          lock.unlock();
        }
      }
      LOG.info("CallScheduler loop exiting");
      return null;
    }

    /* Add a new request to be executed */
    public void queueRequest(CallableRequest request) {
      synchronized (newRequestList) {
        newRequestList.add(request);
        shouldRun.set(true);
      }
      notifyRunLoop();
    }

    /* Indicates a request has completed on a node */
    public void requestFinished(LlapNodeId nodeId) {
      synchronized (completedNodes) {
        completedNodes.add(nodeId);
        shouldRun.set(true);
      }
      notifyRunLoop();
    }

    public void shutdown() {
      if (!isShutdown.getAndSet(true)) {
        executor.shutdownNow();
        notifyRunLoop();
      }
    }

    @VisibleForTesting
    void submitToExecutor(CallableRequest request, LlapNodeId nodeId) {
      ListenableFuture<SourceStateUpdatedResponseProto> future =
          executor.submit(request);
      Futures.addCallback(future, new ResponseCallback(request.getCallback(), nodeId, this));
    }

    @VisibleForTesting
    boolean process() {
      if (isShutdown.get()) {
        return true;
      }
      currentLoopDisabledNodes.clear();
      currentLoopSkippedRequests.clear();

      // Set to false to block the next loop. This must be called before draining the lists,
      // otherwise an add/completion after draining the lists but before setting it to false,
      // will not trigger a run. May cause one unnecessary run if an add comes in before drain.
      // drain list. add request (setTrue). setFalse needs to be avoided.
      shouldRun.compareAndSet(true, false);
      // Drain any calls which may have come in during the last execution of the loop.
      drainNewRequestList();  // Locks newRequestList
      drainCompletedNodes();  // Locks completedNodes


      Iterator<CallableRequest> iterator = pendingRequests.iterator();
      while (iterator.hasNext()) {
        CallableRequest request = iterator.next();
        iterator.remove();
        LlapNodeId nodeId = request.getNodeId();
        if (canRunForNode(nodeId, currentLoopDisabledNodes)) {
          submitToExecutor(request, nodeId);
        } else {
          currentLoopDisabledNodes.add(nodeId);
          currentLoopSkippedRequests.add(request);
        }
      }
      // Tried scheduling everything that could be scheduled in this loop.
      pendingRequests.addAll(0, currentLoopSkippedRequests);
      return false;
    }

    private void drainNewRequestList() {
      synchronized (newRequestList) {
        if (!newRequestList.isEmpty()) {
          pendingRequests.addAll(newRequestList);
          newRequestList.clear();
        }
      }
    }

    private void drainCompletedNodes() {
      synchronized (completedNodes) {
        if (!completedNodes.isEmpty()) {
          for (LlapNodeId nodeId : completedNodes) {
            runningRequests.get(nodeId).decrementAndGet();
          }
        }
        completedNodes.clear();
      }
    }

    private boolean canRunForNode(LlapNodeId nodeId, Set<LlapNodeId> currentRunDisabledNodes) {
      if (currentRunDisabledNodes.contains(nodeId)) {
        return false;
      } else {
        AtomicInteger count = runningRequests.get(nodeId);
        if (count == null) {
          count = new AtomicInteger(0);
          AtomicInteger old = runningRequests.putIfAbsent(nodeId, count);
          count = old != null ? old : count;
        }
        if (count.incrementAndGet() <= maxConcurrentRequestsPerNode) {
          return true;
        } else {
          count.decrementAndGet();
          return false;
        }
      }
    }

    private void notifyRunLoop() {
      lock.lock();
      try {
        queueCondition.signal();
      } finally {
        lock.unlock();
      }
    }
  }


  private static final class ResponseCallback<TYPE extends Message>
      implements FutureCallback<TYPE> {

    private final ExecuteRequestCallback<TYPE> callback;
    private final LlapNodeId nodeId;
    private final RequestManager requestManager;

    public ResponseCallback(ExecuteRequestCallback<TYPE> callback, LlapNodeId nodeId,
                            RequestManager requestManager) {
      this.callback = callback;
      this.nodeId = nodeId;
      this.requestManager = requestManager;
    }

    @Override
    public void onSuccess(TYPE result) {
      try {
        callback.setResponse(result);
      } finally {
        requestManager.requestFinished(nodeId);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      try {
        callback.indicateError(t);
      } finally {
        requestManager.requestFinished(nodeId);
      }
    }
  }

  @VisibleForTesting
  static abstract class CallableRequest<REQUEST extends Message, RESPONSE extends Message>
      implements Callable {

    final LlapNodeId nodeId;
    final ExecuteRequestCallback<RESPONSE> callback;
    final REQUEST request;


    protected CallableRequest(LlapNodeId nodeId, REQUEST request, ExecuteRequestCallback<RESPONSE> callback) {
      this.nodeId = nodeId;
      this.request = request;
      this.callback = callback;
    }

    public LlapNodeId getNodeId() {
      return nodeId;
    }

    public ExecuteRequestCallback<RESPONSE> getCallback() {
      return callback;
    }

    public abstract RESPONSE call() throws Exception;
  }

  private class SubmitWorkCallable extends CallableRequest<SubmitWorkRequestProto, SubmitWorkResponseProto> {

    protected SubmitWorkCallable(LlapNodeId nodeId,
                          SubmitWorkRequestProto submitWorkRequestProto,
                                 ExecuteRequestCallback<SubmitWorkResponseProto> callback) {
      super(nodeId, submitWorkRequestProto, callback);
    }

    @Override
    public SubmitWorkResponseProto call() throws Exception {
      return getProxy(nodeId).submitWork(null, request);
    }
  }

  private class SendSourceStateUpdateCallable
      extends CallableRequest<SourceStateUpdatedRequestProto, SourceStateUpdatedResponseProto> {

    public SendSourceStateUpdateCallable(LlapNodeId nodeId,
                                         SourceStateUpdatedRequestProto request,
                                         ExecuteRequestCallback<SourceStateUpdatedResponseProto> callback) {
      super(nodeId, request, callback);
    }

    @Override
    public SourceStateUpdatedResponseProto call() throws Exception {
      return getProxy(nodeId).sourceStateUpdated(null, request);
    }
  }

  private class SendQueryCompleteCallable
      extends CallableRequest<QueryCompleteRequestProto, QueryCompleteResponseProto> {

    protected SendQueryCompleteCallable(LlapNodeId nodeId,
                                        QueryCompleteRequestProto queryCompleteRequestProto,
                                        ExecuteRequestCallback<QueryCompleteResponseProto> callback) {
      super(nodeId, queryCompleteRequestProto, callback);
    }

    @Override
    public QueryCompleteResponseProto call() throws Exception {
      return getProxy(nodeId).queryComplete(null, request);
    }
  }

  private class SendTerminateFragmentCallable
      extends CallableRequest<TerminateFragmentRequestProto, TerminateFragmentResponseProto> {

    protected SendTerminateFragmentCallable(LlapNodeId nodeId,
                                            TerminateFragmentRequestProto terminateFragmentRequestProto,
                                            ExecuteRequestCallback<TerminateFragmentResponseProto> callback) {
      super(nodeId, terminateFragmentRequestProto, callback);
    }

    @Override
    public TerminateFragmentResponseProto call() throws Exception {
      return getProxy(nodeId).terminateFragment(null, request);
    }
  }

  public interface ExecuteRequestCallback<T extends Message> {
    void setResponse(T response);
    void indicateError(Throwable t);
  }

  private LlapProtocolBlockingPB getProxy(final LlapNodeId nodeId) {
    String hostId = getHostIdentifier(nodeId.getHostname(), nodeId.getPort());

    LlapProtocolBlockingPB proxy = hostProxies.get(hostId);
    if (proxy == null) {
      if (llapToken == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating a client without a token for " + nodeId);
        }
        proxy = new LlapProtocolClientImpl(getConfig(), nodeId.getHostname(),
            nodeId.getPort(), null, retryPolicy, socketFactory);
      } else {
        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(llapTokenUser);
        // Clone the token as we'd need to set the service to the one we are talking to.
        Token<LlapTokenIdentifier> nodeToken = new Token<LlapTokenIdentifier>(llapToken);
        SecurityUtil.setTokenService(nodeToken, NetUtils.createSocketAddrForHost(
            nodeId.getHostname(), nodeId.getPort()));
        ugi.addToken(nodeToken);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating a client for " + nodeId + "; the token is " + nodeToken);
        }
        proxy = ugi.doAs(new PrivilegedAction<LlapProtocolBlockingPB>() {
          @Override
          public LlapProtocolBlockingPB run() {
           return new LlapProtocolClientImpl(getConfig(), nodeId.getHostname(),
               nodeId.getPort(), ugi, retryPolicy, socketFactory);
          }
        });
      }

      LlapProtocolBlockingPB proxyOld = hostProxies.putIfAbsent(hostId, proxy);
      if (proxyOld != null) {
        // TODO Shutdown the new proxy.
        proxy = proxyOld;
      }
    }
    return proxy;
  }

  private String getHostIdentifier(String hostname, int port) {
    return hostname + ":" + port;
  }
}
