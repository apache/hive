/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap;

import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
// TODO: LlapNodeId is just a host+port pair; we could make this class more generic.
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;

public abstract class AsyncPbRpcProxy<ProtocolType, TokenType extends TokenIdentifier> extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncPbRpcProxy.class);

  private final Cache<String, ProtocolType> hostProxies;
  private final RequestManager requestManager;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;
  private final ListeningExecutorService requestManagerExecutor;
  private volatile ListenableFuture<Void> requestManagerFuture;
  private final Token<TokenType> token;
  private final String tokenUser;

  @VisibleForTesting
  public static class RequestManager implements Callable<Void> {
      private final Lock lock = new ReentrantLock();
      private final AtomicBoolean isShutdown = new AtomicBoolean(false);
      private final Condition queueCondition = lock.newCondition();
      private final AtomicBoolean shouldRun = new AtomicBoolean(false);

      private final int maxConcurrentRequestsPerNode;
      private final ListeningExecutorService executor;


      // Tracks new additions via add, while the loop is processing existing ones.
      private final LinkedList<CallableRequest<?, ?>> newRequestList = new LinkedList<>();

      // Tracks existing requests which are cycled through.
      private final LinkedList<CallableRequest<?, ?>> pendingRequests = new LinkedList<>();

      // Tracks requests executing per node
      private final ConcurrentMap<LlapNodeId, AtomicInteger> runningRequests = new ConcurrentHashMap<>();

      // Tracks completed requests pre node
      private final LinkedList<LlapNodeId> completedNodes = new LinkedList<>();

      public RequestManager(int numThreads, int maxPerNode) {
        ExecutorService localExecutor = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setNameFormat("TaskCommunicator #%2d").build());
        maxConcurrentRequestsPerNode = maxPerNode;
        executor = MoreExecutors.listeningDecorator(localExecutor);
      }

      @VisibleForTesting
      Set<LlapNodeId> currentLoopDisabledNodes = new HashSet<>();
      @VisibleForTesting
      List<CallableRequest<?, ?>> currentLoopSkippedRequests = new LinkedList<>();
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
            handleInterrupt(e);
            break;
          } finally {
            lock.unlock();
          }
        }
        LOG.info("CallScheduler loop exiting");
        return null;
      }

      private void handleInterrupt(InterruptedException e) {
        if (isShutdown.get()) return;
        LOG.warn("RunLoop interrupted without being shutdown first");
        throw new RuntimeException(e);
      }

      /* Add a new request to be executed */
      public void queueRequest(CallableRequest<?, ?> request) {
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
      <T extends Message , U extends Message> void submitToExecutor(
          CallableRequest<T, U> request, LlapNodeId nodeId) {
        ListenableFuture<U> future = executor.submit(request);
        Futures.addCallback(future, new ResponseCallback<U>(
            request.getCallback(), nodeId, this));
      }

      @VisibleForTesting
      boolean process() throws InterruptedException {
        if (isShutdown.get()) {
          return true;
        }
        currentLoopDisabledNodes.clear();
        currentLoopSkippedRequests.clear();

        // Set to false to block the next loop. This must be called before draining the lists,
        // otherwise an add/completion after draining the lists but before setting it to false,
        // will not trigger a run. May cause one unnecessary run if an add comes in before drain.
        // drain list. add request (setTrue). setFalse needs to be avoided.
        // TODO: why CAS if the result is not checked?
        shouldRun.compareAndSet(true, false);
        // Drain any calls which may have come in during the last execution of the loop.
        drainNewRequestList();  // Locks newRequestList
        drainCompletedNodes();  // Locks completedNodes


        Iterator<CallableRequest<?, ?>> iterator = pendingRequests.iterator();
        while (iterator.hasNext()) {
          CallableRequest<?, ?> request = iterator.next();
          iterator.remove();
          LlapNodeId nodeId;
          try {
            nodeId = request.getNodeId();
          } catch (InterruptedException e) {
            throw e;
          } catch (Exception e) {
            request.getCallback().indicateError(e);
            continue;
          }

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
        if (!(t instanceof CancellationException)) {
          LOG.warn("RequestManager shutdown with error", t);
        }
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

  protected final void queueRequest(CallableRequest<?, ?> request) {
    requestManager.queueRequest(request);
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
  protected static abstract class CallableRequest<REQUEST extends Message, RESPONSE extends Message>
        implements Callable<RESPONSE> {
    protected final ExecuteRequestCallback<RESPONSE> callback;
    protected final REQUEST request;

    protected CallableRequest(REQUEST request, ExecuteRequestCallback<RESPONSE> callback) {
      this.request = request;
      this.callback = callback;
    }

    public abstract LlapNodeId getNodeId() throws Exception;

    public ExecuteRequestCallback<RESPONSE> getCallback() {
      return callback;
    }

    public abstract RESPONSE call() throws Exception;
  }


  @VisibleForTesting
  protected static abstract class NodeCallableRequest<
    REQUEST extends Message, RESPONSE extends Message> extends CallableRequest<REQUEST, RESPONSE> {
    protected final LlapNodeId nodeId;

    protected NodeCallableRequest(LlapNodeId nodeId, REQUEST request,
        ExecuteRequestCallback<RESPONSE> callback) {
      super(request, callback);
      this.nodeId = nodeId;
    }

    @Override
    public LlapNodeId getNodeId() {
      return nodeId;
    }
  }

  public interface ExecuteRequestCallback<T extends Message> {
    void setResponse(T response);
    void indicateError(Throwable t);
  }

  public AsyncPbRpcProxy(String name, int numThreads, Configuration conf, Token<TokenType> token,
      long connectionTimeoutMs, long retrySleepMs, int expectedNodes, int maxPerNode) {
    super(name);
    // Note: we may make size/etc. configurable later.
    CacheBuilder<String, ProtocolType> cb = CacheBuilder.newBuilder().expireAfterAccess(
        1, TimeUnit.HOURS).removalListener(new RemovalListener<String, ProtocolType>() {
          @Override
          public void onRemoval(RemovalNotification<String, ProtocolType> arg) {
            if (arg == null) return;
            shutdownProtocolImpl(arg.getValue());
          }
        });
    if (expectedNodes > 0) {
      cb.maximumSize(expectedNodes * 2);
    }
    this.hostProxies = cb.build();
    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
    this.token = token;
    if (token != null) {
      String tokenUser = getTokenUser(token);
      this.tokenUser = tokenUser;
    } else {
      this.tokenUser = null;
    }

    this.retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(
        connectionTimeoutMs, retrySleepMs, TimeUnit.MILLISECONDS);

    this.requestManager = new RequestManager(numThreads, maxPerNode);
    ExecutorService localExecutor = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("RequestManagerExecutor").build());
    this.requestManagerExecutor = MoreExecutors.listeningDecorator(localExecutor);

    LOG.info("Setting up AsyncPbRpcProxy with" +
        "numThreads=" + numThreads +
        "retryTime(millis)=" + connectionTimeoutMs +
        "retrySleep(millis)=" + retrySleepMs);
  }

  /**
   * @param nodeId Hostname + post.
   * @param nodeToken A custom node token. If not specified, the default token is used.
   * @return the protocol client implementation for the node.
   */
  protected final ProtocolType getProxy(
      final LlapNodeId nodeId, final Token<TokenType> nodeToken) {
    String hostId = getHostIdentifier(nodeId.getHostname(), nodeId.getPort());
    try {
      return hostProxies.get(hostId, new Callable<ProtocolType>() {
        @Override
        public ProtocolType call() throws Exception {
          return createProxy(nodeId, nodeToken);
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private ProtocolType createProxy(final LlapNodeId nodeId, Token<TokenType> nodeToken) {
    if (nodeToken == null && token == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating a client without a token for " + nodeId);
      }
      return createProtocolImpl(getConfig(), nodeId.getHostname(),
          nodeId.getPort(), null, retryPolicy, socketFactory);
    }
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(tokenUser);
    // Clone the token as we'd need to set the service to the one we are talking to.
    if (nodeToken == null) {
      nodeToken = new Token<TokenType>(token);
    }
    SecurityUtil.setTokenService(nodeToken, NetUtils.createSocketAddrForHost(
        nodeId.getHostname(), nodeId.getPort()));
    ugi.addToken(nodeToken);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating a client for " + nodeId + "; the token is " + nodeToken);
    }
    return ugi.doAs(new PrivilegedAction<ProtocolType>() {
      @Override
      public ProtocolType run() {
       return createProtocolImpl(getConfig(), nodeId.getHostname(),
           nodeId.getPort(), ugi, retryPolicy, socketFactory);
      }
    });
  }

  private String getHostIdentifier(String hostname, int port) {
    return hostname + ":" + port;
  }

  protected abstract ProtocolType createProtocolImpl(Configuration config, String hostname, int port,
      UserGroupInformation ugi, RetryPolicy retryPolicy, SocketFactory socketFactory);

  protected abstract void shutdownProtocolImpl(ProtocolType proxy);

  protected abstract String getTokenUser(Token<TokenType> token);
}