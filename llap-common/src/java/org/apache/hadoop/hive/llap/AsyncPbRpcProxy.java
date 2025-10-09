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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
// TODO: LlapNodeId is just a host+port pair; we could make this class more generic.
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.AsyncCallLimitExceededException;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.AsyncGet;
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
  protected Token<TokenType> token;
  protected String tokenUser;

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

      private final AsyncResponseHandler asyncResponseHandler;

      public RequestManager(int numThreads, int maxPerNode) {
        ExecutorService localExecutor = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder().setNameFormat("TaskCommunicator #%2d").build());
        maxConcurrentRequestsPerNode = maxPerNode;
        executor = MoreExecutors.listeningDecorator(localExecutor);
        asyncResponseHandler = new AsyncResponseHandler(this);
        asyncResponseHandler.start();
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
          asyncResponseHandler.shutdownNow();
          executor.shutdownNow();
          notifyRunLoop();
        }
      }

      @VisibleForTesting
      <T extends Message , U extends Message> void submitToExecutor(
          CallableRequest<T, U> request, LlapNodeId nodeId) {
        ListenableFuture<U> future = executor.submit(request);
        if (request instanceof AsyncCallableRequest) {
          Futures.addCallback(future, new AsyncResponseCallback(
                  request.getCallback(), nodeId, this,
                  (AsyncCallableRequest) request, asyncResponseHandler), MoreExecutors.directExecutor());
        } else {
          Futures.addCallback(future, new ResponseCallback<U>(
                  request.getCallback(), nodeId, this), MoreExecutors.directExecutor());
        }
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
    }, MoreExecutors.directExecutor());
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

  private static final class AsyncResponseCallback<TYPE extends Message>
          implements FutureCallback<TYPE> {

    private final AsyncPbRpcProxy.ExecuteRequestCallback<TYPE> callback;
    private final LlapNodeId nodeId;
    private final AsyncPbRpcProxy.RequestManager requestManager;
    private final AsyncPbRpcProxy.AsyncCallableRequest request;
    private final AsyncResponseHandler asyncResponseHandler;

    public AsyncResponseCallback(AsyncPbRpcProxy.ExecuteRequestCallback<TYPE> callback, LlapNodeId nodeId,
                                 AsyncPbRpcProxy.RequestManager requestManager,
                                 AsyncPbRpcProxy.AsyncCallableRequest request,
                                 AsyncResponseHandler asyncResponseHandler) {
      this.callback = callback;
      this.nodeId = nodeId;
      this.requestManager = requestManager;
      this.request = request;
      this.asyncResponseHandler = asyncResponseHandler;
    }

    @Override
    public void onSuccess(TYPE result) {
      asyncResponseHandler.addToAsyncResponseFutureQueue(request);
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

    /**
     * Override this method to make a synchronous request and wait for response.
     * @return
     * @throws Exception
     */
    public abstract RESPONSE call() throws Exception;
  }

  /**
   * Asynchronous request to a node. The request must override {@link #callInternal()}
   * @param <REQUEST>
   * @param <RESPONSE>
   */
  protected static abstract class AsyncCallableRequest<REQUEST extends Message, RESPONSE extends Message>
          extends NodeCallableRequest<REQUEST, RESPONSE> {

    private final long TIMEOUT = 60000;
    private final long BACKOFF_START = 10;
    private final int FAST_RETRIES = 5;
    private AsyncGet<Message, Exception> responseFuture;

    protected AsyncCallableRequest(LlapNodeId nodeId, REQUEST request,
                                   ExecuteRequestCallback<RESPONSE> callback) {
      super(nodeId, request, callback);
    }

    @Override
    public RESPONSE call() throws Exception {
      boolean asyncMode = Client.isAsynchronousMode();
      long deadline = System.currentTimeMillis() + TIMEOUT;
      int numRetries = 0;
      long nextBackoffMs = BACKOFF_START;
      try {
        Client.setAsynchronousMode(true);
        boolean sent = false;
        while (!sent) {
          try {
            callInternal();
            sent = true;
          } catch (Exception ex) {
            if (ex instanceof ServiceException && ex.getCause() != null
                    && ex.getCause() instanceof AsyncCallLimitExceededException) {
              numRetries++;
              if (numRetries >= FAST_RETRIES) {
                Thread.sleep(nextBackoffMs);
                if (System.currentTimeMillis() > deadline) {
                  throw new HiveException("Async request timed out in  " + TIMEOUT + " ms.", ex.getCause());
                }
                numRetries = 0;
                nextBackoffMs = nextBackoffMs * 2;
              }
              LOG.trace("Async call limit exceeded", ex);
            } else {
              throw ex;
            }
          }
        }
        responseFuture = ProtobufRpcEngine.getAsyncReturnMessage();
        return null;
      } finally {
        Client.setAsynchronousMode(asyncMode);
      }
    }

    public void callInternal() throws Exception {
      // override if async response
    }

    public AsyncGet<Message, Exception> getResponseFuture() {
      return responseFuture;
    }
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

    try {
      setToken(token);
    } catch (IOException e) {
      throw new RuntimeException(e);
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

  protected void setToken(Token<TokenType> newToken) throws IOException {
    if (tokensAreEqual(newToken)) {
      return;
    }
    LOG.info("Setting new token as it's not equal to the old one, new token is: {}", newToken);
    // clear cache to make proxies use the new token
    hostProxies.invalidateAll();

    this.token = newToken;
    if (token != null) {
      String tokenUser = getTokenUser(token);
      if (tokenUser == null) {
        try {
          tokenUser = UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        LOG.warn("Cannot determine token user from the token; using {}", tokenUser);
      }
      this.tokenUser = tokenUser;
    } else {
      this.tokenUser = null;
    }
  }

  protected boolean tokensAreEqual(Token<TokenType> otherToken) throws IOException {
    int oldSeqNumber =
        this.token == null ? -1 : ((LlapTokenIdentifier) this.token.decodeIdentifier()).getSequenceNumber();
    int newSeqNumber =
        otherToken == null ? -1 : ((LlapTokenIdentifier) otherToken.decodeIdentifier()).getSequenceNumber();

    LOG.debug("Check token equality be sequenceNumber: {} <-> {}", oldSeqNumber, newSeqNumber);
    return oldSeqNumber == newSeqNumber;
  }

  /**
   * @param nodeId Hostname + post.
   * @param nodeToken A custom node token. If not specified, the default token is used.
   * @return the protocol client implementation for the node.
   */
  protected final ProtocolType getProxy(
      final LlapNodeId nodeId, final Token<TokenType> nodeToken) {
    String hostId = getHostIdentifier(nodeId.getHostname(), nodeId.getPort());
    LOG.debug("Getting host proxies for {}", hostId);
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

  private ProtocolType createProxy(
      final LlapNodeId nodeId, Token<TokenType> nodeToken) throws IOException {
    if (nodeToken == null && this.token == null) {
      LOG.debug("Creating a client without a token for {}", nodeId);
      return createProtocolImpl(getConfig(), nodeId.getHostname(),
          nodeId.getPort(), null, retryPolicy, socketFactory);
    }
    if (this.token != null && this.tokenUser == null) {
      throw new AssertionError("Invalid internal state from " + this.token);
    }
    // Either the token should be passed in here, or in ctor.
    String tokenUser = this.tokenUser == null ? getTokenUser(nodeToken) : this.tokenUser;
    if (tokenUser == null) {
      tokenUser = UserGroupInformation.getCurrentUser().getShortUserName();
      LOG.warn("Cannot determine token user for UGI; using {}", tokenUser);
    }
    final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(tokenUser);
    // Clone the token as we'd need to set the service to the one we are talking to.
    if (nodeToken == null) {
      nodeToken = new Token<TokenType>(token);
    }
    SecurityUtil.setTokenService(nodeToken, NetUtils.createSocketAddrForHost(
        nodeId.getHostname(), nodeId.getPort()));
    ugi.addToken(nodeToken);
    LOG.debug("Creating a client for {}; the token is {}", nodeId, nodeToken);
    return ugi.doAs(new PrivilegedAction<ProtocolType>() {
      @Override
      public ProtocolType run() {
       return createProtocolImpl(getConfig(), nodeId.getHostname(),
           nodeId.getPort(), ugi, retryPolicy, socketFactory);
      }
    });
  }

  private String getHostIdentifier(String hostname, int port) {
    StringBuilder sb = new StringBuilder();
    try {
      InetAddress inetAddress = InetAddress.getByName(hostname);
      sb.append(inetAddress.getHostAddress()).append(":");
    } catch (UnknownHostException e) {
      // ignore
      LOG.warn("Unable to determine IP address for host: {}.. Ignoring..", hostname, e);
    }
    sb.append(hostname).append(":").append(port);
    return sb.toString();
  }

  protected abstract ProtocolType createProtocolImpl(Configuration config, String hostname, int port,
      UserGroupInformation ugi, RetryPolicy retryPolicy, SocketFactory socketFactory);

  protected abstract void shutdownProtocolImpl(ProtocolType proxy);

  protected abstract String getTokenUser(Token<TokenType> token);
}