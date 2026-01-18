/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.handler;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.AsyncOperationResp;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.JavaUtils.getField;
import static org.apache.hadoop.hive.metastore.utils.JavaUtils.newInstance;

public abstract class AbstractRequestHandler<T extends TBase, A extends AbstractRequestHandler.Result> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRequestHandler.class);
  private static final Map<String, AbstractRequestHandler> ID_TO_HANDLER = new ConcurrentHashMap<>();
  private static final AtomicLong ID_GEN = new AtomicLong(0);
  private static final ScheduledExecutorService REQUEST_CLEANER = Executors.newScheduledThreadPool(1, r -> {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.setName("RequestHandler-Cleaner");
    return thread;
  });

  private static final Map<Class<? extends TBase>, HandlerFactory> REQ_FACTORIES = new ConcurrentHashMap<>();
  static {
    Set<Class<? extends AbstractRequestHandler>> handlerClasses =
        new Reflections("org.apache.hadoop.hive.metastore.handler").getSubTypesOf(AbstractRequestHandler.class);
    for (Class<? extends AbstractRequestHandler> clz : handlerClasses) {
      if (Modifier.isAbstract(clz.getModifiers())) {
        continue;
      }
      RequestHandler handler = clz.getAnnotation(RequestHandler.class);
      Class<? extends TBase> requestBody;
      if (handler == null || (requestBody = handler.requestBody()) == null) {
        continue;
      }
      validateHandler(clz, handler);
      REQ_FACTORIES.put(requestBody, (base, request) -> {
        AbstractRequestHandler opHandler = null;
        if (handler.supportAsync()) {
          opHandler =
              ofCache(getField(request, handler.id()), getField(request, handler.cancel()));
        }
        if (opHandler == null) {
          opHandler =
              newInstance(clz, new Class[]{IHMSHandler.class, requestBody}, new Object[]{base, request});
        }
        return opHandler;
      });
    }
  }

  private Result result;
  private Future<Result> future;
  private ExecutorService executor;
  private final AtomicBoolean aborted = new AtomicBoolean();

  protected T request;
  protected boolean async;
  protected IHMSHandler handler;
  protected final String id;
  private long timeout;

  private AbstractRequestHandler(String id) {
    this.id = id;
  }

  AbstractRequestHandler(IHMSHandler handler, boolean async, T request) {
    this.id = UUID.randomUUID() + "-" + ID_GEN.incrementAndGet();
    this.handler = handler;
    this.request = request;
    this.async = async;
    this.timeout = MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) ? 10 : 5000;

    if (async) {
      ID_TO_HANDLER.put(id, this);
      this.executor = Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("RequestHandler[" + id + "]");
        return thread;
      });
    } else {
      this.executor = MoreExecutors.newDirectExecutorService();
    }
    this.future = executeRequest();
  }

  private Future<Result> executeRequest() {
    Timer.Context timerContext;
    if (StringUtils.isNotEmpty(getMetricAlias())) {
      Timer timer = Metrics.getOrCreateTimer(MetricsConstants.API_PREFIX + getMetricAlias());
      timerContext = timer != null ? timer.time() : null;
    } else {
      timerContext = null;
    }

    Future<Result> resultFuture = executor.submit(() -> {
      A resultV = null;
      beforeExecute();
      try {
        resultV = execute();
      } finally {
        try {
          if (async) {
            REQUEST_CLEANER.schedule(() -> ID_TO_HANDLER.remove(id), 1, TimeUnit.HOURS);
          }
          afterExecute(resultV);
        } finally {
          if (timerContext != null) {
            timerContext.stop();
          }
        }
      }
      return async ? resultV.shrinkIfNecessary() : resultV;
    });
    executor.shutdown();
    return resultFuture;
  }

  private static <T extends TBase, A extends Result> AbstractRequestHandler<T, A>
      ofCache(String reqId, boolean shouldCancel) throws TException {
    AbstractRequestHandler<T, A> opHandler = null;
    if (reqId != null) {
      opHandler = ID_TO_HANDLER.get(reqId);
      if (opHandler == null && !shouldCancel) {
        throw new MetaException("Couldn't find the async request handler: " + reqId);
      }
      if (shouldCancel) {
        if (opHandler != null) {
          opHandler.cancelRequest();
        } else {
          opHandler = new AbstractRequestHandler<>(reqId) {
            @Override
            public RequestStatus getRequestStatus() throws TException {
              RequestStatus resp = new RequestStatus(reqId);
              resp.setMessage("Request has been canceled");
              resp.setFinished(true);
              return resp;
            }
            @Override
            protected A execute() throws TException, IOException {
              throw new UnsupportedOperationException();
            }
            @Override
            public String getMessagePrefix() {
              throw new UnsupportedOperationException();
            }
            @Override
            public String getRequestProgress() {
              throw new UnsupportedOperationException();
            }
          };
        }
      }
    }
    return opHandler;
  }

  public static <T extends AbstractRequestHandler> T offer(IHMSHandler handler, TBase req)
      throws TException, IOException {
    HandlerFactory factory = REQ_FACTORIES.get(req.getClass());
    if (factory != null) {
      return (T) factory.create(handler, req);
    }
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public RequestStatus getRequestStatus() throws TException {
    String logMsgPrefix = getMessagePrefix();
    if (future == null) {
      throw new IllegalStateException(logMsgPrefix + " hasn't started yet");
    }

    RequestStatus resp = new RequestStatus(id);
    if (future.isDone()) {
      resp.setFinished(true);
      resp.setMessage(logMsgPrefix + (future.isCancelled() ? " Canceled" : " Done"));
    } else {
      resp.setMessage(logMsgPrefix + " In-progress, state - " + getRequestProgress());
    }
    
    try {
      result = async ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
    } catch (TimeoutException e) {
      // No Op, return to the caller since long polling timeout has expired
      LOG.trace("{} Long polling timed out", logMsgPrefix);
    } catch (CancellationException e) {
      // The background operation thread was cancelled
      LOG.trace("{} The background operation was cancelled", logMsgPrefix);
    } catch (ExecutionException | InterruptedException e) {
      // No op, we will deal with this exception later
      LOG.error("{} Failed", logMsgPrefix, e);
      if (e.getCause() instanceof Exception ex && !aborted.get()) {
        throw handleException(ex).throwIfInstance(TException.class).defaultMetaException();
      }
      String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
      throw new MetaException(logMsgPrefix + " failed with " + errorMsg);
    }
    return resp;
  }

  public static class RequestStatus {
    final String id;
    String message;
    boolean finished;
    RequestStatus(String id) {
      this.id = id;
    }
    public void setMessage(String message) {
      this.message = message;
    }
    public void setFinished(boolean finished) {
      this.finished = finished;
    }
    public AsyncOperationResp toAsyncOperationResp() {
      AsyncOperationResp resp = new AsyncOperationResp(id);
      resp.setFinished(finished);
      resp.setMessage(message);
      return resp;
    }
  }

  public void cancelRequest() {
    if (!future.isDone()) {
      future.cancel(true);
      aborted.set(true);
      LOG.warn("{} is still running, but a close signal is sent out", getMessagePrefix());
    }
    executor.shutdown();
  }

  /**
   * Retrieve the result after this operation is done,
   * an IllegalStateException would raise if the operation has not completed.
   * @return the operation result
   * @throws TException exception while checking the status of the operation
   */
  public final A getResult() throws TException {
    RequestStatus resp = getRequestStatus();
    if (!resp.finished) {
      throw new IllegalStateException("Result is un-available as " +
          getMessagePrefix() + " is still running");
    }
    return (A) result;
  }

  /**
   * Method invoked prior to executing the given operation.
   * This method may be used to initialize and validate the operation.
   * @throws TException
   */
  protected void beforeExecute() throws TException, IOException {
    // noop
  }

  /**
   * Run this operation.
   * @return computed result
   * @throws TException  if unable to run the operation
   * @throws IOException if the request is invalid
   */
  protected abstract A execute() throws TException, IOException;

  /**
   * Method after the operation is done.
   * Can be used to free the resources this handler holds
   */
  protected void afterExecute(A result) throws MetaException, IOException {
    handler = null;
    request = null;
  }

  /**
   * Get the prefix for logging the message on polling the operation status.
   *
   * @return message prefix
   */
  protected abstract String getMessagePrefix();

  /**
   * Get the message about the operation progress.
   *
   * @return the progress
   */
  protected abstract String getRequestProgress();

  public boolean success() throws TException {
    RequestStatus status = getRequestStatus();
    return status.finished && result != null && result.success();
  }

  /**
   * Get the alias of this handler for metrics.
   * @return the alias, null or empty if no need to measure the operation.
   */
  private String getMetricAlias() {
    RequestHandler rh = getClass().getAnnotation(RequestHandler.class);
    return rh != null ? rh.metricAlias() : null;
  }

  public void checkInterrupted() throws MetaException {
    if (aborted.get()) {
      throw new MetaException(getMessagePrefix() + " has been interrupted");
    }
  }

  @VisibleForTesting
  public static boolean containsRequest(String reqId) {
    return ID_TO_HANDLER.containsKey(reqId);
  }

  interface HandlerFactory {
    AbstractRequestHandler create(IHMSHandler base, TBase req) throws TException, IOException;
  }

  public interface Result {
    /**
     * An indicator to tell if the handler is successful or not.
     * @return true if the operation is successful, false otherwise
     */
    boolean success();

    /**
     * Sometimes we only want a small part of the result exposed to the caller,
     * this method help trim the unnecessary information from the result.
     * @return a think result returned to the caller
     */
    default Result shrinkIfNecessary() {
      return this;
    }
  }

  private static void validateHandler(Class<? extends AbstractRequestHandler> clz,
      RequestHandler handler) {
    try {
      Class<? extends TBase> requestBody = handler.requestBody();
      // Check the constructor
      clz.getDeclaredConstructor(IHMSHandler.class, requestBody);
      if (handler.supportAsync()) {
        requestBody.getMethod(handler.id());
        requestBody.getMethod(handler.cancel());
      }
    } catch (Exception e) {
      throw new RuntimeException(clz + " is not a satisfied handler as it's declared to be", e);
    }
  }
}
