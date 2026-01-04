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
import java.util.Map;
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

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AsyncOperationResp;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;

public abstract class AbstractOperationHandler<T extends TBase, A extends AbstractOperationHandler.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractOperationHandler.class);
  private static final Map<String, AbstractOperationHandler> OPID_TO_HANDLER = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService OPID_CLEANER = Executors.newScheduledThreadPool(1, r -> {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.setName("OperationHandler-Cleaner");
    return thread;
  });

  private static final Map<Class<? extends TBase>, HandlerFactory> REQ_FACTORIES = new ConcurrentHashMap<>();
  static {
    REQ_FACTORIES.put(DropTableRequest.class, (base, request) -> {
      DropTableRequest req = (DropTableRequest) request;
      AbstractOperationHandler opHandler = ofCache(req.getId(), req.isCancel());
      if (opHandler == null) {
        opHandler = new DropTableHandler(base, req);
      }
      return opHandler;
    });

    REQ_FACTORIES.put(DropDatabaseRequest.class, (base, request) -> {
      DropDatabaseRequest req = (DropDatabaseRequest) request;
      AbstractOperationHandler opHandler = ofCache(req.getId(), req.isCancel());
      if (opHandler == null) {
        opHandler = new DropDatabaseHandler(base, req);
      }
      return opHandler;
    });

    REQ_FACTORIES.put(DropPartitionsRequest.class, (base, request) -> {
      DropPartitionsRequest req = (DropPartitionsRequest) request;
      return new DropPartitionsHandler(base, req);
    });

    REQ_FACTORIES.put(AddPartitionsRequest.class, (base, request) -> {
      AddPartitionsRequest req = (AddPartitionsRequest) request;
      return new AddPartitionsHandler(base, req);
    });
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

  private AbstractOperationHandler(String id) {
    this.id = id;
  }

  AbstractOperationHandler(IHMSHandler handler, boolean async, T request) {
    this.id = UUID.randomUUID().toString();
    this.handler = handler;
    this.request = request;
    this.async = async;
    this.timeout = MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) ? 10 : 5000;
    final Timer.Context timerContext;
    if (getHandlerAlias() != null) {
      Timer timer = Metrics.getOrCreateTimer(MetricsConstants.API_PREFIX + getHandlerAlias());
      timerContext = timer != null ? timer.time() : null;
    } else {
      timerContext = null;
    }

    if (async) {
      OPID_TO_HANDLER.put(id, this);
      this.executor = Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("OperationHandler[" + id + "]");
        return thread;
      });
    } else {
      this.executor = MoreExecutors.newDirectExecutorService();
    }

    this.future =  executor.submit(() -> {
      A result = null;
      beforeExecute();
      try {
        result = execute();
      } finally {
        try {
          if (async) {
            OPID_CLEANER.schedule(() -> OPID_TO_HANDLER.remove(id), 1, TimeUnit.HOURS);
          }
          afterExecute(result);
        } finally {
          if (timerContext != null) {
            timerContext.stop();
          }
        }
      }
      return result.shrinkIfNecessary();
    });
    this.executor.shutdown();
  }

  private static <T extends TBase, A extends Result> AbstractOperationHandler<T, A>
      ofCache(String opId, boolean shouldCancel) throws TException {
    AbstractOperationHandler<T, A> opHandler = null;
    if (opId != null) {
      opHandler = OPID_TO_HANDLER.get(opId);
      if (opHandler == null && !shouldCancel) {
        throw new MetaException("Couldn't find the async operation handler: " + opId);
      }
      if (shouldCancel) {
        if (opHandler != null) {
          opHandler.cancelOperation();
        } else {
          opHandler = new AbstractOperationHandler<>(opId) {
            @Override
            public OperationStatus getOperationStatus() throws TException {
              OperationStatus resp = new OperationStatus(opId);
              resp.setMessage("Operation has been canceled");
              resp.setFinished(true);
              return resp;
            }

            @Override
            protected void beforeExecute() throws TException, IOException {
              throw new UnsupportedOperationException();
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
            public String getProgress() {
              throw new UnsupportedOperationException();
            }
          };
        }
      }
    }
    return opHandler;
  }

  public static <T extends AbstractOperationHandler> T offer(IHMSHandler handler, TBase req)
      throws TException, IOException {
    HandlerFactory factory = REQ_FACTORIES.get(req.getClass());
    if (factory != null) {
      return (T) factory.create(handler, req);
    }
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public OperationStatus getOperationStatus() throws TException {
    String logMsgPrefix = getMessagePrefix();
    if (future == null) {
      throw new IllegalStateException(logMsgPrefix + " hasn't started yet");
    }

    OperationStatus resp = new OperationStatus(id);
    if (future.isDone()) {
      resp.setFinished(true);
      resp.setMessage(logMsgPrefix + (future.isCancelled() ? " Canceled" : " Done"));
    } else {
      resp.setMessage(logMsgPrefix + " In-progress, state - " + getProgress());
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

  public static class OperationStatus {
    private final String id;
    private String message;
    private boolean finished;
    OperationStatus(String id) {
      this.id = id;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public String getId() {
      return id;
    }

    public boolean isFinished() {
      return finished;
    }

    public void setFinished(boolean finished) {
      this.finished = finished;
    }

    public AsyncOperationResp toAsyncOperationResp() {
      AsyncOperationResp resp = new AsyncOperationResp(getId());
      resp.setFinished(isFinished());
      resp.setMessage(getMessage());
      return resp;
    }
  }

  public void cancelOperation() {
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
    OperationStatus resp = getOperationStatus();
    if (!resp.isFinished()) {
      throw new IllegalStateException("Result is un-available as " +
          getMessagePrefix() + " is still running");
    }
    return (A) result;
  }

  /**
   * Method invoked prior to executing the given operation.
   * This method may be used to initialize and validate the operation and
   * executed at the same thread as the caller.
   * @throws TException
   */
  protected abstract void beforeExecute() throws TException, IOException;

  /**
   * Run this operation.
   * @return computed result
   * @throws TException  if unable to run the operation
   * @throws IOException if the request is invalid
   */
  protected abstract A execute() throws TException, IOException;

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
  protected abstract String getProgress();

  public boolean success() {
    return result != null && result.success();
  }

  /**
   * Get the alias of this handler for metrics.
   * @return the alias, null if no need to measure the operation.
   */
  protected String getHandlerAlias() {
    return null;
  }

  public void checkInterrupted() throws MetaException {
    if (aborted.get()) {
      throw new MetaException(getMessagePrefix() + " has been interrupted");
    }
  }

  /**
   * Method after the operation is done,
   * can be used to free the resources this handler holds
   */
  protected void afterExecute(A result) throws MetaException, IOException {
    handler = null;
    request = null;
  }

  @VisibleForTesting
  public static boolean containsOp(String opId) {
    return OPID_TO_HANDLER.containsKey(opId);
  }

  public interface HandlerFactory {
    AbstractOperationHandler create(IHMSHandler base, TBase req) throws TException, IOException ;
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
}
