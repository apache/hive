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

package org.apache.hadoop.hive.metastore;

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

import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;

public abstract class AbstractAsyncOperationHandler <T extends TBase, A> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncOperationHandler.class);
  private static final Map<String, AbstractAsyncOperationHandler> OPID_TO_HANDLER = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService OPID_CLEANER = Executors.newScheduledThreadPool(1, r -> {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.setName("AsyncOperationsHandler-cleaner");
    return thread;
  });

  private A result;
  private boolean async;
  private Future<A> future;
  private ExecutorService executor;
  private final AtomicBoolean aborted = new AtomicBoolean();

  protected T request;
  protected IHMSHandler handler;
  protected final String id;

  private AbstractAsyncOperationHandler(String id) {
    this.id = id;
  }

  AbstractAsyncOperationHandler(IHMSHandler handler, boolean async, T request) {
    this.id = UUID.randomUUID().toString();
    this.handler = handler;
    this.request = request;
    this.async = async;
    OPID_TO_HANDLER.put(id, this);
    if (async) {
      this.executor = Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("TableOperationsHandler " + id);
        return thread;
      });
    } else {
      this.executor = MoreExecutors.newDirectExecutorService();
    }
    this.future = executor.submit(() -> {
      try {
        return execute();
      } finally {
        destroy();
        OPID_CLEANER.schedule(() -> OPID_TO_HANDLER.remove(id), 1, TimeUnit.HOURS);
      }
    });
    this.executor.shutdown();
  }

  private static <T extends TBase, A> AbstractAsyncOperationHandler<T, A>
      ofCache(String opId, boolean shouldCancel) throws TException {
    AbstractAsyncOperationHandler<T, A> asyncOp = null;
    if (opId != null) {
      asyncOp = OPID_TO_HANDLER.get(opId);
      if (asyncOp == null && !shouldCancel) {
        throw new MetaException("Couldn't find the async operation handler: " + opId);
      }
      if (shouldCancel) {
        if (asyncOp != null) {
          asyncOp.cancelOperation();
        } else {
          asyncOp = new AbstractAsyncOperationHandler<>(opId) {
            @Override
            public OperationStatus getOperationStatus() throws TException {
              OperationStatus resp = new OperationStatus(opId);
              resp.setMessage("Operation has been canceled");
              resp.setFinished(true);
              return resp;
            }
            @Override
            protected A execute() throws TException, IOException {
              throw new UnsupportedOperationException();
            }
            @Override
            public String getLogMessagePrefix() {
              throw new UnsupportedOperationException();
            }
            @Override
            public String getOperationProgress() {
              throw new UnsupportedOperationException();
            }
          };
        }
      }
    }
    return asyncOp;
  }

  public static <T extends TBase, A> AbstractAsyncOperationHandler<T, A> offer(IHMSHandler handler, T req)
      throws TException {
    if (req instanceof DropTableRequest request) {
      AbstractAsyncOperationHandler<T, A> asycOp = ofCache(request.getId(), request.isCancel());
      if (asycOp == null) {
        asycOp= (AbstractAsyncOperationHandler<T, A>)
            new AsyncDropTableHandler(handler, request.isAsyncDrop(), request);
      }
      return asycOp;
    }
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public OperationStatus getOperationStatus() throws TException {
    String logMsgPrefix = getLogMessagePrefix();
    if (future == null) {
      throw new IllegalStateException(logMsgPrefix + " hasn't started yet");
    }
    try {
      long interval = MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) ? 10 : 5000;
      result = async ? future.get(interval, TimeUnit.MILLISECONDS) : future.get();
    } catch (TimeoutException e) {
      // No Op, return to the caller since long polling timeout has expired
      LOG.trace("{} Long polling timed out", logMsgPrefix);
    } catch (CancellationException e) {
      // The background operation thread was cancelled
      LOG.trace("{} The background operation was cancelled", logMsgPrefix);
    } catch (ExecutionException | InterruptedException e) {
      // No op, we will deal with this exception later
      LOG.error(logMsgPrefix + " Failed", e);
      if (e.getCause() instanceof Exception ex && !aborted.get()) {
        throw handleException(ex).throwIfInstance(TException.class).defaultMetaException();
      }
      String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
      throw new MetaException(logMsgPrefix + " failed with " + errorMsg);
    }

    OperationStatus resp = new OperationStatus(id);
    if (future.isDone()) {
      resp.setFinished(true);
      resp.setMessage(logMsgPrefix + (future.isCancelled() ? " Canceled" : " Done"));
    } else {
      resp.setMessage(logMsgPrefix + " In-progress, state - " + getOperationProgress());
    }
    return resp;
  }

  static class OperationStatus {
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
  }

  public void cancelOperation() {
    if (!future.isDone()) {
      LOG.warn("Drop operation: {} is still running, but a close signal is triggered", id);
      future.cancel(true);
      aborted.set(true);
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
      throw new IllegalStateException("Result is un-available as the operation " + id + " is still running");
    }
    return result;
  }

  /**
   *  Run this operation.
   *  @return computed result
   * @throws TException  if unable to run the operation
   * @throws IOException if the request is invalid
   */
  protected abstract A execute() throws TException, IOException;

  /**
   * Get the prefix for logging the message on polling the operation status.
   *
   * @return message prefix
   */
  protected abstract String getLogMessagePrefix();

  /**
   * Get the message about the operation progress.
   *
   * @return the progress
   */
  protected abstract String getOperationProgress();

  public void checkInterrupted() throws MetaException {
    if (aborted.get()) {
      String errorMessage = "FAILED: drop table " + id + " has been interrupted";
      throw new MetaException(errorMessage);
    }
  }

  /**
   * Free the resources this handler holds
   */
  void destroy() {
    request = null;
    executor = null;
    handler = null;
  }

  @VisibleForTesting
  public static boolean containsOp(String opId) {
    return OPID_TO_HANDLER.containsKey(opId);
  }
}
