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
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;

public abstract class AbstractOperationHandler<T extends TBase, A> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractOperationHandler.class);
  private static final Map<String, AbstractOperationHandler> OPID_TO_HANDLER = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService OPID_CLEANER = Executors.newScheduledThreadPool(1, r -> {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.setName("OperationHandler-Cleaner");
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
  private long timeout;

  private AbstractOperationHandler(String id) {
    this.id = id;
  }

  AbstractOperationHandler(IHMSHandler handler, boolean async, T request)
      throws TException, IOException {
    this.id = UUID.randomUUID().toString();
    this.handler = handler;
    this.request = request;
    this.async = async;
    this.timeout = MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) ? 10 : 5000;
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
    beforeExecute();
    this.future =
        executor.submit(() -> {
          try {
            return execute();
          } finally {
            afterExecute();
            OPID_CLEANER.schedule(() -> OPID_TO_HANDLER.remove(id), 1, TimeUnit.HOURS);
          }
        });
    this.executor.shutdown();
  }

  private static <T extends TBase, A> AbstractOperationHandler<T, A>
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

  public static <T extends TBase> AbstractOperationHandler offer(IHMSHandler handler, T req)
      throws TException, IOException {
    AbstractOperationHandler opHandler = null;
    if (req instanceof DropTableRequest request) {
      opHandler = ofCache(request.getId(), request.isCancel());
      if (opHandler == null) {
        opHandler = new DropTableHandler(handler, request.isAsyncDrop(), request);
      }
    } else if (req instanceof DropDatabaseRequest request) {
      opHandler = ofCache(request.getId(), request.isCancel());
      if (opHandler == null) {
        opHandler = new DropDatabaseHandler(handler, request);
      }
    } else if (req instanceof DropPartitionsRequest request) {
      opHandler = new DropPartitionsHandler(handler, request);
    } else if (req instanceof AddPartitionsRequest request) {
      opHandler = new AddPartitionsHandler(handler, request);
    }
    if (opHandler != null) {
      return opHandler;
    }
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public OperationStatus getOperationStatus() throws TException {
    String logMsgPrefix = getMessagePrefix();
    if (future == null) {
      throw new IllegalStateException(logMsgPrefix + " hasn't started yet");
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

    OperationStatus resp = new OperationStatus(id);
    if (future.isDone()) {
      resp.setFinished(true);
      resp.setMessage(logMsgPrefix + (future.isCancelled() ? " Canceled" : " Done"));
    } else {
      resp.setMessage(logMsgPrefix + " In-progress, state - " + getProgress());
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
    return result;
  }

  /**
   * Method invoked prior to executing the given operation.
   * This method may be used to initialize and validate the operation and
   * executed at the same thread as the caller.
   * @throws TException
   */
  protected void beforeExecute() throws TException, IOException {

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
  protected abstract String getMessagePrefix();

  /**
   * Get the message about the operation progress.
   *
   * @return the progress
   */
  protected abstract String getProgress();

  public void checkInterrupted() throws MetaException {
    if (aborted.get()) {
      throw new MetaException(getMessagePrefix() + " has been interrupted");
    }
  }

  /**
   * Method after the operation is done,
   * can be used to free the resources this handler holds
   */
  protected void afterExecute() {
    request = null;
    handler = null;
  }

  @VisibleForTesting
  public static boolean containsOp(String opId) {
    return OPID_TO_HANDLER.containsKey(opId);
  }

}
