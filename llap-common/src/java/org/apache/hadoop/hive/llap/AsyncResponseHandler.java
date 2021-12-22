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

import com.google.protobuf.Message;
import org.apache.hadoop.hive.llap.AsyncPbRpcProxy.AsyncCallableRequest;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncResponseHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncResponseHandler.class);

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final AsyncPbRpcProxy.RequestManager requestManager;
  private final ExecutorService responseWaitingService = Executors.newSingleThreadExecutor();
  private final LinkedBlockingDeque<AsyncCallableRequest<Message, Message>>
          incomingResponseFutures = new LinkedBlockingDeque<>();

  public AsyncResponseHandler(AsyncPbRpcProxy.RequestManager requestManager) {
    this.requestManager = requestManager;
  }

  public void start() {
    responseWaitingService.submit(new AsyncResponseHandlerRunnable());
  }

  public void addToAsyncResponseFutureQueue(AsyncCallableRequest<Message, Message> request) {
    incomingResponseFutures.add(request);
  }

  public void shutdownNow() {
    isShutdown.set(true);
    responseWaitingService.shutdownNow();
  }

  private final class AsyncResponseHandlerRunnable implements Runnable {

    private final List<AsyncCallableRequest<Message, Message>> responseFuturesQueue = new ArrayList<>();

    @Override
    public void run() {
      while (!isShutdown.get()) {
        try {
          if (responseFuturesQueue.isEmpty()) {
            // there are no more futures to hear from, just block on incoming futures
            AsyncCallableRequest<Message, Message> request = incomingResponseFutures.take();
            responseFuturesQueue.add(request);
          }
        } catch (InterruptedException e) {
          LOG.warn("Async response handler was interrupted", e);
        }
        Iterator<AsyncCallableRequest<Message, Message>> iterator = responseFuturesQueue.iterator();
        while (iterator.hasNext()) {
          AsyncCallableRequest<Message, Message> request = iterator.next();
          AsyncGet<Message, Exception> responseFuture = request.getResponseFuture();
          if (responseFuture != null && responseFuture.isDone()) {
            try {
              iterator.remove();
              LlapNodeId nodeId = request.getNodeId();
              // since isDone is true, getFuture.get should return immediately
              try {
                Message remoteValue = responseFuture.get(-1, TimeUnit.MILLISECONDS);
                if (remoteValue instanceof Throwable) {
                  request.getCallback().indicateError((Throwable) remoteValue);
                } else {
                  request.getCallback().setResponse(remoteValue);
                }
              } catch (Exception e) {
                request.getCallback().indicateError(e);
              } finally {
                requestManager.requestFinished(nodeId);
              }
            } catch (Throwable e) {
              LOG.warn("ResponseDispatcher caught", e);
            }
          }
        }
        // check if there are more futures to add but do not block as we still
        // have futures that we are waiting to hear from
        while (!incomingResponseFutures.isEmpty()) {
          AsyncCallableRequest<Message, Message> request = incomingResponseFutures.poll();
          responseFuturesQueue.add(request);
        }
      }
      LOG.info("Async response handler exiting");
    }
  }
}
