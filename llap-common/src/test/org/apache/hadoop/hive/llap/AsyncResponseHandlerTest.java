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
import org.apache.hadoop.hive.llap.AsyncPbRpcProxy.ExecuteRequestCallback;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AsyncResponseHandlerTest {

  private AsyncResponseHandler responseHandler;

  @Before
  public void setup() {
    AsyncPbRpcProxy.RequestManager requestManager = mock(AsyncPbRpcProxy.RequestManager.class);
    responseHandler = new AsyncResponseHandler(requestManager);
    responseHandler.start();
  }

  @After
  public void teardown() {
    responseHandler.shutdownNow();
  }

  @Test
  public void testAck() throws InterruptedException {
    ExecuteRequestCallback<Message> callback = mock(ExecuteRequestCallback.class);
    Message returnMessage = mock(Message.class);
    DummyAsyncRequest asyncRequest = createAsyncRequest(returnMessage, null, callback);

    responseHandler.addToAsyncResponseFutureQueue(asyncRequest);

    verify(callback, times(0)).setResponse(any());
    verify(callback, times(0)).indicateError(any());

    asyncRequest.finish();

    assertTrueEventually(() -> {
      verify(callback, times(1)).setResponse(returnMessage);
      verify(callback, times(0)).indicateError(any());
    });
  }

  @Test
  public void testRemoteFail() throws InterruptedException {
    ExecuteRequestCallback<Message> callback = mock(ExecuteRequestCallback.class);
    Exception returnException = new Exception();
    DummyAsyncRequest asyncRequest = createAsyncRequest(null, returnException, callback);

    responseHandler.addToAsyncResponseFutureQueue(asyncRequest);

    verify(callback, times(0)).setResponse(any());
    verify(callback, times(0)).indicateError(any());

    asyncRequest.finish();

    assertTrueEventually(() -> {
      verify(callback, times(1)).indicateError(returnException);
      verify(callback, times(0)).setResponse(any());
    });

  }

  @Test
  public void testStress() throws InterruptedException {
    final int numCommunicators = 10;
    final int totalCallbacks = 200000;

    ExecuteRequestCallback<Message> callbacks[] = new ExecuteRequestCallback[totalCallbacks];
    DummyAsyncRequest asyncRequests[] = new DummyAsyncRequest[totalCallbacks];
    for (int i = 0; i < totalCallbacks; i++) {
      callbacks[i] = mock(ExecuteRequestCallback.class);
      asyncRequests[i] = createAsyncRequest(null, null, callbacks[i]);
    }

    Thread[] communicators = new Thread[numCommunicators];
    for (int i = 0; i < numCommunicators; i++) {
      int communicatorStart = i * (totalCallbacks / numCommunicators);
      int communicatorEnd = (i + 1) * (totalCallbacks / numCommunicators);
      communicators[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          for (int j = communicatorStart; j < communicatorEnd; j++) {
            responseHandler.addToAsyncResponseFutureQueue(asyncRequests[j]);
          }
        }
      });
    }

    Thread ackerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        Random random = new Random();
        int[] ackOrder = new int[totalCallbacks];
        for (int i = 0; i < totalCallbacks; i++) {
          ackOrder[i] = i;
        }
        for (int i = 0; i < totalCallbacks; i++) {
          int swapx = random.nextInt(totalCallbacks);
          int swapy = random.nextInt(totalCallbacks);
          int temp = ackOrder[swapx];
          ackOrder[swapx] = ackOrder[swapy];
          ackOrder[swapy] = temp;
        }
        for (int i = 0; i < totalCallbacks; i++) {
          asyncRequests[i].finish();
        }
      }
    });
    for (int i = 0; i < numCommunicators; i++) {
      communicators[i].start();
    }
    ackerThread.start();

    for (int i = 0; i < numCommunicators; i++) {
      communicators[i].join();
    }
    ackerThread.join();

    assertTrueEventually(() -> {
      for (int i = 0; i < totalCallbacks; i++) {
        verify(callbacks[i], times(1)).setResponse(null);
      }
    });
  }

  private DummyAsyncRequest createAsyncRequest(Message returnValue, Exception returnException, ExecuteRequestCallback<Message> callback) {
    return new DummyAsyncRequest(returnValue, returnException, callback);
  }

  private final class DummyAsyncRequest extends AsyncPbRpcProxy.AsyncCallableRequest<Message, Message> {

    private volatile boolean isFinished = false;
    private Message returnValue;
    private Exception remoteException;

    protected DummyAsyncRequest(Message returnValue, Exception remoteException, ExecuteRequestCallback<Message> callback) {
      super(mock(LlapNodeId.class), mock(Message.class), callback);
      this.returnValue = returnValue;
      this.remoteException = remoteException;
    }

    @Override
    public void callInternal() throws Exception {
      //do nothing
    }

    @Override
    public AsyncGet<Message, Exception> getResponseFuture() {
      AsyncGet<Message, Exception> asyncGet = new AsyncGet<Message, Exception>() {
        @Override
        public Message get(long timeout, TimeUnit unit) throws Exception {
          if (remoteException != null) {
            throw remoteException;
          }
          return returnValue;
        }

        @Override
        public boolean isDone() {
          return isFinished;
        }
      };
      return asyncGet;
    }

    public void finish() {
      isFinished = true;
    }
  }

  private void assertTrueEventually(AssertTask assertTask) throws InterruptedException {
    assertTrueEventually(assertTask, 100000);
  }

  private void assertTrueEventually(AssertTask assertTask, int timeoutMillis) throws InterruptedException {
    long endTime = System.currentTimeMillis() + timeoutMillis;
    AssertionError assertionError = null;

    while (System.currentTimeMillis() < endTime) {
      try {
        assertTask.call();
        return;
      } catch (AssertionError e) {
        assertionError = e;
        sleep(50);
      }
    }
    throw assertionError;
  }

  private static interface AssertTask {
    void call() throws AssertionError;
  }

}
