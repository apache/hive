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

package org.apache.hadoop.hive.llap.tezplugins;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemonProtocolClientImpl;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.service.AbstractService;

public class TaskCommunicator extends AbstractService {

  private final ConcurrentMap<String, LlapDaemonProtocolBlockingPB> hostProxies;
  private ListeningExecutorService executor;

  public TaskCommunicator(int numThreads) {
    super(TaskCommunicator.class.getSimpleName());
    ExecutorService localExecutor = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("TaskCommunicator #%2d").build());
    this.hostProxies = new ConcurrentHashMap<>();
    executor = MoreExecutors.listeningDecorator(localExecutor);
  }

  @Override
  public void serviceStop() {
    executor.shutdownNow();
  }

  public void submitWork(SubmitWorkRequestProto request, String host, int port,
                         final ExecuteRequestCallback<SubmitWorkResponseProto> callback) {
    ListenableFuture<SubmitWorkResponseProto> future = executor.submit(new SubmitWorkCallable(request, host, port));
    Futures.addCallback(future, new FutureCallback<SubmitWorkResponseProto>() {
      @Override
      public void onSuccess(SubmitWorkResponseProto result) {
        callback.setResponse(result);
      }

      @Override
      public void onFailure(Throwable t) {
        callback.indicateError(t);
      }
    });

  }

  private class SubmitWorkCallable implements Callable<SubmitWorkResponseProto> {
    final String hostname;
    final int port;
    final SubmitWorkRequestProto request;

    private SubmitWorkCallable(SubmitWorkRequestProto request, String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
      this.request = request;
    }

    @Override
    public SubmitWorkResponseProto call() throws Exception {
      return getProxy(hostname, port).submitWork(null, request);
    }
  }

  public interface ExecuteRequestCallback<T extends Message> {
    void setResponse(T response);
    void indicateError(Throwable t);
  }

  private LlapDaemonProtocolBlockingPB getProxy(String hostname, int port) {
    String hostId = getHostIdentifier(hostname, port);

    LlapDaemonProtocolBlockingPB proxy = hostProxies.get(hostId);
    if (proxy == null) {
      proxy = new LlapDaemonProtocolClientImpl(getConfig(), hostname, port);
      LlapDaemonProtocolBlockingPB proxyOld = hostProxies.putIfAbsent(hostId, proxy);
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
