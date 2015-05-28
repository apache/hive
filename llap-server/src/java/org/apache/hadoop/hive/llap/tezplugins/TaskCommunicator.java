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

import javax.net.SocketFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemonProtocolClientImpl;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;

public class TaskCommunicator extends AbstractService {

  private static final Log LOG = LogFactory.getLog(TaskCommunicator.class);

  private final ConcurrentMap<String, LlapDaemonProtocolBlockingPB> hostProxies;
  private ListeningExecutorService executor;

  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;

  public TaskCommunicator(int numThreads, Configuration conf) {
    super(TaskCommunicator.class.getSimpleName());
    ExecutorService localExecutor = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("TaskCommunicator #%2d").build());
    this.hostProxies = new ConcurrentHashMap<>();
    executor = MoreExecutors.listeningDecorator(localExecutor);

    this.socketFactory = NetUtils.getDefaultSocketFactory(conf);

    long connectionTimeout =
        conf.getLong(LlapConfiguration.LLAP_TASK_COMMUNICATOR_CONNECTION_TIMEOUT_MILLIS,
            LlapConfiguration.LLAP_TASK_COMMUNICATOR_CONNECTION_TIMEOUT_MILLIS_DEFAULT);
    long retrySleep = conf.getLong(
        LlapConfiguration.LLAP_TASK_COMMUNICATOR_CONNECTION_SLEEP_BETWEEN_RETRIES_MILLIS,
        LlapConfiguration.LLAP_TASK_COMMUNICATOR_CONNECTION_SLEEP_BETWEEN_RETRIES_MILLIS_DEFAULT);
    this.retryPolicy = RetryPolicies.retryUpToMaximumTimeWithFixedSleep(connectionTimeout, retrySleep,
        TimeUnit.MILLISECONDS);
    LOG.info("Setting up taskCommunicator with" +
        "numThreads=" + numThreads +
        "retryTime(millis)=" + connectionTimeout +
        "retrySleep(millis)=" + retrySleep);
  }

  @Override
  public void serviceStop() {
    executor.shutdownNow();
  }

  public void sendSubmitWork(SubmitWorkRequestProto request, String host, int port,
                         final ExecuteRequestCallback<SubmitWorkResponseProto> callback) {
    ListenableFuture<SubmitWorkResponseProto> future = executor.submit(new SubmitWorkCallable(host, port, request));
    Futures.addCallback(future, new ResponseCallback(callback));
  }

  public void sendSourceStateUpdate(final SourceStateUpdatedRequestProto request, final String host,
                                    final int port,
                                    final ExecuteRequestCallback<SourceStateUpdatedResponseProto> callback) {
    ListenableFuture<SourceStateUpdatedResponseProto> future =
        executor.submit(new SendSourceStateUpdateCallable(host, port, request));
    Futures.addCallback(future, new ResponseCallback(callback));
  }

  public void sendQueryComplete(final QueryCompleteRequestProto request, final String host,
                                final int port,
                                final ExecuteRequestCallback<QueryCompleteResponseProto> callback) {
    ListenableFuture<QueryCompleteResponseProto> future =
        executor.submit(new SendQueryCompleteCallable(host, port, request));
    Futures.addCallback(future, new ResponseCallback(callback));
  }

  public void sendTerminateFragment(final TerminateFragmentRequestProto request, final String host,
                                    final int port,
                                    final ExecuteRequestCallback<TerminateFragmentResponseProto> callback) {
    ListenableFuture<TerminateFragmentResponseProto> future =
        executor.submit(new SendTerminateFragmentCallable(host, port, request));
    Futures.addCallback(future, new ResponseCallback(callback));
  }

  private static class ResponseCallback<TYPE extends Message> implements FutureCallback<TYPE> {

    private final ExecuteRequestCallback<TYPE> callback;

    public ResponseCallback(ExecuteRequestCallback<TYPE> callback) {
      this.callback = callback;
    }

    @Override
    public void onSuccess(TYPE result) {
      callback.setResponse(result);
    }

    @Override
    public void onFailure(Throwable t) {
      callback.indicateError(t);
    }
  }

  private static abstract class CallableRequest<REQUEST extends Message, RESPONSE extends Message>
      implements Callable {

    final String hostname;
    final int port;
    final REQUEST request;


    protected CallableRequest(String hostname, int port, REQUEST request) {
      this.hostname = hostname;
      this.port = port;
      this.request = request;
    }

    public abstract RESPONSE call() throws Exception;
  }

  private class SubmitWorkCallable extends CallableRequest<SubmitWorkRequestProto, SubmitWorkResponseProto> {

    protected SubmitWorkCallable(String hostname, int port,
                          SubmitWorkRequestProto submitWorkRequestProto) {
      super(hostname, port, submitWorkRequestProto);
    }

    @Override
    public SubmitWorkResponseProto call() throws Exception {
      return getProxy(hostname, port).submitWork(null, request);
    }
  }

  private class SendSourceStateUpdateCallable
      extends CallableRequest<SourceStateUpdatedRequestProto, SourceStateUpdatedResponseProto> {

    public SendSourceStateUpdateCallable(String hostname, int port,
                                         SourceStateUpdatedRequestProto request) {
      super(hostname, port, request);
    }

    @Override
    public SourceStateUpdatedResponseProto call() throws Exception {
      return getProxy(hostname, port).sourceStateUpdated(null, request);
    }
  }

  private class SendQueryCompleteCallable
      extends CallableRequest<QueryCompleteRequestProto, QueryCompleteResponseProto> {

    protected SendQueryCompleteCallable(String hostname, int port,
                                        QueryCompleteRequestProto queryCompleteRequestProto) {
      super(hostname, port, queryCompleteRequestProto);
    }

    @Override
    public QueryCompleteResponseProto call() throws Exception {
      return getProxy(hostname, port).queryComplete(null, request);
    }
  }

  private class SendTerminateFragmentCallable
      extends CallableRequest<TerminateFragmentRequestProto, TerminateFragmentResponseProto> {

    protected SendTerminateFragmentCallable(String hostname, int port,
                                            TerminateFragmentRequestProto terminateFragmentRequestProto) {
      super(hostname, port, terminateFragmentRequestProto);
    }

    @Override
    public TerminateFragmentResponseProto call() throws Exception {
      return getProxy(hostname, port).terminateFragment(null, request);
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
      proxy = new LlapDaemonProtocolClientImpl(getConfig(), hostname, port, retryPolicy, socketFactory);
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
