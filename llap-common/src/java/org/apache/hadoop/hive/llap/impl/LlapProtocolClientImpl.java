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

package org.apache.hadoop.hive.llap.impl;

import javax.annotation.Nullable;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UpdateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UpdateFragmentResponseProto;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Change all this to be based on a regular interface instead of relying on the Proto service - Exception signatures cannot be controlled without this for the moment.


public class LlapProtocolClientImpl implements LlapProtocolBlockingPB {
  private final static Logger LOG = LoggerFactory.getLogger(LlapProtocolClientImpl.class);

  private final Configuration conf;
  private final InetSocketAddress serverAddr;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;
  private LlapProtocolBlockingPB proxy;
  private final UserGroupInformation ugi;


  public LlapProtocolClientImpl(Configuration conf, String hostname, int port,
                                UserGroupInformation ugi,
                                @Nullable RetryPolicy retryPolicy,
                                @Nullable SocketFactory socketFactory) {
    // Technically, methods run on a threadpool that is created externally with the UGI.
    // However, that is brittle, so we'd save the UGI explicitly here.
    this.ugi = ugi;
    this.conf = conf;
    this.serverAddr = NetUtils.createSocketAddr(hostname, port);
    this.retryPolicy = retryPolicy;
    if (socketFactory == null) {
      this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
    } else {
      this.socketFactory = socketFactory;
    }
  }

  @Override
  public SubmitWorkResponseProto submitWork(
      RpcController controller, SubmitWorkRequestProto request) throws ServiceException {
    try {
      return getProxy().submitWork(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(RpcController controller,
      SourceStateUpdatedRequestProto request) throws ServiceException {
    try {
      return getProxy().sourceStateUpdated(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public QueryCompleteResponseProto queryComplete(RpcController controller,
                                                  QueryCompleteRequestProto request) throws
      ServiceException {
    try {
      return getProxy().queryComplete(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TerminateFragmentResponseProto terminateFragment(
      RpcController controller,
      TerminateFragmentRequestProto request) throws ServiceException {
    try {
      return getProxy().terminateFragment(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpdateFragmentResponseProto updateFragment(RpcController controller,
      UpdateFragmentRequestProto request) throws ServiceException {
    try {
      return getProxy().updateFragment(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public LlapProtocolBlockingPB getProxy() throws IOException {
    if (proxy == null) {
      proxy = createProxy();
    }
    return proxy;
  }

  public LlapProtocolBlockingPB createProxy() throws IOException {
    RPC.setProtocolEngine(conf, LlapProtocolBlockingPB.class, ProtobufRpcEngine.class);
    LOG.info("Creating protocol proxy as " + ugi);
    if (ugi == null) return createProxyInternal();
    try {
      return ugi.doAs(new PrivilegedExceptionAction<LlapProtocolBlockingPB>() {
        @Override
        public LlapProtocolBlockingPB run() throws IOException {
          return createProxyInternal();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private LlapProtocolBlockingPB createProxyInternal() throws IOException {
    ProtocolProxy<LlapProtocolBlockingPB> proxy =
        RPC.getProtocolProxy(LlapProtocolBlockingPB.class, 0, serverAddr,
            UserGroupInformation.getCurrentUser(), conf, NetUtils.getDefaultSocketFactory(conf), 0,
            retryPolicy);
    return proxy.getProxy();
  }
}
