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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

// TODO: move other protocols to use this too.
public class ProtobufProxy<BlockingInterface> {

  private final Configuration conf;
  private final InetSocketAddress serverAddr;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;
  private BlockingInterface proxy;
  private final Class<?> blockingInterfaceClass;
  private UserGroupInformation ugi;


  public ProtobufProxy(Configuration conf, UserGroupInformation ugi,
      String hostname, int port, @Nullable RetryPolicy retryPolicy,
      @Nullable SocketFactory socketFactory, Class<?> blockingInterfaceClass) {
    this.conf = conf;
    this.serverAddr = NetUtils.createSocketAddr(hostname, port);
    this.retryPolicy = retryPolicy;
    if (socketFactory == null) {
      this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
    } else {
      this.socketFactory = socketFactory;
    }
    this.blockingInterfaceClass = blockingInterfaceClass;
    this.ugi = ugi;
  }

  public BlockingInterface getProxy() throws IOException {
    if (proxy == null) {
      proxy = createProxy();
    }
    return proxy;
  }

  private BlockingInterface createProxy() throws IOException {
    RPC.setProtocolEngine(conf, blockingInterfaceClass, ProtobufRpcEngine.class);
    if (ugi == null) return createProxyInternal();
    try {
      return ugi.doAs(new PrivilegedExceptionAction<BlockingInterface>() {
        @Override
        public BlockingInterface run() throws IOException {
          return createProxyInternal();
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private BlockingInterface createProxyInternal() throws IOException {
    @SuppressWarnings("unchecked")
    ProtocolProxy<BlockingInterface> proxy = (ProtocolProxy<BlockingInterface>)
        RPC.getProtocolProxy(blockingInterfaceClass, 0, serverAddr,
            UserGroupInformation.getCurrentUser(), conf, socketFactory, 0, retryPolicy);
    return proxy.getProxy();
  }
}
