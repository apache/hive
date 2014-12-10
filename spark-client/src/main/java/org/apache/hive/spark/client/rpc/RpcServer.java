/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An RPC server. The server matches remote clients based on a secret that is generated on
 * the server - the secret needs to be given to the client through some other mechanism for
 * this to work.
 */
@InterfaceAudience.Private
public class RpcServer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
  private static final SecureRandom RND = new SecureRandom();

  private final String address;
  private final Channel channel;
  private final EventLoopGroup group;
  private final int port;
  private final Collection<ClientInfo> pendingClients;
  private final RpcConfiguration config;

  public RpcServer(Map<String, String> config) throws IOException, InterruptedException {
    this.config = new RpcConfiguration(config);
    this.group = new NioEventLoopGroup(
        this.config.getRpcThreadCount(),
        new ThreadFactoryBuilder()
            .setNameFormat("RPC-Handler-%d")
            .setDaemon(true)
            .build());
    this.channel = new ServerBootstrap()
      .group(group)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            HelloDispatcher dispatcher = new HelloDispatcher();
            final Rpc newRpc = Rpc.createRpc(RpcServer.this.config, ch, dispatcher, group);
            dispatcher.rpc = newRpc;

            Runnable cancelTask = new Runnable() {
                @Override
                public void run() {
                  LOG.warn("Timed out waiting for hello from client.");
                  newRpc.close();
                }
            };
            dispatcher.cancelTask = group.schedule(cancelTask,
                RpcServer.this.config.getServerConnectTimeoutMs(),
                TimeUnit.MILLISECONDS);

          }
      })
      .option(ChannelOption.SO_BACKLOG, 1)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true)
      .bind(0)
      .sync()
      .channel();
    this.port = ((InetSocketAddress) channel.localAddress()).getPort();
    this.pendingClients = new ConcurrentLinkedQueue<ClientInfo>();
    this.address = this.config.getServerAddress();
  }

  /**
   * Tells the RPC server to expect a connection from a new client.
   *
   * @param secret The secret the client will send to the server to identify itself.
   * @param serverDispatcher The dispatcher to use when setting up the RPC instance.
   * @return A future that can be used to wait for the client connection, which also provides the
   *         secret needed for the client to connect.
   */
  public Future<Rpc> registerClient(String secret, RpcDispatcher serverDispatcher) {
    final Promise<Rpc> promise = group.next().newPromise();

    Runnable timeout = new Runnable() {
      @Override
      public void run() {
        promise.setFailure(new TimeoutException("Timed out waiting for client connection."));
      }
    };
    ScheduledFuture<?> timeoutFuture = group.schedule(timeout,
        config.getServerConnectTimeoutMs(),
        TimeUnit.MILLISECONDS);
    final ClientInfo client = new ClientInfo(promise, secret, serverDispatcher, timeoutFuture);
    pendingClients.add(client);


    promise.addListener(new GenericFutureListener<Promise<Rpc>>() {
      @Override
      public void operationComplete(Promise<Rpc> p) {
        if (p.isCancelled()) {
          pendingClients.remove(client);
        }
      }
    });

    return promise;
  }

  /**
   * Creates a secret for identifying a client connection.
   */
  public String createSecret() {
    byte[] secret = new byte[config.getSecretBits() / 8];
    RND.nextBytes(secret);

    StringBuilder sb = new StringBuilder();
    for (byte b : secret) {
      if (b < 10) {
        sb.append("0");
      }
      sb.append(Integer.toHexString(b));
    }
    return sb.toString();
  }

  public String getAddress() {
    return address;
  }

  public int getPort() {
    return port;
  }

  @Override
  public void close() {
    try {
      channel.close();
      for (Iterator<ClientInfo> clients = pendingClients.iterator(); clients.hasNext(); ) {
        ClientInfo client = clients.next();
        clients.remove();
        client.promise.cancel(true);
      }
    } finally {
      group.shutdownGracefully();
    }
  }

  private class HelloDispatcher extends RpcDispatcher {

    private Rpc rpc;
    private ScheduledFuture<?> cancelTask;

    protected void handle(ChannelHandlerContext ctx, Rpc.Hello msg) {
      cancelTask.cancel(true);

      for (Iterator<ClientInfo> clients = pendingClients.iterator(); clients.hasNext(); ) {
        ClientInfo client = clients.next();
        if (client.secret.equals(msg.secret)) {
          rpc.replaceDispatcher(client.dispatcher);
          client.timeoutFuture.cancel(true);
          client.promise.setSuccess(rpc);
          return;
        }
      }

      LOG.debug("Closing channel because secret '{}' does not match any pending client.",
          msg.secret);
      ctx.close();
    }

  }

  private static class ClientInfo {

    final Promise<Rpc> promise;
    final String secret;
    final RpcDispatcher dispatcher;
    final ScheduledFuture<?> timeoutFuture;

    private ClientInfo(Promise<Rpc> promise, String secret, RpcDispatcher dispatcher,
        ScheduledFuture<?> timeoutFuture) {
      this.promise = promise;
      this.secret = secret;
      this.dispatcher = dispatcher;
      this.timeoutFuture = timeoutFuture;
    }

  }

}
