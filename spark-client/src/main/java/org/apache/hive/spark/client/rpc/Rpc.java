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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * Encapsulates the RPC functionality. Provides higher-level methods to talk to the remote
 * endpoint.
 */
@InterfaceAudience.Private
public class Rpc implements Closeable {

  private static final String DISPATCHER_HANDLER_NAME = "dispatcher";
  private static final Logger LOG = LoggerFactory.getLogger(Rpc.class);

  /**
   * Creates an RPC client for a server running on the given remote host and port.
   *
   * @param config RPC configuration data.
   * @param eloop Event loop for managing the connection.
   * @param host Host name or IP address to connect to.
   * @param port Port where server is listening.
   * @param secret Secret for identifying the client with the server.
   * @param dispatcher Dispatcher used to handle RPC calls.
   * @return A future that can be used to monitor the creation of the RPC object.
   */
  public static Promise<Rpc> createClient(
      Map<String, String> config,
      final NioEventLoopGroup eloop,
      String host,
      int port,
      final String secret,
      final RpcDispatcher dispatcher) throws Exception {
    final RpcConfiguration rpcConf = new RpcConfiguration(config);
    int connectTimeoutMs = rpcConf.getConnectTimeoutMs();

    final ChannelFuture cf = new Bootstrap()
        .group(eloop)
        .handler(new ChannelInboundHandlerAdapter() { })
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
        .connect(host, port);

    final Promise<Rpc> promise = eloop.next().newPromise();
    final AtomicReference<Rpc> rpc = new AtomicReference<Rpc>();

    // Set up a timeout to undo everything.
    final Runnable timeoutTask = new Runnable() {
      @Override
      public void run() {
        promise.setFailure(new TimeoutException("Timed out waiting for RPC server connection."));
      }
    };
    final ScheduledFuture<?> timeoutFuture = eloop.schedule(timeoutTask,
        rpcConf.getServerConnectTimeoutMs(), TimeUnit.MILLISECONDS);

    // The channel listener instantiates the Rpc instance when the connection is established, and
    // sends the "Hello" message to complete the handshake.
    cf.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture cf) throws Exception {
        if (cf.isSuccess()) {
          rpc.set(createRpc(rpcConf, (SocketChannel) cf.channel(), dispatcher, eloop));
          // The RPC listener waits for confirmation from the server that the "Hello" was good.
          // Once it's finished, the Rpc object is provided to the caller by completing the
          // promise.
          Future<Void> hello = rpc.get().call(new Rpc.Hello(secret));
          hello.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> p) {
              timeoutFuture.cancel(true);
              if (p.isSuccess()) {
                promise.setSuccess(rpc.get());
              } else {
                promise.setFailure(p.cause());
              }
            }
          });
        } else {
          promise.setFailure(cf.cause());
        }
      }
    });

    // Handle cancellation of the promise.
    promise.addListener(new GenericFutureListener<Promise<Rpc>>() {
      @Override
      public void operationComplete(Promise<Rpc> p) {
        if (p.isCancelled()) {
          cf.cancel(true);
        }
      }
    });

    return promise;
  }

  /**
   * Creates an RPC handler for a connected socket channel.
   *
   * @param config RpcConfiguration object.
   * @param client Socket channel connected to the RPC remote end.
   * @param dispatcher Dispatcher used to handle RPC calls.
   * @param egroup Event executor for handling futures.
   */
  static Rpc createRpc(RpcConfiguration config, SocketChannel client,
      RpcDispatcher dispatcher, EventExecutorGroup egroup) {
    LogLevel logLevel = LogLevel.TRACE;
    if (config.getRpcChannelLogLevel() != null) {
      try {
        logLevel = LogLevel.valueOf(config.getRpcChannelLogLevel());
      } catch (Exception e) {
        LOG.warn("Invalid log level {}, reverting to default.", config.getRpcChannelLogLevel());
      }
    }

    boolean logEnabled = false;
    switch (logLevel) {
    case DEBUG:
      logEnabled = LOG.isDebugEnabled();
      break;
    case ERROR:
      logEnabled = LOG.isErrorEnabled();
      break;
    case INFO:
      logEnabled = LOG.isInfoEnabled();
      break;
    case TRACE:
      logEnabled = LOG.isTraceEnabled();
      break;
    case WARN:
      logEnabled = LOG.isWarnEnabled();
      break;
    }

    if (logEnabled) {
      client.pipeline()
          .addLast("logger", new LoggingHandler(Rpc.class, logLevel));
    }

    client.pipeline()
        .addLast("codec", new KryoMessageCodec(config.getMaxMessageSize(),
            MessageHeader.class, NullMessage.class))
        .addLast(DISPATCHER_HANDLER_NAME, dispatcher);
    return new Rpc(client, dispatcher, egroup);
  }

  @VisibleForTesting
  static Rpc createEmbedded(RpcDispatcher dispatcher) {
    EmbeddedChannel c = new EmbeddedChannel(
        new LoggingHandler(Rpc.class),
        new KryoMessageCodec(0, MessageHeader.class, NullMessage.class),
        dispatcher);
    return new Rpc(c, dispatcher, ImmediateEventExecutor.INSTANCE);
  }

  private final AtomicBoolean rpcClosed;
  private final AtomicLong rpcId;
  private final AtomicReference<RpcDispatcher> dispatcher;
  private final Channel channel;
  private final Collection<Listener> listeners;
  private final EventExecutorGroup egroup;
  private final Object channelLock;

  private Rpc(Channel channel, RpcDispatcher dispatcher, EventExecutorGroup egroup) {
    Preconditions.checkArgument(channel != null);
    Preconditions.checkArgument(dispatcher != null);
    Preconditions.checkArgument(egroup != null);
    this.channel = channel;
    this.channelLock = new Object();
    this.dispatcher = new AtomicReference(dispatcher);
    this.egroup = egroup;
    this.listeners = Lists.newLinkedList();
    this.rpcClosed = new AtomicBoolean();
    this.rpcId = new AtomicLong();

    // Note: this does not work for embedded channels.
    channel.pipeline().addLast("monitor", new ChannelInboundHandlerAdapter() {
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
          close();
        }
    });
  }

  public void addListener(Listener l) {
    synchronized (listeners) {
      listeners.add(l);
    }
  }

  /**
   * Send an RPC call to the remote endpoint and returns a future that can be used to monitor the
   * operation.
   */
  public Future<Void> call(Object msg) {
    return call(msg, Void.class);
  }

  /**
   * Send an RPC call to the remote endpoint and returns a future that can be used to monitor the
   * operation.
   *
   * @param msg RPC call to send.
   * @param retType Type of expected reply.
   * @return A future used to monitor the operation.
   */
  public <T> Future<T> call(Object msg, Class<T> retType) {
    Preconditions.checkArgument(msg != null);
    Preconditions.checkState(channel.isActive(), "RPC channel is closed.");
    try {
      final long id = rpcId.getAndIncrement();
      final Promise<T> promise = createPromise();
      ChannelFutureListener listener = new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture cf) {
            if (!cf.isSuccess() && !promise.isDone()) {
              promise.setFailure(cf.cause());
              dispatcher.get().discardRpc(id);
            }
          }
      };

      dispatcher.get().registerRpc(id, promise, msg.getClass().getName());
      synchronized (channelLock) {
        channel.write(new MessageHeader(id, Rpc.MessageType.CALL)).addListener(listener);
        channel.writeAndFlush(msg).addListener(listener);
      }
      return promise;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a promise backed by this RPC's event loop.
   */
  public <T> Promise<T> createPromise() {
    return egroup.next().newPromise();
  }

  @VisibleForTesting
  Channel getChannel() {
    return channel;
  }

  /**
   * This is only used by RpcServer after the handshake is successful. It shouldn't be called in
   * any other situation. It particularly will not work for embedded channels used for testing.
   */
  void replaceDispatcher(RpcDispatcher newDispatcher) {
    channel.pipeline().remove(DISPATCHER_HANDLER_NAME);
    channel.pipeline().addLast(DISPATCHER_HANDLER_NAME, newDispatcher);
    dispatcher.set(newDispatcher);
  }

  @Override
  public void close() {
    if (!rpcClosed.compareAndSet(false, true)) {
      return;
    }
    try {
      channel.close().sync();
    } catch (InterruptedException ie) {
      Thread.interrupted();
    } finally {
      synchronized (listeners) {
        for (Listener l : listeners) {
          try {
            l.rpcClosed(this);
          } catch (Exception e) {
            LOG.warn("Error caught in Rpc.Listener invocation.", e);
          }
        }
      }
    }
  }

  public static interface Listener {

    void rpcClosed(Rpc rpc);

  }

  static enum MessageType {
    CALL,
    REPLY,
    ERROR;
  }

  static class MessageHeader {
    final long id;
    final MessageType type;

    MessageHeader() {
      this(-1, null);
    }

    MessageHeader(long id, MessageType type) {
      this.id = id;
      this.type = type;
    }

  }

  static class Hello {
    final String secret;

    Hello() {
      this(null);
    }

    Hello(String secret) {
      this.secret = secret;
    }

  }

  static class NullMessage {

  }

}
