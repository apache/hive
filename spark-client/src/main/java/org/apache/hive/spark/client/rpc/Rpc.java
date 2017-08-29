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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

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

  private static final Logger LOG = LoggerFactory.getLogger(Rpc.class);

  static final String SASL_REALM = "rsc";
  static final String SASL_USER = "rsc";
  static final String SASL_PROTOCOL = "rsc";
  static final String SASL_AUTH_CONF = "auth-conf";

  /**
   * Creates an RPC client for a server running on the given remote host and port.
   *
   * @param config RPC configuration data.
   * @param eloop Event loop for managing the connection.
   * @param host Host name or IP address to connect to.
   * @param port Port where server is listening.
   * @param clientId The client ID that identifies the connection.
   * @param secret Secret for authenticating the client with the server.
   * @param dispatcher Dispatcher used to handle RPC calls.
   * @return A future that can be used to monitor the creation of the RPC object.
   */
  public static Promise<Rpc> createClient(
      Map<String, String> config,
      final NioEventLoopGroup eloop,
      String host,
      int port,
      final String clientId,
      final String secret,
      final RpcDispatcher dispatcher) throws Exception {
    final RpcConfiguration rpcConf = new RpcConfiguration(config);
    int connectTimeoutMs = (int) rpcConf.getConnectTimeoutMs();

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
        connectTimeoutMs, TimeUnit.MILLISECONDS);

    // The channel listener instantiates the Rpc instance when the connection is established,
    // and initiates the SASL handshake.
    cf.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture cf) throws Exception {
        if (cf.isSuccess()) {
          SaslClientHandler saslHandler = new SaslClientHandler(rpcConf, clientId, promise,
            timeoutFuture, secret, dispatcher);
          Rpc rpc = createRpc(rpcConf, saslHandler, (SocketChannel) cf.channel(), eloop);
          saslHandler.rpc = rpc;
          saslHandler.sendHello(cf.channel());
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

  static Rpc createServer(SaslHandler saslHandler, RpcConfiguration config, SocketChannel channel,
      EventExecutorGroup egroup) throws IOException {
    return createRpc(config, saslHandler, channel, egroup);
  }

  private static Rpc createRpc(RpcConfiguration config,
      SaslHandler saslHandler,
      SocketChannel client,
      EventExecutorGroup egroup)
      throws IOException {
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
      client.pipeline().addLast("logger", new LoggingHandler(Rpc.class, logLevel));
    }

    KryoMessageCodec kryo = new KryoMessageCodec(config.getMaxMessageSize(),
        MessageHeader.class, NullMessage.class, SaslMessage.class);
    saslHandler.setKryoMessageCodec(kryo);
    client.pipeline()
        .addLast("codec", kryo)
        .addLast("sasl", saslHandler);
    return new Rpc(config, client, egroup);
  }

  @VisibleForTesting
  static Rpc createEmbedded(RpcDispatcher dispatcher) {
    EmbeddedChannel c = new EmbeddedChannel(
        new LoggingHandler(Rpc.class),
        new KryoMessageCodec(0, MessageHeader.class, NullMessage.class),
        dispatcher);
    Rpc rpc = new Rpc(new RpcConfiguration(Collections.<String, String>emptyMap()),
      c, ImmediateEventExecutor.INSTANCE);
    rpc.dispatcher = dispatcher;
    return rpc;
  }

  private final RpcConfiguration config;
  private final AtomicBoolean rpcClosed;
  private final AtomicLong rpcId;
  private final Channel channel;
  private final Collection<Listener> listeners;
  private final EventExecutorGroup egroup;
  private volatile RpcDispatcher dispatcher;

  private Rpc(RpcConfiguration config, Channel channel, EventExecutorGroup egroup) {
    Preconditions.checkArgument(channel != null);
    Preconditions.checkArgument(egroup != null);
    this.config = config;
    this.channel = channel;
    this.dispatcher = null;
    this.egroup = egroup;
    this.listeners = Lists.newLinkedList();
    this.rpcClosed = new AtomicBoolean();
    this.rpcId = new AtomicLong();

    // Note: this does not work for embedded channels.
    channel.pipeline().addLast("monitor", new ChannelInboundHandlerAdapter() {
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
          close();
          super.channelInactive(ctx);
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

  public boolean isActive() {
    return channel.isActive();
  }

  /**
   * Send an RPC call to the remote endpoint and returns a future that can be used to monitor the
   * operation.
   *
   * @param msg RPC call to send.
   * @param retType Type of expected reply.
   * @return A future used to monitor the operation.
   */
  public <T> Future<T> call(final Object msg, Class<T> retType) {
    Preconditions.checkArgument(msg != null);
    Preconditions.checkState(channel.isActive(), "RPC channel is closed.");
    try {
      final long id = rpcId.getAndIncrement();
      final Promise<T> promise = createPromise();
      final ChannelFutureListener listener = new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture cf) {
            if (!cf.isSuccess() && !promise.isDone()) {
              LOG.warn("Failed to send RPC, closing connection.", cf.cause());
              promise.setFailure(cf.cause());
              dispatcher.discardRpc(id);
              close();
            }
          }
      };

      dispatcher.registerRpc(id, promise, msg.getClass().getName());
      channel.eventLoop().submit(new Runnable() {
        @Override
        public void run() {
          channel.write(new MessageHeader(id, Rpc.MessageType.CALL)).addListener(listener);
          channel.writeAndFlush(msg).addListener(listener);
        }
      });
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

  void setDispatcher(RpcDispatcher dispatcher) {
    Preconditions.checkNotNull(dispatcher);
    Preconditions.checkState(this.dispatcher == null);
    this.dispatcher = dispatcher;
    channel.pipeline().addLast("dispatcher", dispatcher);
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

  public interface Listener {

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

  static class NullMessage {

  }

  static class SaslMessage {
    final String clientId;
    final byte[] payload;

    SaslMessage() {
      this(null, null);
    }

    SaslMessage(byte[] payload) {
      this(null, payload);
    }

    SaslMessage(String clientId, byte[] payload) {
      this.clientId = clientId;
      this.payload = payload;
    }

  }

  private static class SaslClientHandler extends SaslHandler implements CallbackHandler {

    private final SaslClient client;
    private final String clientId;
    private final String secret;
    private final RpcDispatcher dispatcher;
    private Promise<Rpc> promise;
    private ScheduledFuture<?> timeout;

    // Can't be set in constructor due to circular dependency.
    private Rpc rpc;

    SaslClientHandler(
        RpcConfiguration config,
        String clientId,
        Promise<Rpc> promise,
        ScheduledFuture<?> timeout,
        String secret,
        RpcDispatcher dispatcher)
        throws IOException {
      super(config);
      this.clientId = clientId;
      this.promise = promise;
      this.timeout = timeout;
      this.secret = secret;
      this.dispatcher = dispatcher;
      this.client = Sasl.createSaslClient(new String[] { config.getSaslMechanism() },
        null, SASL_PROTOCOL, SASL_REALM, config.getSaslOptions(), this);
    }

    @Override
    protected boolean isComplete() {
      return client.isComplete();
    }

    @Override
    protected String getNegotiatedProperty(String name) {
      return (String) client.getNegotiatedProperty(name);
    }

    @Override
    protected SaslMessage update(SaslMessage challenge) throws IOException {
      byte[] response = client.evaluateChallenge(challenge.payload);
      return response != null ? new SaslMessage(response) : null;
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws IOException {
      return client.wrap(data, offset, len);
    }

    @Override
    public byte[] unwrap(byte[] data, int offset, int len) throws IOException {
      return client.unwrap(data, offset, len);
    }

    @Override
    public void dispose() throws IOException {
      if (!client.isComplete()) {
        onError(new SaslException("Client closed before SASL negotiation finished."));
      }
      client.dispose();
    }

    @Override
    protected void onComplete() throws Exception {
      timeout.cancel(true);
      rpc.setDispatcher(dispatcher);
      promise.setSuccess(rpc);
      timeout = null;
      promise = null;
    }

    @Override
    protected void onError(Throwable error) {
      timeout.cancel(true);
      if (!promise.isDone()) {
        promise.setFailure(error);
      }
    }

    @Override
    public void handle(Callback[] callbacks) {
      for (Callback cb : callbacks) {
        if (cb instanceof NameCallback) {
          ((NameCallback)cb).setName(clientId);
        } else if (cb instanceof PasswordCallback) {
          ((PasswordCallback)cb).setPassword(secret.toCharArray());
        } else if (cb instanceof RealmCallback) {
          RealmCallback rb = (RealmCallback) cb;
          rb.setText(rb.getDefaultText());
        }
      }
    }

    void sendHello(Channel c) throws Exception {
      byte[] hello = client.hasInitialResponse() ?
        client.evaluateChallenge(new byte[0]) : new byte[0];
      c.writeAndFlush(new SaslMessage(clientId, hello)).addListener(future -> {
        if (!future.isSuccess()) {
          LOG.error("Failed to send hello to server", future.cause());
          onError(future.cause());
        }
      });
    }

  }

}
