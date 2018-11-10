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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

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
  private final ConcurrentMap<String, ClientInfo> pendingClients;
  private final RpcConfiguration config;
  private String applicationId;
  private final HiveConf hiveConf;

  public RpcServer(Map<String, String> mapConf, HiveConf hiveConf) throws IOException, InterruptedException {
    this.config = new RpcConfiguration(mapConf);
    this.hiveConf = hiveConf;
    this.group = new NioEventLoopGroup(
        this.config.getRpcThreadCount(),
        new ThreadFactoryBuilder()
            .setNameFormat("Spark-Driver-RPC-Handler-%d")
            .setDaemon(true)
            .build());
     ServerBootstrap serverBootstrap = new ServerBootstrap()
      .group(group)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            SaslServerHandler saslHandler = new SaslServerHandler(config);
            final Rpc newRpc = Rpc.createServer(saslHandler, config, ch, group);
            saslHandler.rpc = newRpc;

            Runnable cancelTask = new Runnable() {
                @Override
                public void run() {
                  LOG.warn("Timed out waiting for the completion of SASL negotiation "
                          + "between HiveServer2 and the Remote Spark Driver.");
                  newRpc.close();
                }
            };
            saslHandler.cancelTask = group.schedule(cancelTask,
                RpcServer.this.config.getConnectTimeoutMs(),
                TimeUnit.MILLISECONDS);

          }
      })
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);

    this.channel = bindServerPort(serverBootstrap).channel();
    this.port = ((InetSocketAddress) channel.localAddress()).getPort();
    this.pendingClients = Maps.newConcurrentMap();
    this.address = this.config.getServerAddress();

    LOG.info("Successfully created Remote Spark Driver RPC Server with address {}:{}", this.address, this.port);
  }

  /**
   * Retry the list of configured ports until one is found
   * @param serverBootstrap
   * @return
   * @throws InterruptedException
   * @throws IOException
   */
  private ChannelFuture bindServerPort(ServerBootstrap serverBootstrap)
      throws InterruptedException, IOException {
    List<Integer> ports = config.getServerPorts();
    if (ports.contains(0)) {
      return serverBootstrap.bind(0).sync();
    } else {
      Random rand = new Random();
      while(!ports.isEmpty()) {
        int index = rand.nextInt(ports.size());
        int port = ports.get(index);
        ports.remove(index);
        try {
          return serverBootstrap.bind(port).sync();
        } catch(Exception e) {
          // Retry the next port
        }
      }
      throw new IOException("Remote Spark Driver RPC Server cannot bind to any of the configured ports: "
              + Arrays.toString(config.getServerPorts().toArray()));
    }
  }

  /**
   * Tells the RPC server to expect a connection from a new client.
   *
   * @param clientId An identifier for the client. Must be unique.
   * @param secret The secret the client will send to the server to identify itself.
   * @param serverDispatcher The dispatcher to use when setting up the RPC instance.
   * @return A future that can be used to wait for the client connection, which also provides the
   *         secret needed for the client to connect.
   */
  public Future<Rpc> registerClient(final String clientId, String secret,
      RpcDispatcher serverDispatcher) {
    return registerClient(clientId, secret, serverDispatcher, config.getServerConnectTimeoutMs());
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  /**
   * This function converts an application in form of a String into a ApplicationId.
   *
   * @param appIDStr The application id in form of a string
   * @return the application id as an instance of ApplicationId class.
   */
  private static ApplicationId getApplicationIDFromString(String appIDStr) {
    String[] parts = appIDStr.split("_");
    if (parts.length < 3) {
      throw new IllegalStateException("the application id found is not valid. application id: " + appIDStr);
    }
    long timestamp = Long.parseLong(parts[1]);
    int id = Integer.parseInt(parts[2]);
    return ApplicationId.newInstance(timestamp, id);
  }

  public static boolean isApplicationAccepted(HiveConf conf, String applicationId) {
    if (applicationId == null) {
      return false;
    }
    YarnClient yarnClient = null;
    try {
      ApplicationId appId = getApplicationIDFromString(applicationId);
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      return appReport != null && appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED;
    } catch (Exception ex) {
      LOG.error("Failed getting application status for: " + applicationId + ": " + ex, ex);
      return false;
    } finally {
      if (yarnClient != null) {
        try {
          yarnClient.stop();
        } catch (Exception ex) {
          LOG.error("Failed to stop yarn client: " + ex, ex);
        }
      }
    }
  }

  static class YarnApplicationStateFinder {
    public boolean isApplicationAccepted(HiveConf conf, String applicationId) {
      if (applicationId == null) {
        return false;
      }
      YarnClient yarnClient = null;
      try {
        LOG.info("Trying to find " + applicationId);
        ApplicationId appId = getApplicationIDFromString(applicationId);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        return appReport != null && appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED;
      } catch (Exception ex) {
        LOG.error("Failed getting application status for: " + applicationId + ": " + ex, ex);
        return false;
      } finally {
        if (yarnClient != null) {
          try {
            yarnClient.stop();
          } catch (Exception ex) {
            LOG.error("Failed to stop yarn client: " + ex, ex);
          }
        }
      }
    }
  }

  @VisibleForTesting
  Future<Rpc> registerClient(final String clientId, String secret,
      RpcDispatcher serverDispatcher, final long clientTimeoutMs) {
    return registerClient(clientId, secret, serverDispatcher, clientTimeoutMs, new YarnApplicationStateFinder());
  }

  @VisibleForTesting
  Future<Rpc> registerClient(final String clientId, String secret,
      RpcDispatcher serverDispatcher, long clientTimeoutMs,
      YarnApplicationStateFinder yarnApplicationStateFinder) {
    final Promise<Rpc> promise = group.next().newPromise();

    Runnable timeout = new Runnable() {
      @Override
      public void run() {
        // check to see if application is in ACCEPTED state, if so, don't set failure
        // if applicationId is not null
        //   do yarn application -status $applicationId
        //   if state == ACCEPTED
        //     reschedule timeout runnable
        //   else
        //    set failure as below
        LOG.info("Trying to find " + applicationId);
        if (yarnApplicationStateFinder.isApplicationAccepted(hiveConf, applicationId)) {
          final ClientInfo client = pendingClients.get(clientId);
          if (client != null) {
            LOG.info("Extending timeout for client " + clientId);
            ScheduledFuture<?> oldTimeoutFuture = client.timeoutFuture;
            client.timeoutFuture = group.schedule(this,
                clientTimeoutMs,
                TimeUnit.MILLISECONDS);
            oldTimeoutFuture.cancel(true);
            return;
          }
        }
        promise.setFailure(new TimeoutException(
                String.format("Client '%s' timed out waiting for connection from the Remote Spark" +
                        " Driver", clientId)));
      }
    };
    ScheduledFuture<?> timeoutFuture = group.schedule(timeout,
        clientTimeoutMs,
        TimeUnit.MILLISECONDS);
    final ClientInfo client = new ClientInfo(clientId, promise, secret, serverDispatcher,
        timeoutFuture);
    if (pendingClients.putIfAbsent(clientId, client) != null) {
      throw new IllegalStateException(
          String.format("Remote Spark Driver with client ID '%s' already registered", clientId));
    }

    promise.addListener(new GenericFutureListener<Promise<Rpc>>() {
      @Override
      public void operationComplete(Promise<Rpc> p) {
        if (!p.isSuccess()) {
          pendingClients.remove(clientId);
        }
      }
    });

    return promise;
  }

  /**
   * Tells the RPC server to cancel the connection from an existing pending client.
   *
   * @param clientId The identifier for the client
   * @param failure The error about why the connection should be canceled
   */
  public void cancelClient(final String clientId, final Throwable failure) {
    final ClientInfo cinfo = pendingClients.remove(clientId);
    if (cinfo == null) {
      // Nothing to be done here.
      return;
    }
    cinfo.timeoutFuture.cancel(true);
    if (!cinfo.promise.isDone()) {
      cinfo.promise.setFailure(failure);
    }
  }

  /**
   * Tells the RPC server to cancel the connection from an existing pending client.
   *
   * @param clientId The identifier for the client
   * @param msg The error message about why the connection should be canceled
   */
  public void cancelClient(final String clientId, final String msg) {
    cancelClient(clientId, new RuntimeException(String.format(
            "Cancelling Remote Spark Driver client connection '%s' with error: " + msg, clientId)));
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
      for (ClientInfo client : pendingClients.values()) {
        client.promise.cancel(true);
      }
      pendingClients.clear();
    } finally {
      group.shutdownGracefully();
    }
  }

  private class SaslServerHandler extends SaslHandler implements CallbackHandler {

    private final SaslServer server;
    private Rpc rpc;
    private ScheduledFuture<?> cancelTask;
    private String clientId;
    private ClientInfo client;

    SaslServerHandler(RpcConfiguration config) throws IOException {
      super(config);
      this.server = Sasl.createSaslServer(config.getSaslMechanism(), Rpc.SASL_PROTOCOL,
        Rpc.SASL_REALM, config.getSaslOptions(), this);
    }

    @Override
    protected boolean isComplete() {
      return server.isComplete();
    }

    @Override
    protected String getNegotiatedProperty(String name) {
      return (String) server.getNegotiatedProperty(name);
    }

    @Override
    protected Rpc.SaslMessage update(Rpc.SaslMessage challenge) throws IOException {
      if (clientId == null) {
        Preconditions.checkArgument(challenge.clientId != null,
          "Missing client ID in SASL handshake.");
        clientId = challenge.clientId;
        client = pendingClients.get(clientId);
        Preconditions.checkArgument(client != null,
          "Unexpected client ID '%s' in SASL handshake.", clientId);
      }

      return new Rpc.SaslMessage(server.evaluateResponse(challenge.payload));
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws IOException {
      return server.wrap(data, offset, len);
    }

    @Override
    public byte[] unwrap(byte[] data, int offset, int len) throws IOException {
      return server.unwrap(data, offset, len);
    }

    @Override
    public void dispose() throws IOException {
      if (!server.isComplete()) {
        onError(new SaslException("Server closed before SASL negotiation finished."));
      }
      server.dispose();
    }

    @Override
    protected void onComplete() throws Exception {
      cancelTask.cancel(true);
      client.timeoutFuture.cancel(true);
      rpc.setDispatcher(client.dispatcher);
      client.promise.setSuccess(rpc);
      pendingClients.remove(client.id);
    }

    @Override
    protected void onError(Throwable error) {
      cancelTask.cancel(true);
      if (client != null) {
        client.timeoutFuture.cancel(true);
        if (!client.promise.isDone()) {
          client.promise.setFailure(error);
        }
      }
    }

    @Override
    public void handle(Callback[] callbacks) {
      Preconditions.checkState(client != null, "Handshake not initialized yet.");
      for (Callback cb : callbacks) {
        if (cb instanceof NameCallback) {
          ((NameCallback)cb).setName(clientId);
        } else if (cb instanceof PasswordCallback) {
          ((PasswordCallback)cb).setPassword(client.secret.toCharArray());
        } else if (cb instanceof AuthorizeCallback) {
          ((AuthorizeCallback) cb).setAuthorized(true);
        } else if (cb instanceof RealmCallback) {
          RealmCallback rb = (RealmCallback) cb;
          rb.setText(rb.getDefaultText());
        }
      }
    }

  }

  private static class ClientInfo {

    final String id;
    final Promise<Rpc> promise;
    final String secret;
    final RpcDispatcher dispatcher;
    ScheduledFuture<?> timeoutFuture;

    private ClientInfo(String id, Promise<Rpc> promise, String secret, RpcDispatcher dispatcher,
        ScheduledFuture<?> timeoutFuture) {
      this.id = id;
      this.promise = promise;
      this.secret = secret;
      this.dispatcher = dispatcher;
      this.timeoutFuture = timeoutFuture;
    }

  }

}
