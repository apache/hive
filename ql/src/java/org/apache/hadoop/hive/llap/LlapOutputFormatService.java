/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.LlapOutputSocketInitMessage;
import org.apache.hadoop.hive.llap.io.ChunkedOutputStream;
import org.apache.hadoop.hive.llap.security.SecretManager;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.string.StringEncoder;


/**
 * Responsible for sending back result set data to the connections
 * made by external clients via the LLAP input format.
 */
public class LlapOutputFormatService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapOutputFormat.class);

  private static final AtomicBoolean started = new AtomicBoolean(false);
  private static final AtomicBoolean initing = new AtomicBoolean(false);
  private static LlapOutputFormatService INSTANCE;

  // TODO: the global lock might be to coarse here.
  private final Object lock = new Object();
  private final Map<String, RecordWriter<?,?>> writers = new HashMap<String, RecordWriter<?,?>>();
  private final Map<String, String> errors = new HashMap<String, String>();
  private final Configuration conf;
  private static final int WAIT_TIME = 5;

  private EventLoopGroup eventLoopGroup;
  private ServerBootstrap serverBootstrap;
  private ChannelFuture listeningChannelFuture;
  private int port;
  private final SecretManager sm;
  private final long writerTimeoutMs;

  private LlapOutputFormatService(Configuration conf, SecretManager sm) throws IOException {
    this.sm = sm;
    this.conf = conf;
    this.writerTimeoutMs = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_DAEMON_OUTPUT_STREAM_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  public static void initializeAndStart(Configuration conf, SecretManager sm) throws Exception {
    if (!initing.getAndSet(true)) {
      INSTANCE = new LlapOutputFormatService(conf, sm);
      INSTANCE.start();
      started.set(true);
    }
  }

  public static LlapOutputFormatService get() throws IOException {
    Preconditions.checkState(started.get(),
        "LlapOutputFormatService must be started before invoking get");
    return INSTANCE;
  }

  public void start() throws IOException {
    LOG.info("Starting LlapOutputFormatService");

    int portFromConf = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT);
    int sendBufferSize = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_SEND_BUFFER_SIZE);
    // Netty defaults to no of processors * 2. Can be changed via -Dio.netty.eventLoopThreads
    eventLoopGroup = new NioEventLoopGroup();
    serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(eventLoopGroup);
    serverBootstrap.channel(NioServerSocketChannel.class);
    serverBootstrap.childHandler(new LlapOutputFormatServiceChannelHandler(sendBufferSize));
    try {
      listeningChannelFuture = serverBootstrap.bind(portFromConf).sync();
      this.port = ((InetSocketAddress) listeningChannelFuture.channel().localAddress()).getPort();
      LOG.info("LlapOutputFormatService: Binding to port: {} with send buffer size: {} ", this.port,
          sendBufferSize);
    } catch (InterruptedException err) {
      throw new IOException("LlapOutputFormatService: Error binding to port " + portFromConf, err);
    }
  }

  public void stop() throws IOException, InterruptedException {
    LOG.info("Stopping LlapOutputFormatService");

    if (listeningChannelFuture != null) {
      listeningChannelFuture.channel().close().sync();
      listeningChannelFuture = null;
    } else {
      LOG.warn("LlapOutputFormatService does not appear to have a listening port to close.");
    }

    eventLoopGroup.shutdownGracefully(1, WAIT_TIME, TimeUnit.SECONDS).sync();
  }

  @SuppressWarnings("unchecked")
  public <K,V> RecordWriter<K, V> getWriter(String id) throws IOException, InterruptedException {
    RecordWriter<?, ?> writer = null;
    synchronized (lock) {
      long startTime = System.nanoTime();
      boolean isFirst = true;
      while ((writer = writers.get(id)) == null) {
        String error = errors.remove(id);
        if (error != null) {
          throw new IOException(error);
        }
        if (isFirst) {
          LOG.info("Waiting for writer for " + id);
          isFirst = false;
        }
        if (((System.nanoTime() - startTime) / 1000000) > writerTimeoutMs) {
          throw new IOException("The writer for " + id + " has timed out after "
              + writerTimeoutMs + "ms");
        }
        lock.wait(writerTimeoutMs);
      }
    }
    LOG.info("Returning writer for: "+id);
    return (RecordWriter<K, V>) writer;
  }

  public int getPort() {
    return port;
  }

  protected class LlapOutputFormatServiceHandler
    extends SimpleChannelInboundHandler<LlapOutputSocketInitMessage> {
    private final int sendBufferSize;

    public LlapOutputFormatServiceHandler(final int sendBufferSize) {
      this.sendBufferSize = sendBufferSize;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, LlapOutputSocketInitMessage msg) {
      String id = msg.getFragmentId();
      byte[] tokenBytes = msg.hasToken() ? msg.getToken().toByteArray() : null;
      try {
        registerReader(ctx, id, tokenBytes);
      } catch (Throwable t) {
        // Make sure we fail the channel if something goes wrong.
        // We internally handle all the "expected" exceptions, so log a lot of information here.
        failChannel(ctx, id, StringUtils.stringifyException(t));
      }
    }

    private void registerReader(ChannelHandlerContext ctx, String id, byte[] tokenBytes) {
      if (sm != null) {
        try {
          sm.verifyToken(tokenBytes);
        } catch (SecurityException | IOException ex) {
          failChannel(ctx, id, ex.getMessage());
          return;
        }
      }
      LOG.debug("registering socket for: " + id);
      int maxPendingWrites = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_MAX_PENDING_WRITES);
      @SuppressWarnings("rawtypes")
      LlapRecordWriter writer = new LlapRecordWriter(id,
          new ChunkedOutputStream(
              new ChannelOutputStream(ctx, id, sendBufferSize, maxPendingWrites),
              sendBufferSize, id));
      boolean isFailed = true;
      synchronized (lock) {
        if (!writers.containsKey(id)) {
          isFailed = false;
          writers.put(id, writer);
          // Add listener to handle any cleanup for when the connection is closed
          ctx.channel().closeFuture().addListener(new LlapOutputFormatChannelCloseListener(id));
          lock.notifyAll();
        }
      }
      if (isFailed) {
        failChannel(ctx, id, "Writer already registered for " + id);
      }
    }

    /** Do not call under lock. */
    private void failChannel(ChannelHandlerContext ctx, String id, String error) {
      // TODO: write error to the channel? there's no mechanism for that now.
      ctx.close();
      synchronized (lock) {
        errors.put(id, error);
        lock.notifyAll();
      }
      LOG.error(error);
    }
  }

  protected class LlapOutputFormatChannelCloseListener implements ChannelFutureListener {
    private String id;

    LlapOutputFormatChannelCloseListener(String id) {
      this.id = id;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      RecordWriter<?, ?> writer = null;
      synchronized (INSTANCE) {
        writer = writers.remove(id);
      }

      if (writer == null) {
        LOG.warn("Did not find a writer for ID " + id);
      }
    }
  }

  protected class LlapOutputFormatServiceChannelHandler extends ChannelInitializer<SocketChannel> {
    private final int sendBufferSize;
    public LlapOutputFormatServiceChannelHandler(final int sendBufferSize) {
      this.sendBufferSize = sendBufferSize;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline().addLast(
          new ProtobufVarint32FrameDecoder(),
          new ProtobufDecoder(LlapOutputSocketInitMessage.getDefaultInstance()),
          new StringEncoder(),
          new LlapOutputFormatServiceHandler(sendBufferSize));
    }
  }
}
