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
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;

import com.google.common.base.Preconditions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.Future;


/**
 * Responsible for sending back result set data to the connections made by external clients via the LLAP input format.
 */
public class LlapOutputFormatService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapOutputFormat.class);

  private static final AtomicBoolean started = new AtomicBoolean(false);
  private static final AtomicBoolean initing = new AtomicBoolean(false);
  private static LlapOutputFormatService INSTANCE;

  private final Map<String, RecordWriter> writers;
  private final Configuration conf;
  private static final int WAIT_TIME = 5;
  private static final int MAX_QUERY_ID_LENGTH = 256;

  private EventLoopGroup eventLoopGroup;
  private ServerBootstrap serverBootstrap;
  private ChannelFuture listeningChannelFuture;
  private int port;

  private LlapOutputFormatService(Configuration conf) throws IOException {
    writers = new HashMap<String, RecordWriter>();
    this.conf = conf;
  }

  public static void initializeAndStart(Configuration conf) throws Exception {
    if (!initing.getAndSet(true)) {
      INSTANCE = new LlapOutputFormatService(conf);
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
    eventLoopGroup = new NioEventLoopGroup(1);
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

    Future terminationFuture = eventLoopGroup.shutdownGracefully(1, WAIT_TIME, TimeUnit.SECONDS);
    terminationFuture.sync();
  }

  public <K,V> RecordWriter<K, V> getWriter(String id) throws IOException, InterruptedException {
    RecordWriter writer = null;
    synchronized(INSTANCE) {
      while ((writer = writers.get(id)) == null) {
        LOG.info("Waiting for writer for: "+id);
        INSTANCE.wait();
      }
    }
    LOG.info("Returning writer for: "+id);
    return writer;
  }

  public int getPort() {
    return port;
  }

  protected class LlapOutputFormatServiceHandler extends SimpleChannelInboundHandler<String> {
    private final int sendBufferSize;
    public LlapOutputFormatServiceHandler(final int sendBufferSize) {
      this.sendBufferSize = sendBufferSize;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String msg) {
      String id = msg;
      registerReader(ctx, id);
    }

    private void registerReader(ChannelHandlerContext ctx, String id) {
      synchronized(INSTANCE) {
        LOG.debug("registering socket for: " + id);
        OutputStream stream = new ChannelOutputStream(ctx, id, sendBufferSize);
        LlapRecordWriter writer = new LlapRecordWriter(stream);
        writers.put(id, writer);

        // Add listener to handle any cleanup for when the connection is closed
        ctx.channel().closeFuture().addListener(new LlapOutputFormatChannelCloseListener(id));

        INSTANCE.notifyAll();
      }
    }
  }

  protected class LlapOutputFormatChannelCloseListener implements ChannelFutureListener {
    private String id;

    LlapOutputFormatChannelCloseListener(String id) {
      this.id = id;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      RecordWriter writer = null;

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
          new DelimiterBasedFrameDecoder(MAX_QUERY_ID_LENGTH, Delimiters.nulDelimiter()),
          new StringDecoder(),
          new StringEncoder(),
          new LlapOutputFormatServiceHandler(sendBufferSize));
    }
  }
}
