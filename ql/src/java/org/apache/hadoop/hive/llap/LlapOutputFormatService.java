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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

  private static LlapOutputFormatService service;
  private final Map<String, RecordWriter> writers;
  private final HiveConf conf;
  private static final int WAIT_TIME = 5;
  private static final int MAX_QUERY_ID_LENGTH = 256;

  private EventLoopGroup eventLoopGroup;
  private ServerBootstrap serverBootstrap;
  private ChannelFuture listeningChannelFuture;
  private int port;

  private LlapOutputFormatService() throws IOException {
    writers = new HashMap<String, RecordWriter>();
    conf = new HiveConf();
  }

  public static LlapOutputFormatService get() throws IOException {
    if (service == null) {
      service = new LlapOutputFormatService();
      service.start();
    }
    return service;
  }

  public void start() throws IOException {
    LOG.info("Starting LlapOutputFormatService");

    int portFromConf = conf.getIntVar(HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT);
    eventLoopGroup = new NioEventLoopGroup(1);
    serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(eventLoopGroup);
    serverBootstrap.channel(NioServerSocketChannel.class);
    serverBootstrap.childHandler(new LlapOutputFormatServiceChannelHandler());
    try {
      listeningChannelFuture = serverBootstrap.bind(portFromConf).sync();
      this.port = ((InetSocketAddress) listeningChannelFuture.channel().localAddress()).getPort();
      LOG.info("LlapOutputFormatService: Binding to port " + this.port);
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
    synchronized(service) {
      while ((writer = writers.get(id)) == null) {
        LOG.info("Waiting for writer for: "+id);
        service.wait();
      }
    }
    LOG.info("Returning writer for: "+id);
    return writer;
  }

  public int getPort() {
    return port;
  }

  protected class LlapOutputFormatServiceHandler extends SimpleChannelInboundHandler<String> {
    @Override
    public void channelRead0(ChannelHandlerContext ctx, String msg) {
      String id = msg;
      registerReader(ctx, id);
    }

    private void registerReader(ChannelHandlerContext ctx, String id) {
      synchronized(service) {
        LOG.debug("registering socket for: "+id);
        int bufSize = 128 * 1024; // configable?
        OutputStream stream = new ChannelOutputStream(ctx, id, bufSize);
        LlapRecordWriter writer = new LlapRecordWriter(stream);
        writers.put(id, writer);

        // Add listener to handle any cleanup for when the connection is closed
        ctx.channel().closeFuture().addListener(new LlapOutputFormatChannelCloseListener(id));

        service.notifyAll();
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

      synchronized (service) {
        writer = writers.remove(id);
      }

      if (writer == null) {
        LOG.warn("Did not find a writer for ID " + id);
      }
    }
  }

  protected class LlapOutputFormatServiceChannelHandler extends ChannelInitializer<SocketChannel> {
    @Override
    public void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline().addLast(
          new DelimiterBasedFrameDecoder(MAX_QUERY_ID_LENGTH, Delimiters.nulDelimiter()),
          new StringDecoder(),
          new StringEncoder(),
          new LlapOutputFormatServiceHandler());
    }
  }
}
