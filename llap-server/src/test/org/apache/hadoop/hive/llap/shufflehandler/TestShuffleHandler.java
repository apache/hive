/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.shufflehandler;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hive.common.util.Retry;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

public class TestShuffleHandler {

  @Rule
  public Retry retry = new Retry(2); // in case of port collision in some tests

  private static final File TEST_DIR =
      new File(System.getProperty("test.build.data"), TestShuffleHandler.class.getName())
          .getAbsoluteFile();
  private static final String HADOOP_TMP_DIR = "hadoop.tmp.dir";

  static class LastSocketAddress {
    SocketAddress lastAddress;

    void setAddress(SocketAddress lastAddress) {
      this.lastAddress = lastAddress;
    }

    SocketAddress getSocketAddress() {
      return lastAddress;
    }
  }

  private static class MockShuffleHandler2
      extends org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler {
    MockShuffleHandler2(Configuration conf) {
      super(conf);
    }

    boolean socketKeepAlive = false;

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx, HttpRequest request,
            HttpResponse response, URL requestUri) throws IOException {
          SocketChannel channel = (SocketChannel) (ctx.channel());
          socketKeepAlive = channel.config().isKeepAlive();
        }
      };
    }

    protected boolean isSocketKeepAlive() {
      return socketKeepAlive;
    }
  }

  @Test(timeout = 10000)
  public void testKeepAlive() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to -ve keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, -100);
    final LastSocketAddress lastSocketAddress = new LastSocketAddress();

    ShuffleHandler shuffleHandler = new ShuffleHandler(conf) {
      @Override
      protected Shuffle getShuffle(final Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String jobId, int dagId, String mapId,
              int reduce, String user) throws IOException {
            return null;
          }

          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
                  throws IOException {
          }

          @Override
          protected void populateHeaders(List<String> mapIds, String jobId, int dagId, String user,
              int reduce, HttpResponse response, boolean keepAliveParam,
              Map<String, MapOutputInfo> mapOutputInfoMap) throws IOException {
            // Send some dummy data (populate content length details)
            ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }

            long contentLength = dob.getLength();
            super.setResponseHeaders(response, keepAliveParam, contentLength);
          }

          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch, String user,
              String mapId, int reduce, MapOutputInfo mapOutputInfo) throws IOException {
            lastSocketAddress.setAddress(ch.remoteAddress());

            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }

          @Override
          protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.channel().close();
            }
          }

          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error());
              ctx.channel().close();
            }
          }
        };
      }
    };

    shuffleHandler.start();

    String shuffleBaseURL = "http://127.0.0.1:"
        + conf.get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
    URL url = new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&"
        + "map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME, ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpHeaders.Values.KEEP_ALIVE,
        conn.getHeaderField(HttpHeaders.Names.CONNECTION));
    Assert.assertEquals("timeout=1", conn.getHeaderField(HttpHeaders.Values.KEEP_ALIVE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    byte[] buffer = new byte[1024];
    while (input.read(buffer) != -1) {
    }
    SocketAddress firstAddress = lastSocketAddress.getSocketAddress();
    input.close();

    // For keepAlive via URL
    url = new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&"
        + "map=attempt_12345_1_m_1_0&keepAlive=true");
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME, ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpHeaders.Values.KEEP_ALIVE,
        conn.getHeaderField(HttpHeaders.Names.CONNECTION));
    Assert.assertEquals("timeout=1", conn.getHeaderField(HttpHeaders.Values.KEEP_ALIVE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    header = new ShuffleHeader();
    header.readFields(input);
    input.close();
    SocketAddress secondAddress = lastSocketAddress.getSocketAddress();
    Assert.assertNotNull("Initial shuffle address should not be null", firstAddress);
    Assert.assertNotNull("Keep-Alive shuffle address should not be null", secondAddress);
    Assert.assertEquals(
        "Initial shuffle address and keep-alive shuffle " + "address should be the same",
        firstAddress, secondAddress);
  }

  @Test
  public void testSocketKeepAlive() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to -ve keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, -100);
    HttpURLConnection conn = null;
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2(conf);
    try {
      shuffleHandler.start();

      String shuffleBaseURL = "http://127.0.0.1:"
          + conf.get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      URL url = new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&"
          + "map=attempt_12345_1_m_1_0");
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      conn.getInputStream();
      Assert.assertTrue("socket should be set KEEP_ALIVE", shuffleHandler.isSocketKeepAlive());
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
      shuffleHandler.stop();
    }
  }

  @Test
  public void testConfigPortStatic() throws Exception {
    Random rand = new Random();
    int port = rand.nextInt(10) + 50000;
    Configuration conf = new Configuration();
    // provide a port for ShuffleHandler
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, port);
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2(conf);
    try {
      shuffleHandler.start();
      Assert.assertEquals(port, shuffleHandler.getPort());
    } finally {
      shuffleHandler.stop();
    }
  }

  @Test
  public void testConfigPortDynamic() throws Exception {
    Configuration conf = new Configuration();
    // 0 as config, should be dynamically chosen by netty
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2(conf);
    try {
      shuffleHandler.start();
      Assert.assertTrue("ShuffleHandler should use a random chosen port", shuffleHandler.getPort() > 0);
    } finally {
      shuffleHandler.stop();
    }
  }
}