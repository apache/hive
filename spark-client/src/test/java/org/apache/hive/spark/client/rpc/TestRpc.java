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
import java.net.InetAddress;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.sasl.SaslException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestRpc {

  private static final Logger LOG = LoggerFactory.getLogger(TestRpc.class);

  private Collection<Closeable> closeables;
  private static final Map<String, String> emptyConfig =
      ImmutableMap.of(HiveConf.ConfVars.SPARK_RPC_CHANNEL_LOG_LEVEL.varname, "DEBUG");

  @Before
  public void setUp() {
    closeables = Lists.newArrayList();
  }

  @After
  public void cleanUp() throws Exception {
    for (Closeable c : closeables) {
      IOUtils.closeQuietly(c);
    }
  }

  private <T extends Closeable> T autoClose(T closeable) {
    closeables.add(closeable);
    return closeable;
  }

  @Test
  public void testRpcDispatcher() throws Exception {
    Rpc serverRpc = autoClose(Rpc.createEmbedded(new TestDispatcher()));
    Rpc clientRpc = autoClose(Rpc.createEmbedded(new TestDispatcher()));

    TestMessage outbound = new TestMessage("Hello World!");
    Future<TestMessage> call = clientRpc.call(outbound, TestMessage.class);

    LOG.debug("Transferring messages...");
    transfer(serverRpc, clientRpc);

    TestMessage reply = call.get(10, TimeUnit.SECONDS);
    assertEquals(outbound.message, reply.message);
  }

  @Test
  public void testClientServer() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    Rpc[] rpcs = createRpcConnection(server);
    Rpc serverRpc = rpcs[0];
    Rpc client = rpcs[1];

    TestMessage outbound = new TestMessage("Hello World!");
    Future<TestMessage> call = client.call(outbound, TestMessage.class);
    TestMessage reply = call.get(10, TimeUnit.SECONDS);
    assertEquals(outbound.message, reply.message);

    TestMessage another = new TestMessage("Hello again!");
    Future<TestMessage> anotherCall = client.call(another, TestMessage.class);
    TestMessage anotherReply = anotherCall.get(10, TimeUnit.SECONDS);
    assertEquals(another.message, anotherReply.message);

    String errorMsg = "This is an error.";
    try {
      client.call(new ErrorCall(errorMsg)).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof RpcException);
      assertTrue(ee.getCause().getMessage().indexOf(errorMsg) >= 0);
    }

    // Test from server to client too.
    TestMessage serverMsg = new TestMessage("Hello from the server!");
    Future<TestMessage> serverCall = serverRpc.call(serverMsg, TestMessage.class);
    TestMessage serverReply = serverCall.get(10, TimeUnit.SECONDS);
    assertEquals(serverMsg.message, serverReply.message);
  }

  @Test
  public void testServerAddress() throws Exception {
    String hostAddress = InetAddress.getLocalHost().getHostName();
    Map<String, String> config = new HashMap<String, String>();

    // Test if rpc_server_address is configured
    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_ADDRESS.varname, hostAddress);
    RpcServer server1 = autoClose(new RpcServer(config));
    assertTrue("Host address should match the expected one", server1.getAddress() == hostAddress);

    // Test if rpc_server_address is not configured but HS2 server host is configured
    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_ADDRESS.varname, "");
    config.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, hostAddress);
    RpcServer server2 = autoClose(new RpcServer(config));
    assertTrue("Host address should match the expected one", server2.getAddress() == hostAddress);

    // Test if both are not configured
    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_ADDRESS.varname, "");
    config.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, "");
    RpcServer server3 = autoClose(new RpcServer(config));
    assertTrue("Host address should match the expected one", server3.getAddress() == InetAddress.getLocalHost().getHostName());
  }

  @Test
  public void testBadHello() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));

    Future<Rpc> serverRpcFuture = server.registerClient("client", "newClient",
        new TestDispatcher());
    NioEventLoopGroup eloop = new NioEventLoopGroup();

    Future<Rpc> clientRpcFuture = Rpc.createClient(emptyConfig, eloop,
        "localhost", server.getPort(), "client", "wrongClient", new TestDispatcher());

    try {
      autoClose(clientRpcFuture.get(10, TimeUnit.SECONDS));
      fail("Should have failed to create client with wrong secret.");
    } catch (ExecutionException ee) {
      // On failure, the SASL handler will throw an exception indicating that the SASL
      // negotiation failed.
      assertTrue("Unexpected exception: " + ee.getCause(),
        ee.getCause() instanceof SaslException);
    }

    serverRpcFuture.cancel(true);
  }

  @Test
  public void testServerPort() throws Exception {
    Map<String, String> config = new HashMap<String, String>();

    RpcServer server0 = new RpcServer(config);
    assertTrue("Empty port range should return a random valid port: " + server0.getPort(), server0.getPort() >= 0);
    IOUtils.closeQuietly(server0);

    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_PORT.varname, "49152-49222,49223,49224-49333");
    RpcServer server1 = new RpcServer(config);
    assertTrue("Port should be within configured port range:" + server1.getPort(), server1.getPort() >= 49152 && server1.getPort() <= 49333);
    IOUtils.closeQuietly(server1);

    int expectedPort = 65535;
    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_PORT.varname, String.valueOf(expectedPort));
    RpcServer server2 = new RpcServer(config);
    assertTrue("Port should match configured one: " + server2.getPort(), server2.getPort() == expectedPort);
    IOUtils.closeQuietly(server2);

    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_PORT.varname, "49552-49222,49223,49224-49333");
    try {
      autoClose(new RpcServer(config));
      assertTrue("Invalid port range should throw an exception", false); // Should not reach here
    } catch(IOException e) {
      assertEquals("Incorrect RPC server port configuration for HiveServer2", e.getMessage());
    }

    // Retry logic
    expectedPort = 65535;
    config.put(HiveConf.ConfVars.SPARK_RPC_SERVER_PORT.varname, String.valueOf(expectedPort) + ",21-23");
    RpcServer server3 = new RpcServer(config);
    assertTrue("Port should match configured one:" + server3.getPort(), server3.getPort() == expectedPort);
    IOUtils.closeQuietly(server3);
  }

  @Test
  public void testCloseListener() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    Rpc[] rpcs = createRpcConnection(server);
    Rpc client = rpcs[1];

    final AtomicInteger closeCount = new AtomicInteger();
    client.addListener(new Rpc.Listener() {
        @Override
        public void rpcClosed(Rpc rpc) {
          closeCount.incrementAndGet();
        }
    });

    client.close();
    client.close();
    assertEquals(1, closeCount.get());
  }

  @Test
  public void testNotDeserializableRpc() throws Exception {
    RpcServer server = autoClose(new RpcServer(emptyConfig));
    Rpc[] rpcs = createRpcConnection(server);
    Rpc client = rpcs[1];

    try {
      client.call(new NotDeserializable(42)).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof RpcException);
      assertTrue(ee.getCause().getMessage().indexOf("KryoException") >= 0);
    }
  }

  @Test
  public void testEncryption() throws Exception {
    Map<String, String> eConf = ImmutableMap.<String,String>builder()
      .putAll(emptyConfig)
      .put(RpcConfiguration.RPC_SASL_OPT_PREFIX + "qop", Rpc.SASL_AUTH_CONF)
      .build();
    RpcServer server = autoClose(new RpcServer(eConf));
    Rpc[] rpcs = createRpcConnection(server, eConf, null);
    Rpc client = rpcs[1];

    TestMessage outbound = new TestMessage("Hello World!");
    Future<TestMessage> call = client.call(outbound, TestMessage.class);
    TestMessage reply = call.get(10, TimeUnit.SECONDS);
    assertEquals(outbound.message, reply.message);
  }

  @Test
  public void testClientTimeout() throws Exception {
    Map<String, String> conf = ImmutableMap.<String,String>builder()
      .putAll(emptyConfig)
      .build();
    RpcServer server = autoClose(new RpcServer(conf));
    String secret = server.createSecret();

    try {
      autoClose(server.registerClient("client", secret, new TestDispatcher(), 1L).get());
      fail("Server should have timed out client.");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() instanceof TimeoutException);
    }

    NioEventLoopGroup eloop = new NioEventLoopGroup();
    Future<Rpc> clientRpcFuture = Rpc.createClient(conf, eloop,
        "localhost", server.getPort(), "client", secret, new TestDispatcher());
    try {
      autoClose(clientRpcFuture.get());
      fail("Client should have failed to connect to server.");
    } catch (ExecutionException ee) {
      // Error should not be a timeout.
      assertFalse(ee.getCause() instanceof TimeoutException);
    }
  }

  @Test
  public void testRpcServerMultiThread() throws Exception {
    final RpcServer server = autoClose(new RpcServer(emptyConfig));
    final String msg = "Hello World!";
    Callable<String> callable = () -> {
      Rpc[] rpcs = createRpcConnection(server, emptyConfig, UUID.randomUUID().toString());
      Rpc rpc;
      if (ThreadLocalRandom.current().nextBoolean()) {
        rpc = rpcs[0];
      } else {
        rpc = rpcs[1];
      }
      TestMessage outbound = new TestMessage("Hello World!");
      Future<TestMessage> call = rpc.call(outbound, TestMessage.class);
      TestMessage reply = call.get(10, TimeUnit.SECONDS);
      return reply.message;
    };
    final int numThreads = ThreadLocalRandom.current().nextInt(5) + 5;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<java.util.concurrent.Future<String>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(callable));
    }
    executor.shutdown();
    for (java.util.concurrent.Future<String> future : futures) {
      assertEquals(msg, future.get());
    }
  }

  private void transfer(Rpc serverRpc, Rpc clientRpc) {
    EmbeddedChannel client = (EmbeddedChannel) clientRpc.getChannel();
    EmbeddedChannel server = (EmbeddedChannel) serverRpc.getChannel();

    server.runPendingTasks();
    client.runPendingTasks();

    int count = 0;
    while (!client.outboundMessages().isEmpty()) {
      server.writeInbound(client.readOutbound());
      count++;
    }
    server.flush();
    LOG.debug("Transferred {} outbound client messages.", count);

    count = 0;
    while (!server.outboundMessages().isEmpty()) {
      client.writeInbound(server.readOutbound());
      count++;
    }
    client.flush();
    LOG.debug("Transferred {} outbound server messages.", count);
  }

  /**
   * Creates a client connection between the server and a client.
   *
   * @return two-tuple (server rpc, client rpc)
   */
  private Rpc[] createRpcConnection(RpcServer server) throws Exception {
    return createRpcConnection(server, emptyConfig, null);
  }

  private Rpc[] createRpcConnection(RpcServer server, Map<String, String> clientConf,
      String clientId) throws Exception {
    if (clientId == null) {
      clientId = "client";
    }
    String secret = server.createSecret();
    Future<Rpc> serverRpcFuture = server.registerClient(clientId, secret, new TestDispatcher());
    NioEventLoopGroup eloop = new NioEventLoopGroup();
    Future<Rpc> clientRpcFuture = Rpc.createClient(clientConf, eloop,
        "localhost", server.getPort(), clientId, secret, new TestDispatcher());

    Rpc serverRpc = autoClose(serverRpcFuture.get(10, TimeUnit.SECONDS));
    Rpc clientRpc = autoClose(clientRpcFuture.get(10, TimeUnit.SECONDS));
    return new Rpc[]{serverRpc, clientRpc};
  }

  private static class TestMessage {

    final String message;

    public TestMessage() {
      this(null);
    }

    public TestMessage(String message) {
      this.message = message;
    }

  }

  private static class ErrorCall {

    final String error;

    public ErrorCall() {
      this(null);
    }

    public ErrorCall(String error) {
      this.error = error;
    }

  }

  private static class NotDeserializable {

    NotDeserializable(int unused) {

    }

  }

  private static class TestDispatcher extends RpcDispatcher {
    protected TestMessage handle(ChannelHandlerContext ctx, TestMessage msg) {
      return msg;
    }

    protected void handle(ChannelHandlerContext ctx, ErrorCall msg) {
      throw new IllegalArgumentException(msg.error);
    }

    protected void handle(ChannelHandlerContext ctx, NotDeserializable msg) {
      // No op. Shouldn't actually be called, if it is, the test will fail.
    }

  }
}
