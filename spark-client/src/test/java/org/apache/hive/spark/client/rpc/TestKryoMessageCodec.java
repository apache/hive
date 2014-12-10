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

import java.util.List;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.logging.LoggingHandler;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestKryoMessageCodec {

  @Test
  public void testKryoCodec() throws Exception {
    ByteBuf buf = newBuffer();
    Object message = "Hello World!";

    KryoMessageCodec codec = new KryoMessageCodec(0);
    codec.encode(null, message, buf);

    List<Object> objects = Lists.newArrayList();
    codec.decode(null, buf, objects);

    assertEquals(1, objects.size());
    assertEquals(message, objects.get(0));
  }

  @Test
  public void testFragmentation() throws Exception {
    ByteBuf buf = newBuffer();
    Object[] messages = { "msg1", "msg2" };
    int[] indices = new int[messages.length];

    KryoMessageCodec codec = new KryoMessageCodec(0);

    for (int i = 0; i < messages.length; i++) {
      codec.encode(null, messages[i], buf);
      indices[i] = buf.writerIndex();
    }

    List<Object> objects = Lists.newArrayList();

    // Don't read enough data for the first message to be decoded.
    codec.decode(null, buf.slice(0, indices[0] - 1), objects);
    assertEquals(0, objects.size());

    // Read enough data for just the first message to be decoded.
    codec.decode(null, buf.slice(0, indices[0] + 1), objects);
    assertEquals(1, objects.size());
  }

  @Test
  public void testEmbeddedChannel() throws Exception {
    Object message = "Hello World!";
    EmbeddedChannel c = new EmbeddedChannel(
      new LoggingHandler(getClass()),
      new KryoMessageCodec(0));
    c.writeAndFlush(message);
    assertEquals(1, c.outboundMessages().size());
    assertFalse(message.getClass().equals(c.outboundMessages().peek().getClass()));
    c.writeInbound(c.readOutbound());
    assertEquals(1, c.inboundMessages().size());
    assertEquals(message, c.readInbound());
    c.close();
  }

  @Test
  public void testAutoRegistration() throws Exception {
    KryoMessageCodec codec = new KryoMessageCodec(0, TestMessage.class);
    ByteBuf buf = newBuffer();
    codec.encode(null, new TestMessage(), buf);

    List<Object> out = Lists.newArrayList();
    codec.decode(null, buf, out);

    assertEquals(1, out.size());
    assertTrue(out.get(0) instanceof TestMessage);
  }

  @Test
  public void testMaxMessageSize() throws Exception {
    KryoMessageCodec codec = new KryoMessageCodec(1024);
    ByteBuf buf = newBuffer();
    codec.encode(null, new TestMessage(new byte[512]), buf);

    try {
      codec.encode(null, new TestMessage(new byte[1025]), buf);
      fail("Should have failed to encode large message.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().indexOf("maximum allowed size") > 0);
    }

    KryoMessageCodec unlimited = new KryoMessageCodec(0);
    buf = newBuffer();
    unlimited.encode(null, new TestMessage(new byte[1025]), buf);

    try {
      List<Object> out = Lists.newArrayList();
      codec.decode(null, buf, out);
      fail("Should have failed to decode large message.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().indexOf("maximum allowed size") > 0);
    }
  }

  @Test
  public void testNegativeMessageSize() throws Exception {
    KryoMessageCodec codec = new KryoMessageCodec(1024);
    ByteBuf buf = newBuffer();
    buf.writeInt(-1);

    try {
      List<Object> out = Lists.newArrayList();
      codec.decode(null, buf, out);
      fail("Should have failed to decode message with negative size.");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().indexOf("must be positive") > 0);
    }
  }

  private ByteBuf newBuffer() {
    return UnpooledByteBufAllocator.DEFAULT.buffer(1024);
  }

  private static class TestMessage {
    byte[] data;

    TestMessage() {
      this(null);
    }

    TestMessage(byte[] data) {
      this.data = data;
    }
  }

}
