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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.logging.LoggingHandler;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestKryoMessageCodec {

  private static final String MESSAGE = "Hello World!";

  @Test
  public void testKryoCodec() throws Exception {
    List<Object> objects = encodeAndDecode(MESSAGE, null);
    assertEquals(1, objects.size());
    assertEquals(MESSAGE, objects.get(0));
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
    EmbeddedChannel c = new EmbeddedChannel(
      new LoggingHandler(getClass()),
      new KryoMessageCodec(0));
    c.writeAndFlush(MESSAGE);
    assertEquals(1, c.outboundMessages().size());
    assertFalse(MESSAGE.getClass().equals(c.outboundMessages().peek().getClass()));
    c.writeInbound(c.readOutbound());
    assertEquals(1, c.inboundMessages().size());
    assertEquals(MESSAGE, c.readInbound());
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

  @Test
  public void testEncryptionOnly() throws Exception {
    List<Object> objects = Collections.<Object>emptyList();
    try {
      objects = encodeAndDecode(MESSAGE, new TestEncryptionHandler(true, false));
    } catch (Exception e) {
      // Pass.
    }
    // Do this check in case the ciphertext actually makes sense in some way.
    for (Object msg : objects) {
      assertFalse(MESSAGE.equals(objects.get(0)));
    }
  }

  @Test
  public void testDecryptionOnly() throws Exception {
    List<Object> objects = Collections.<Object>emptyList();
    try {
      objects = encodeAndDecode(MESSAGE, new TestEncryptionHandler(false, true));
    } catch (Exception e) {
      // Pass.
    }
    // Do this check in case the decrypted plaintext actually makes sense in some way.
    for (Object msg : objects) {
      assertFalse(MESSAGE.equals(objects.get(0)));
    }
  }

  @Test
  public void testEncryptDecrypt() throws Exception {
    List<Object> objects = encodeAndDecode(MESSAGE, new TestEncryptionHandler(true, true));
    assertEquals(1, objects.size());
    assertEquals(MESSAGE, objects.get(0));
  }

  private List<Object> encodeAndDecode(Object message, KryoMessageCodec.EncryptionHandler eh)
      throws Exception {
    ByteBuf buf = newBuffer();
    KryoMessageCodec codec = new KryoMessageCodec(0);
    codec.setEncryptionHandler(eh);
    codec.encode(null, message, buf);

    List<Object> objects = Lists.newArrayList();
    codec.decode(null, buf, objects);
    return objects;
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

  private static class TestEncryptionHandler implements KryoMessageCodec.EncryptionHandler {

    private static final byte KEY = 0x42;

    private final boolean encrypt;
    private final boolean decrypt;

    TestEncryptionHandler(boolean encrypt, boolean decrypt) {
      this.encrypt = encrypt;
      this.decrypt = decrypt;
    }

    public byte[] wrap(byte[] data, int offset, int len) throws IOException {
      return encrypt ? transform(data, offset, len) : data;
    }

    public byte[] unwrap(byte[] data, int offset, int len) throws IOException {
      return decrypt ? transform(data, offset, len) : data;
    }

    public void dispose() throws IOException {

    }

    private byte[] transform(byte[] data, int offset, int len) {
      byte[] dest = new byte[len];
      for (int i = 0; i < len; i++) {
        dest[i] = (byte) (data[offset + i] ^ KEY);
      }
      return dest;
    }

  }

}
