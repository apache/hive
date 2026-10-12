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
package org.apache.hadoop.hive.metastore.security;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class TestTMessageSizeTransport {

  private static TMessageSizeTransport newTransport(int maxMessageSize, byte[] data) throws TTransportException {
    TConfiguration conf = new TConfiguration();
    conf.setMaxMessageSize(Math.max(maxMessageSize, data.length));
    TMemoryInputTransport wrapped = new TMemoryInputTransport(conf, data);
    conf.setMaxMessageSize(maxMessageSize);

    return new TMessageSizeTransport(wrapped);
  }

  @Test
  public void testReadEnforcesMaxMessageSize() throws Exception {
    TMessageSizeTransport transport = newTransport(4, new byte[10]);
    byte[] buf = new byte[10];

    // Within the limit: no exception.
    transport.read(buf, 0, 4);

    // One more byte exceeds the configured max message size.
    try {
      transport.read(buf, 4, 1);
      Assert.fail("Expected TTransportException once the message size limit was exceeded");
    } catch (TTransportException e) {
      Assert.assertEquals(TTransportException.MESSAGE_SIZE_LIMIT, e.getType());
    }
  }

  @Test
  public void testConsumeBufferEnforcesMaxMessageSize() throws Exception {
    TMessageSizeTransport transport = newTransport(4, new byte[10]);

    // Within the limit: no exception.
    transport.consumeBuffer(4);

    // One more byte exceeds the configured max message size, same as the read() path.
    try {
      transport.consumeBuffer(1);
      Assert.fail("Expected an exception once the message size limit was exceeded via consumeBuffer");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof TTransportException);
      Assert.assertEquals(TTransportException.MESSAGE_SIZE_LIMIT,
          ((TTransportException) e.getCause()).getType());
    }
  }

  @Test
  public void testConsumeBufferForwardsToWrappedTransport() throws Exception {
    TConfiguration conf = new TConfiguration();
    conf.setMaxMessageSize(100);
    TMemoryInputTransport wrapped = new TMemoryInputTransport(conf, new byte[10]);
    TMessageSizeTransport transport = new TMessageSizeTransport(wrapped);

    transport.consumeBuffer(3);

    Assert.assertEquals(3, wrapped.getBufferPosition());
    Assert.assertEquals(7, transport.getBytesRemainingInBuffer());
  }
}
