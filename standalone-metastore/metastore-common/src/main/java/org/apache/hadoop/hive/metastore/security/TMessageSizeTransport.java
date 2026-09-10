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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.security;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TEndpointTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Transport wrapper that enforces a cumulative max-message-size limit on reads.
 *
 * Thrift 0.24 removed countConsumedMessageBytes() from TMemoryInputTransport.read(), which
 * broke per-message size enforcement for SASL transports: TSaslTransport buffers each
 * response frame into a TMemoryInputTransport, and in 0.23 the cumulative counter was
 * decremented on every read — throwing TTransportException once the limit was exceeded. In
 * 0.24 the counter silently saturates at 0 with no exception.
 *
 * This wrapper reinstates cumulative tracking by extending TEndpointTransport (which owns
 * remainingMessageSize) and calling countConsumedMessageBytes() on each read. flush() resets
 * the counter for the next outgoing message, mirroring TIOStreamTransport behaviour.
 */
public class TMessageSizeTransport extends TEndpointTransport {

  private final TTransport wrapped;

  public TMessageSizeTransport(TTransport wrapped) throws TTransportException {
    super(wrapped.getConfiguration() != null ? wrapped.getConfiguration() : new TConfiguration());
    this.wrapped = wrapped;
  }

  public TTransport getWrapped() {
    return wrapped;
  }

  @Override
  public boolean isOpen() {
    return wrapped.isOpen();
  }

  @Override
  public void open() throws TTransportException {
    wrapped.open();
  }

  @Override
  public void close() {
    wrapped.close();
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int n = wrapped.read(buf, off, len);
    if (n > 0) {
      countConsumedMessageBytes(n);
    }
    return n;
  }

  /**
   * Upfront check + read loop that calls wrapped.read() directly so bytes are counted
   * exactly once (via the final countConsumedMessageBytes call, not via this.read()).
   */
  @Override
  public int readAll(byte[] buf, int off, int len) throws TTransportException {
    checkReadBytesAvailable(len);
    int got = 0;
    int ret;
    while (got < len) {
      ret = wrapped.read(buf, off + got, len - got);
      if (ret <= 0) {
        throw new TTransportException(
            "Cannot read. Remote side has closed. Tried to read "
                + len + " bytes, but only got " + got + " bytes.");
      }
      got += ret;
    }
    countConsumedMessageBytes(got);
    return got;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    wrapped.write(buf, off, len);
  }

  /** Reset the per-message counter after each flush so the next response starts fresh. */
  @Override
  public void flush() throws TTransportException {
    wrapped.flush();
    resetConsumedMessageSize(-1);
  }

  @Override
  public byte[] getBuffer() {
    return wrapped.getBuffer();
  }

  @Override
  public int getBufferPosition() {
    return wrapped.getBufferPosition();
  }

  @Override
  public int getBytesRemainingInBuffer() {
    return wrapped.getBytesRemainingInBuffer();
  }

  @Override
  public void consumeBuffer(int len) {
    wrapped.consumeBuffer(len);
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {
    wrapped.updateKnownMessageSize(size);
    resetConsumedMessageSize(size < 0 ? -1 : size);
  }
}
