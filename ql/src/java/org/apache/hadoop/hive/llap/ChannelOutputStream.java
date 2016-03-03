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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OutputStream to write to the Netty Channel
 */
public class ChannelOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelOutputStream.class);

  private ChannelHandlerContext chc;
  private int bufSize;
  private String id;
  private ByteBuf buf;
  private byte[] singleByte = new byte[1];
  private boolean closed = false;

  private ChannelFutureListener listener = new ChannelFutureListener() {
    @Override
    public void operationComplete(ChannelFuture future) {
      if (future.isCancelled()) {
        LOG.error(id + " was cancelled");
      } else if (!future.isSuccess()) {
        LOG.error("Error on ID " + id, future.cause());
      }
    }
  };

  public ChannelOutputStream(ChannelHandlerContext chc, String id, int bufSize) {
    this.chc = chc;
    this.id = id;
    this.bufSize = bufSize;
    this.buf = chc.alloc().buffer(bufSize);
  }

  @Override
  public void write(int b) throws IOException {
    singleByte[0] = (byte) b;
    write(singleByte, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int currentOffset = off;
    int bytesRemaining = len;

    while (bytesRemaining + buf.readableBytes() > bufSize) {
      int iterationLen = bufSize - buf.readableBytes();
      writeInternal(b, currentOffset, iterationLen);
      currentOffset += iterationLen;
      bytesRemaining -= iterationLen;
    }

    if (bytesRemaining > 0) {
      writeInternal(b, currentOffset, bytesRemaining);
    }
  }

  @Override
  public void flush() throws IOException {
    if (buf.isReadable()) {
      writeToChannel();
    }
    chc.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      throw new IOException("Already closed: " + id);
    }

    try {
      flush();
    } catch (IOException err) {
      LOG.error("Error flushing stream before close", err);
    }

    try {
      chc.close().addListener(listener).sync();
    } catch (InterruptedException err) {
      throw new IOException(err);
    } finally {
      buf.release();
      buf = null;
      chc = null;
      closed = true;
    }
  }

  private void writeToChannel() throws IOException {
    if (closed) {
      throw new IOException("Already closed: " + id);
    }

    chc.write(buf.copy()).addListener(listener);
    buf.clear();
  }

  private void writeInternal(byte[] b, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Already closed: " + id);
    }

    buf.writeBytes(b, off, len);
    if (buf.readableBytes() >= bufSize) {
      writeToChannel();
    }
  }
}
