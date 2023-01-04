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

package org.apache.hadoop.hive.llap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Semaphore;
import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * Provides an adapter between {@link java.nio.channels.WritableByteChannel}
 * and {@link io.netty.channel.ChannelHandlerContext}.
 * Additionally provides a form of flow-control by limiting the number of
 * queued async writes.
 */
public class WritableByteChannelAdapter implements WritableByteChannel {

  private static final Logger LOG = LoggerFactory.getLogger(WritableByteChannelAdapter.class);
  private ChannelHandlerContext chc;
  private final int maxPendingWrites;
  // This semaphore provides two functions:
  // 1. Forces a cap on the number of outstanding async writes to channel
  // 2. Ensures that channel isn't closed if there are any outstanding async writes
  private final Semaphore writeResources;
  private boolean closed = false;
  private final String id;
  private BufferAllocator allocator;

  private ChannelFutureListener writeListener = new ChannelFutureListener() {
    @Override
    public void operationComplete(ChannelFuture future) {
      //Async write completed
      //Up the semaphore
      writeResources.release();

      if (future.isCancelled()) {
        LOG.error("Write cancelled on ID " + id);
      } else if (!future.isSuccess()) {
        LOG.error("Write error on ID " + id, future.cause());
      }
    }
  };

  private ChannelFutureListener closeListener = new ChannelFutureListener() {
    @Override
    public void operationComplete(ChannelFuture future) {
      if (future.isCancelled()) {
        LOG.error("Close cancelled on ID " + id);
      } else if (!future.isSuccess()) {
        LOG.error("Close failed on ID " + id, future.cause());
      }
    }
  };

  public WritableByteChannelAdapter(ChannelHandlerContext chc, int maxPendingWrites, String id) {
    this.chc = chc;
    this.maxPendingWrites = maxPendingWrites;
    this.writeResources = new Semaphore(maxPendingWrites);
    this.id = id;
  }

  public void setAllocator(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int size = src.remaining();
    //Down the semaphore or block until available
    takeWriteResources(1);
    ArrowByteBufAllocator abba = new ArrowByteBufAllocator(allocator);
    ByteBuf buf = abba.buffer(size);
    buf.writeBytes(src);
    chc.writeAndFlush(buf).addListener(writeListener);
    return size;
  }

  @Override
  public boolean isOpen() {
    return chc.channel().isOpen();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      throw new IOException("Already closed: " + id);
    }

    closed = true;
    //Block until all semaphore resources are released
    //by outstanding async writes
    takeWriteResources(maxPendingWrites);

    try {
      chc.close().addListener(closeListener);
    } finally {
      chc = null;
      closed = true;
    }
  }

  private void takeWriteResources(int numResources) throws IOException {
    try {
      writeResources.acquire(numResources);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted while waiting for write resources for " + id);
    }
  }
}
