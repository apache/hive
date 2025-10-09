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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
  * Transport that simply wraps another transport.
  * This is the equivalent of FilterInputStream for Thrift transports.
  */
 public class TFilterTransport extends TTransport {
   protected final TTransport wrapped;

   public TFilterTransport(TTransport wrapped) {
     this.wrapped = wrapped;
   }

   @Override
   public void open() throws TTransportException {
     wrapped.open();
   }

   @Override
   public boolean isOpen() {
     return wrapped.isOpen();
   }

   @Override
   public boolean peek() {
     return wrapped.peek();
   }

   @Override
   public void close() {
     wrapped.close();
   }

   @Override
   public int read(byte[] buf, int off, int len) throws TTransportException {
     return wrapped.read(buf, off, len);
   }

   @Override
   public int readAll(byte[] buf, int off, int len) throws TTransportException {
     return wrapped.readAll(buf, off, len);
   }

   @Override
   public void write(byte[] buf) throws TTransportException {
     wrapped.write(buf);
   }

   @Override
   public void write(byte[] buf, int off, int len) throws TTransportException {
     wrapped.write(buf, off, len);
   }

   @Override
   public void flush() throws TTransportException {
     wrapped.flush();
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
   public TConfiguration getConfiguration() {
     return wrapped.getConfiguration();
   }

   @Override
   public void updateKnownMessageSize(long l) throws TTransportException {
     wrapped.updateKnownMessageSize(l);
   }

   @Override
   public void checkReadBytesAvailable(long l) throws TTransportException {
     wrapped.checkReadBytesAvailable(l);
   }
}
