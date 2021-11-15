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

package org.apache.hadoop.hive.conf;

import org.apache.hive.common.util.SuppressFBWarnings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * LoopingByteArrayInputStream.
 *
 * This was designed specifically to handle the problem in Hadoop's Configuration object that it
 * tries to read the entire contents of the same InputStream repeatedly without resetting it.
 *
 * The Configuration object does attempt to close the InputStream though, so, since close does
 * nothing for the ByteArrayInputStream object, override it to reset it.
 *
 * It also uses a thread local ByteArrayInputStream for method calls.  This is because
 * Configuration's copy constructor does a shallow copy of the resources, meaning that when the
 * copy constructor is used to generate HiveConfs in different threads, they all share the same
 * LoopingByteArrayInputStream.  ByteArrayInputStreams are not thread safe in such situations.
 */
public class LoopingByteArrayInputStream extends InputStream {

  private final byte[] buf;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "Intended")
  public LoopingByteArrayInputStream(byte[] buf) {
    this.buf = buf;
  }

  private final ThreadLocal<ByteArrayInputStream> threadLocalByteArrayInputStream =
      new ThreadLocal<ByteArrayInputStream>() {
        @Override
        protected ByteArrayInputStream initialValue() {
          return null;
        }
      };

  private ByteArrayInputStream getByteArrayInputStream() {
    ByteArrayInputStream bais = threadLocalByteArrayInputStream.get();
    if (bais == null) {
      bais = new ByteArrayInputStream(buf);
      threadLocalByteArrayInputStream.set(bais);
    }
    return bais;
  }

  @Override
  public synchronized int available() {
    return getByteArrayInputStream().available();
  }

  @Override
  public void mark(int arg0) {
    getByteArrayInputStream().mark(arg0);
  }

  @Override
  public boolean markSupported() {
    return getByteArrayInputStream().markSupported();
  }

  @Override
  public synchronized int read() {
    return getByteArrayInputStream().read();
  }

  @Override
  public synchronized int read(byte[] arg0, int arg1, int arg2) {
    return getByteArrayInputStream().read(arg0, arg1, arg2);
  }

  @Override
  public synchronized void reset() {
    getByteArrayInputStream().reset();
  }

  @Override
  public synchronized long skip(long arg0) {
    return getByteArrayInputStream().skip(arg0);
  }

  @Override
  public int read(byte[] arg0) throws IOException {
    return getByteArrayInputStream().read(arg0);
  }

  @Override
  public void close() throws IOException {
    getByteArrayInputStream().reset();
    // According to the Java documentation this does nothing, but just in case
    getByteArrayInputStream().close();
  }

}
