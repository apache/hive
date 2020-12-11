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
package org.apache.hadoop.hive.common.io;

import org.apache.hive.common.util.SuppressFBWarnings;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A thread-not-safe version of ByteArrayOutputStream, which removes all
 * synchronized modifiers.
 */
public class NonSyncByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * The maximum size of array to allocate.
   * Some VMs reserve some header words in an array.
   * Attempts to allocate larger arrays may result in
   * OutOfMemoryError: Requested array size exceeds VM limit
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  public NonSyncByteArrayOutputStream(int size) {
    super(size);
  }

  public NonSyncByteArrayOutputStream() {
    super();
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Ref external obj for efficiency")
  public byte[] getData() {
    return buf;
  }

  public int getLength() {
    return count;
  }

  public void setWritePosition(int writePosition) {
    count = writePosition;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() {
    count = 0;
  }

  public void write(DataInput in, int length) throws IOException {
    enLargeBuffer(length);
    in.readFully(buf, count, length);
    count += length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(int b) {
    enLargeBuffer(1);
    buf[count] = (byte) b;
    count += 1;
  }

  private void enLargeBuffer(final int increment) {
    final int requestCapacity = Math.addExact(count, increment);
    final int currentCapacity = buf.length;

    if (requestCapacity > currentCapacity) {
      // Increase size by a factor of 1.5x
      int newCapacity = currentCapacity + (currentCapacity >> 1);

      // Check for overflow scenarios
      if (newCapacity < 0 || newCapacity > MAX_ARRAY_SIZE) {
        newCapacity = MAX_ARRAY_SIZE;
      } else if (newCapacity < requestCapacity) {
        newCapacity = requestCapacity;
      }
      buf = Arrays.copyOf(buf, newCapacity);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }
    enLargeBuffer(len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }
}
