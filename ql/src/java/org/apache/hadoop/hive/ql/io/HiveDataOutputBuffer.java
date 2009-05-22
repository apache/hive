/**
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
package org.apache.hadoop.hive.ql.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A thread-not-safe version of Hadoop's DataOutputBuffer, which removes all
 * synchronized modifiers.
 */
public class HiveDataOutputBuffer extends DataOutputStream {

  private static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() {
      return buf;
    }

    public int getLength() {
      return count;
    }

    public void reset() {
      count = 0;
    }

    public void write(DataInput in, int length) throws IOException {
      enLargeBuffer(length);
      in.readFully(buf, count, length);
      count += length;
    }

    public void write(int b) {
      enLargeBuffer(1);
      buf[count] = (byte) b;
      count += 1;
    }

    private int enLargeBuffer(int increment) {
      int temp = count + increment;
      int newLen = temp;
      if (temp > buf.length) {
        if ((buf.length << 1) > temp)
          newLen = buf.length << 1;
        byte newbuf[] = new byte[newLen];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      return newLen;
    }

    public void write(byte b[], int off, int len) {
      if ((off < 0) || (off > b.length) || (len < 0)
          || ((off + len) > b.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return;
      }
      enLargeBuffer(len);
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    public void writeTo(OutputStream out) throws IOException {
      out.write(buf, 0, count);
    }
  }

  private Buffer buffer;

  /** Constructs a new empty buffer. */
  public HiveDataOutputBuffer() {
    this(new Buffer());
  }

  private HiveDataOutputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /**
   * Returns the current contents of the buffer. Data is only valid to
   * {@link #getLength()}.
   */
  public byte[] getData() {
    return buffer.getData();
  }

  /** Returns the length of the valid data currently in the buffer. */
  public int getLength() {
    return buffer.getLength();
  }

  /** Resets the buffer to empty. */
  public HiveDataOutputBuffer reset() {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  public void write(int b) throws IOException {
    buffer.write(b);
    incCount(1);
  }

  public void write(byte b[], int off, int len) throws IOException {
    buffer.write(b, off, len);
    incCount(len);
  }

  private void incCount(int value) {
    if (written + value < 0) {
      written = Integer.MAX_VALUE;
    } else
      written += value;
  }
}
