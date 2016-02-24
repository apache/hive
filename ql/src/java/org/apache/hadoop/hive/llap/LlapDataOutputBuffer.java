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
package org.apache.hadoop.hive.ql.llap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
/**
 * A thread-not-safe version of Hadoop's DataOutputBuffer, which removes all
 * synchronized modifiers.
 */
public class LlapDataOutputBuffer implements DataOutput {

  int readOffset;
  int writeOffset;
  byte[] buffer;

  /** Constructs a new empty buffer. */
  public LlapDataOutputBuffer(int length) {
    buffer = new byte[length];
    reset();
  }

  /**
   * Returns the current contents of the buffer. Data is only valid to
   * {@link #getLength()}.
   */
  public byte[] getData() {
    return buffer;
  }

  /** Returns the length of the valid data currently in the buffer. */
  public int getLength() {
    return (writeOffset - readOffset) % buffer.length;
  }

  /** Resets the buffer to empty. */
  public LlapDataOutputBuffer reset() {
    readOffset = 0;
    writeOffset = 0;
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(DataInput in, int length) throws IOException {
    //
  }

  @Override
  public synchronized void write(int b) throws IOException {
    while (readOffset == writeOffset) {
      try {
	wait();
      } catch(InterruptedException e) {
      }
    }
    buffer[writeOffset] = (byte)b;
    writeOffset = (writeOffset + 1) % buffer.length;
    notify();
  }

  public synchronized int read() throws IOException {
    while (readOffset == writeOffset) {
      try {
	wait();
      } catch(InterruptedException e) {
      }
    }
    int b = buffer[readOffset];
    readOffset = (readOffset + 1) % buffer.length;
    notify();
    return b;
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    while(len-- != 0) {
      write(b[off++]);
    }
  }

  @Override
  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }


  @Override
  public void writeBoolean(boolean v) throws IOException {
    write(v?1:0);
  }

  @Override
  public void writeByte(int v) throws IOException  {
    write(v);
  }

  @Override
  public void writeChar(int v) throws IOException  {
    write(v);
  }

  @Override
  public void writeBytes(String v) throws IOException  {
    write(v.getBytes(), 0, v.length());
  }

  @Override
  public void writeChars(String v) throws IOException  {
    write(v.getBytes(), 0, v.length());
  }

  @Override
  public void writeDouble(double v) throws IOException  {
    write(ByteBuffer.allocate(8).putDouble(v).array(),0,8);
  }

  @Override
  public void writeFloat(float v) throws IOException  {
    write(ByteBuffer.allocate(4).putFloat(v).array(),0,4);
  }

  @Override
  public void writeInt(int v) throws IOException  {
    write(v);
    write(v>>>8);
    write(v>>>16);
    write(v>>>24);
  }

  @Override
  public void writeLong(long v) throws IOException  {
    int v1 = (int)v;
    int v2 = (int)v>>>32;
    write(v1);
    write(v2);
  }

  @Override
  public void writeShort(int v) throws IOException  {
    write(v);
    write(v>>>8);
  }

  @Override
  public void writeUTF(String v) throws IOException  {
    write(v.getBytes(), 0, v.length());
  }
}
