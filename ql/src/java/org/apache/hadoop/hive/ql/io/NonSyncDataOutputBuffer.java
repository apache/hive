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

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hive.common.io.NonSyncByteArrayOutputStream;

/**
 * A thread-not-safe version of Hadoop's DataOutputBuffer, which removes all
 * synchronized modifiers.
 */
public class NonSyncDataOutputBuffer extends DataOutputStream {

  private final NonSyncByteArrayOutputStream buffer;

  /** Constructs a new empty buffer. */
  public NonSyncDataOutputBuffer() {
    this(new NonSyncByteArrayOutputStream());
  }

  private NonSyncDataOutputBuffer(NonSyncByteArrayOutputStream buffer) {
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
  public NonSyncDataOutputBuffer reset() {
    written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.write(b);
    incCount(1);
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    buffer.write(b, off, len);
    incCount(len);
  }

  private void incCount(int value) {
    if (written + value < 0) {
      written = Integer.MAX_VALUE;
    } else {
      written += value;
    }
  }
}
