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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This class supports string and binary data by value reference -- i.e. each field is
 * explicitly present, as opposed to provided by a dictionary reference.
 * In some cases, all the values will be in the same byte array to begin with,
 * but this need not be the case. If each value is in a separate byte
 * array to start with, or not all of the values are in the same original
 * byte array, you can still assign data by reference into this column vector.
 * This gives flexibility to use this in multiple situations.
 * <p>
 * When setting data by reference, the caller
 * is responsible for allocating the byte arrays used to hold the data.
 * You can also set data by value, as long as you call the initBuffer() method first.
 * You can mix "by value" and "by reference" in the same column vector,
 * though that use is probably not typical.
 */
public class BytesColumnVector extends ColumnVector {
  public byte[][] vector;
  public int[] start;          // start offset of each field

  /*
   * The length of each field. If the value repeats for every entry, then it is stored
   * in vector[0] and isRepeating from the superclass is set to true.
   */
  public int[] length;
  private byte[] buffer;   // optional buffer to use when actually copying in data
  private int nextFree;    // next free position in buffer

  // Reusable text object
  private final Text textObject = new Text();

  // Estimate that there will be 16 bytes per entry
  static final int DEFAULT_BUFFER_SIZE = 16 * VectorizedRowBatch.DEFAULT_SIZE;

  // Proportion of extra space to provide when allocating more buffer space.
  static final float EXTRA_SPACE_FACTOR = (float) 1.2;

  /**
   * Use this constructor for normal operation.
   * All column vectors should be the default size normally.
   */
  public BytesColumnVector() {
    this(VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Don't call this constructor except for testing purposes.
   *
   * @param size  number of elements in the column vector
   */
  public BytesColumnVector(int size) {
    super(size);
    vector = new byte[size][];
    start = new int[size];
    length = new int[size];
  }

  /** Set a field by reference.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   * @param start start byte position within source
   * @param length  length of source byte sequence
   */
  public void setRef(int elementNum, byte[] sourceBuf, int start, int length) {
    vector[elementNum] = sourceBuf;
    this.start[elementNum] = start;
    this.length[elementNum] = length;
  }

  /**
   * You must call initBuffer first before using setVal().
   * Provide the estimated number of bytes needed to hold
   * a full column vector worth of byte string data.
   *
   * @param estimatedValueSize  Estimated size of buffer space needed
   */
  public void initBuffer(int estimatedValueSize) {
    nextFree = 0;

    // if buffer is already allocated, keep using it, don't re-allocate
    if (buffer != null) {
      return;
    }

    // allocate a little extra space to limit need to re-allocate
    int bufferSize = this.vector.length * (int)(estimatedValueSize * EXTRA_SPACE_FACTOR);
    if (bufferSize < DEFAULT_BUFFER_SIZE) {
      bufferSize = DEFAULT_BUFFER_SIZE;
    }
    buffer = new byte[bufferSize];
  }

  /**
   * Initialize buffer to default size.
   */
  public void initBuffer() {
    initBuffer(0);
  }

  /**
   * @return amount of buffer space currently allocated
   */
  public int bufferSize() {
    if (buffer == null) {
      return 0;
    }
    return buffer.length;
  }

  /**
   * Set a field by actually copying in to a local buffer.
   * If you must actually copy data in to the array, use this method.
   * DO NOT USE this method unless it's not practical to set data by reference with setRef().
   * Setting data by reference tends to run a lot faster than copying data in.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   * @param start start byte position within source
   * @param length  length of source byte sequence
   */
  public void setVal(int elementNum, byte[] sourceBuf, int start, int length) {
    if ((nextFree + length) > buffer.length) {
      increaseBufferSpace(length);
    }
    System.arraycopy(sourceBuf, start, buffer, nextFree, length);
    vector[elementNum] = buffer;
    this.start[elementNum] = nextFree;
    this.length[elementNum] = length;
    nextFree += length;
  }

  /**
   * Set a field to the concatenation of two string values. Result data is copied
   * into the internal buffer.
   *
   * @param elementNum index within column vector to set
   * @param leftSourceBuf container of left argument
   * @param leftStart start of left argument
   * @param leftLen length of left argument
   * @param rightSourceBuf container of right argument
   * @param rightStart start of right argument
   * @param rightLen length of right arugment
   */
  public void setConcat(int elementNum, byte[] leftSourceBuf, int leftStart, int leftLen,
      byte[] rightSourceBuf, int rightStart, int rightLen) {
    int newLen = leftLen + rightLen;
    if ((nextFree + newLen) > buffer.length) {
      increaseBufferSpace(newLen);
    }
    vector[elementNum] = buffer;
    this.start[elementNum] = nextFree;
    this.length[elementNum] = newLen;

    System.arraycopy(leftSourceBuf, leftStart, buffer, nextFree, leftLen);
    nextFree += leftLen;
    System.arraycopy(rightSourceBuf, rightStart, buffer, nextFree, rightLen);
    nextFree += rightLen;
  }

  /**
   * Increase buffer space enough to accommodate next element.
   * This uses an exponential increase mechanism to rapidly
   * increase buffer size to enough to hold all data.
   * As batches get re-loaded, buffer space allocated will quickly
   * stabilize.
   *
   * @param nextElemLength size of next element to be added
   */
  public void increaseBufferSpace(int nextElemLength) {

    // Keep doubling buffer size until there will be enough space for next element.
    int newLength = 2 * buffer.length;
    while((nextFree + nextElemLength) > newLength) {
      newLength *= 2;
    }

    // Allocate new buffer, copy data to it, and set buffer to new buffer.
    byte[] newBuffer = new byte[newLength];
    System.arraycopy(buffer, 0, newBuffer, 0, nextFree);
    buffer = newBuffer;
  }

  @Override
  public Writable getWritableObject(int index) {
    if (this.isRepeating) {
      index = 0;
    }
    Writable result = null;
    if (!isNull[index] && vector[index] != null) {
      textObject.clear();
      textObject.append(vector[index], start[index], length[index]);
      result = textObject;
    } else {
      result = NullWritable.get();
    }
    return result;
  }
}
