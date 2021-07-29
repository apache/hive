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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;


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

  // Calls to ensureValPreallocated() ensure that currentValue and currentOffset
  // are set to enough space for the value.
  private byte[] currentValue;   // bytes for the next value
  private int currentOffset;    // starting position in the current buffer

  // A shared static buffer allocation that we use for the small values
  private byte[] sharedBuffer;
  // The next unused offset in the sharedBuffer.
  private int sharedBufferOffset;

  private int bufferAllocationCount;

  // Estimate that there will be 16 bytes per entry
  static final int DEFAULT_BUFFER_SIZE = 16 * VectorizedRowBatch.DEFAULT_SIZE;

  // Proportion of extra space to provide when allocating more buffer space.
  static final float EXTRA_SPACE_FACTOR = (float) 1.2;

  // Largest item size allowed in sharedBuffer
  static final int MAX_SIZE_FOR_SMALL_ITEM = 1024 * 1024;

  // Largest size allowed for sharedBuffer
  static final int MAX_SIZE_FOR_SHARED_BUFFER = 1024 * 1024 * 1024;

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
    super(Type.BYTES, size);
    vector = new byte[size][];
    start = new int[size];
    length = new int[size];
  }

  /**
   * Additional reset work for BytesColumnVector (releasing scratch bytes for by value strings).
   */
  @Override
  public void reset() {
    super.reset();
    initBuffer(0);
  }

  /**
   * Set a field by reference.
   *
   * This is a FAST version that assumes the caller has checked to make sure the sourceBuf
   * is not null and elementNum is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.  Only the output entry fields will be set by this method.
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
   * @param estimatedValueSize  Estimated size of buffer space needed per row
   */
  public void initBuffer(int estimatedValueSize) {
    sharedBufferOffset = 0;

    // if buffer is already allocated, keep using it, don't re-allocate
    if (sharedBuffer != null) {
      // Free up any previously allocated buffers that are referenced by vector
      if (bufferAllocationCount > 0) {
        for (int idx = 0; idx < vector.length; ++idx) {
          vector[idx] = null;
        }
      }
    } else {
      // allocate a little extra space to limit need to re-allocate
      long bufferSize = (long) (this.vector.length * estimatedValueSize * EXTRA_SPACE_FACTOR);
      if (bufferSize < DEFAULT_BUFFER_SIZE) {
        bufferSize = DEFAULT_BUFFER_SIZE;
      }
      if (bufferSize > MAX_SIZE_FOR_SHARED_BUFFER) {
        bufferSize = MAX_SIZE_FOR_SHARED_BUFFER;
      }
      sharedBuffer = new byte[(int) bufferSize];
    }
    bufferAllocationCount = 0;
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
    return sharedBuffer == null ? 0 : sharedBuffer.length;
  }

  /**
   * Set a field by actually copying in to a local buffer.
   * If you must actually copy data in to the array, use this method.
   * DO NOT USE this method unless it's not practical to set data by reference with setRef().
   * Setting data by reference tends to run a lot faster than copying data in.
   *
   * This is a FAST version that assumes the caller has checked to make sure the sourceBuf
   * is not null and elementNum is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.  Only the output entry fields will be set by this method.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   * @param start start byte position within source
   * @param length  length of source byte sequence
   */
  public void setVal(int elementNum, byte[] sourceBuf, int start, int length) {
    ensureValPreallocated(length);
    if (length > 0) {
      System.arraycopy(sourceBuf, start, currentValue, currentOffset, length);
    }
    setValPreallocated(elementNum, length);
  }

  /**
   * Set a field by actually copying in to a local buffer.
   * If you must actually copy data in to the array, use this method.
   * DO NOT USE this method unless it's not practical to set data by reference with setRef().
   * Setting data by reference tends to run a lot faster than copying data in.
   *
   * This is a FAST version that assumes the caller has checked to make sure the sourceBuf
   * is not null and elementNum is correctly adjusted for isRepeating.  And, that the isNull entry
   * has been set.  Only the output entry fields will be set by this method.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   */
  public void setVal(int elementNum, byte[] sourceBuf) {
    setVal(elementNum, sourceBuf, 0, sourceBuf.length);
  }

  /**
   * Ensures that we have space allocated for the next value, which has size
   * length bytes.
   *
   * Updates currentValue and currentOffset for this value.
   *
   * Always use before getValPreallocatedBytes, getValPreallocatedStart.
   * setValPreallocated must be called to actually reserve the bytes.
   */
  public void ensureValPreallocated(int length) {
    if ((sharedBufferOffset + length) > sharedBuffer.length) {
      // sets currentValue and currentOffset
      allocateBuffer(length);
    } else {
      currentValue = sharedBuffer;
      currentOffset = sharedBufferOffset;
    }
  }

  public byte[] getValPreallocatedBytes() {
    return currentValue;
  }

  public int getValPreallocatedStart() {
    return currentOffset;
  }

  /**
   * Set the length of the preallocated values bytes used.
   * @param elementNum
   * @param length
   */
  public void setValPreallocated(int elementNum, int length) {
    vector[elementNum] = currentValue;
    this.start[elementNum] = currentOffset;
    this.length[elementNum] = length;
    // If the current value is the shared buffer, move the next offset forward.
    if (currentValue == sharedBuffer) {
      sharedBufferOffset += length;
    }
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
    ensureValPreallocated(newLen);
    setValPreallocated(elementNum, newLen);

    System.arraycopy(leftSourceBuf, leftStart, currentValue, currentOffset, leftLen);
    System.arraycopy(rightSourceBuf, rightStart, currentValue,
        currentOffset + leftLen, rightLen);
  }

  /**
   * Allocate/reuse enough buffer space to accommodate next element.
   * Sets currentValue and currentOffset to the correct buffer and offset.
   *
   * This uses an exponential increase mechanism to rapidly
   * increase buffer size to enough to hold all data.
   * As batches get re-loaded, buffer space allocated will quickly
   * stabilize.
   *
   * @param nextElemLength size of next element to be added
   */
  private void allocateBuffer(int nextElemLength) {
    // If this is a large value or shared buffer is maxed out, allocate a
    // single use buffer. Assumes that sharedBuffer length and
    // MAX_SIZE_FOR_SHARED_BUFFER are powers of 2.
    if (nextElemLength > MAX_SIZE_FOR_SMALL_ITEM ||
        sharedBufferOffset + nextElemLength >= MAX_SIZE_FOR_SHARED_BUFFER) {
      // allocate a value for the next value
      ++bufferAllocationCount;
      currentValue = new byte[nextElemLength];
      currentOffset = 0;
    } else {

      // sharedBuffer might still be out of space
      if ((sharedBufferOffset + nextElemLength) > sharedBuffer.length) {
        int newLength = sharedBuffer.length * 2;
        while (newLength < nextElemLength) {
          newLength *= 2;
        }
        sharedBuffer = new byte[newLength];
        ++bufferAllocationCount;
        sharedBufferOffset = 0;
      }
      currentValue = sharedBuffer;
      currentOffset = sharedBufferOffset;
    }
  }

  /** Copy the current object contents into the output. Only copy selected entries,
    * as indicated by selectedInUse and the sel array.
    */
  @Override
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, ColumnVector outputColVector) {

    BytesColumnVector output = (BytesColumnVector) outputColVector;
    boolean[] outputIsNull = output.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        outputIsNull[0] = false;
        output.setVal(0, vector[0], start[0], length[0]);
      } else {
        outputIsNull[0] = true;
        output.noNulls = false;
      }
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    if (noNulls) {
      if (selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != size; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           output.setVal(i, vector[i], start[i], length[i]);
         }
        } else {
          for(int j = 0; j != size; j++) {
            final int i = sel[j];
            output.setVal(i, vector[i], start[i], length[i]);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != size; i++) {
          output.setVal(i, vector[i], start[i], length[i]);
        }
      }
    } else /* there are nulls in our column */ {

      // Carefully handle NULLs...

      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          if (!isNull[i]) {
            output.isNull[i] = false;
            output.setVal(i, vector[i], start[i], length[i]);
          } else {
            output.isNull[i] = true;
            output.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          if (!isNull[i]) {
            output.isNull[i] = false;
            output.setVal(i, vector[i], start[i], length[i]);
          } else {
            output.isNull[i] = true;
            output.noNulls = false;
          }
        }
      }
    }
  }

  /** Simplify vector by brute-force flattening noNulls and isRepeating
    * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
    * with many arguments, at the expense of loss of some performance.
    */
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;

      // setRef is used below and this is safe, because the reference
      // is to data owned by this column vector. If this column vector
      // gets re-used, the whole thing is re-used together so there
      // is no danger of a dangling reference.

      // Only copy data values if entry is not null. The string value
      // at position 0 is undefined if the position 0 value is null.
      if (noNulls || !isNull[0]) {

        if (selectedInUse) {
          for (int j = 0; j < size; j++) {
            int i = sel[j];
            this.setRef(i, vector[0], start[0], length[0]);
          }
        } else {
          for (int i = 0; i < size; i++) {
            this.setRef(i, vector[0], start[0], length[0]);
          }
        }
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  // Fill the all the vector entries with provided value
  public void fill(byte[] value) {
    isRepeating = true;
    isNull[0] = false;
    setVal(0, value, 0, value.length);
  }

  // Fill the column vector with nulls
  public void fillWithNulls() {
    noNulls = false;
    isRepeating = true;
    vector[0] = null;
    isNull[0] = true;
  }

  /**
   * Set the element in this column vector from the given input vector.
   *
   * The inputElementNum will be adjusted to 0 if the input column has isRepeating set.
   *
   * On the other hand, the outElementNum must have been adjusted to 0 in ADVANCE when the output
   * has isRepeating set.
   *
   * IMPORTANT: if the output entry is marked as NULL, this method will do NOTHING.  This
   * supports the caller to do output NULL processing in advance that may cause the output results
   * operation to be ignored.  Thus, make sure the output isNull entry is set in ADVANCE.
   *
   * The inputColVector noNulls and isNull entry will be examined.  The output will only
   * be set if the input is NOT NULL.  I.e. noNulls || !isNull[inputElementNum] where
   * inputElementNum may have been adjusted to 0 for isRepeating.
   *
   * If the input entry is NULL or out-of-range, the output will be marked as NULL.
   * I.e. set output noNull = false and isNull[outElementNum] = true.  An example of out-of-range
   * is the DecimalColumnVector which can find the input decimal does not fit in the output
   * precision/scale.
   *
   * (Since we return immediately if the output entry is NULL, we have no need and do not mark
   * the output entry to NOT NULL).
   *
   */
  @Override
  public void setElement(int outputElementNum, int inputElementNum, ColumnVector inputColVector) {

    // Invariants.
    if (isRepeating && outputElementNum != 0) {
      throw new AssertionError("Output column number expected to be 0 when isRepeating");
    }
    if (inputColVector.isRepeating) {
      inputElementNum = 0;
    }

    // Do NOTHING if output is NULL.
    if (!noNulls && isNull[outputElementNum]) {
      return;
    }

    if (inputColVector.noNulls || !inputColVector.isNull[inputElementNum]) {
      BytesColumnVector in = (BytesColumnVector) inputColVector;
      setVal(outputElementNum, in.vector[inputElementNum],
          in.start[inputElementNum], in.length[inputElementNum]);
    } else {

      // Only mark output NULL when input is NULL.
      isNull[outputElementNum] = true;
      noNulls = false;
    }
  }

  @Override
  public void init() {
    initBuffer(0);
  }

  public String toString(int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      return new String(vector[row], start[row], length[row]);
    } else {
      return null;
    }
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      buffer.append('"');
      buffer.append(new String(vector[row], start[row], length[row]));
      buffer.append('"');
    } else {
      buffer.append("null");
    }
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size > vector.length) {
      int[] oldStart = start;
      start = new int[size];
      int[] oldLength = length;
      length = new int[size];
      byte[][] oldVector = vector;
      vector = new byte[size][];
      if (preserveData) {
        if (isRepeating) {
          vector[0] = oldVector[0];
          start[0] = oldStart[0];
          length[0] = oldLength[0];
        } else {
          System.arraycopy(oldVector, 0, vector, 0, oldVector.length);
          System.arraycopy(oldStart, 0, start, 0 , oldStart.length);
          System.arraycopy(oldLength, 0, length, 0, oldLength.length);
        }
      }
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    BytesColumnVector other = (BytesColumnVector)otherCv;
    super.shallowCopyTo(other);
    other.currentOffset = currentOffset;
    other.vector = vector;
    other.start = start;
    other.length = length;
    other.currentValue = currentValue;
    other.sharedBuffer = sharedBuffer;
    other.sharedBufferOffset = sharedBufferOffset;
  }
}
