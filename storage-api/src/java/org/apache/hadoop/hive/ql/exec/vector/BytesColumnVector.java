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

  // A call to increaseBufferSpace() or ensureValPreallocated() will ensure that buffer[] points to
  // a byte[] with sufficient space for the specified size.
  private byte[] buffer;   // optional buffer to use when actually copying in data
  private int nextFree;    // next free position in buffer

  // Hang onto a byte array for holding smaller byte values
  private byte[] smallBuffer;
  private int smallBufferNextFree;

  private int bufferAllocationCount;

  // Estimate that there will be 16 bytes per entry
  static final int DEFAULT_BUFFER_SIZE = 16 * VectorizedRowBatch.DEFAULT_SIZE;

  // Proportion of extra space to provide when allocating more buffer space.
  static final float EXTRA_SPACE_FACTOR = (float) 1.2;

  // Largest size allowed in smallBuffer
  static final int MAX_SIZE_FOR_SMALL_BUFFER = 1024 * 1024;

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
   * @param estimatedValueSize  Estimated size of buffer space needed
   */
  public void initBuffer(int estimatedValueSize) {
    nextFree = 0;
    smallBufferNextFree = 0;

    // if buffer is already allocated, keep using it, don't re-allocate
    if (buffer != null) {
      // Free up any previously allocated buffers that are referenced by vector
      if (bufferAllocationCount > 0) {
        for (int idx = 0; idx < vector.length; ++idx) {
          vector[idx] = null;
        }
        buffer = smallBuffer; // In case last row was a large bytes value
      }
    } else {
      // allocate a little extra space to limit need to re-allocate
      int bufferSize = this.vector.length * (int)(estimatedValueSize * EXTRA_SPACE_FACTOR);
      if (bufferSize < DEFAULT_BUFFER_SIZE) {
        bufferSize = DEFAULT_BUFFER_SIZE;
      }
      buffer = new byte[bufferSize];
      smallBuffer = buffer;
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
    if ((nextFree + length) > buffer.length) {
      increaseBufferSpace(length);
    }
    if (length > 0) {
      System.arraycopy(sourceBuf, start, buffer, nextFree, length);
    }
    vector[elementNum] = buffer;
    this.start[elementNum] = nextFree;
    this.length[elementNum] = length;
    nextFree += length;
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
   * Preallocate space in the local buffer so the caller can fill in the value bytes themselves.
   *
   * Always use with getValPreallocatedBytes, getValPreallocatedStart, and setValPreallocated.
   */
  public void ensureValPreallocated(int length) {
    if ((nextFree + length) > buffer.length) {
      increaseBufferSpace(length);
    }
  }

  public byte[] getValPreallocatedBytes() {
    return buffer;
  }

  public int getValPreallocatedStart() {
    return nextFree;
  }

  /**
   * Set the length of the preallocated values bytes used.
   * @param elementNum
   * @param length
   */
  public void setValPreallocated(int elementNum, int length) {
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
    // A call to increaseBufferSpace() or ensureValPreallocated() will ensure that buffer[] points to
    // a byte[] with sufficient space for the specified size.
    // This will either point to smallBuffer, or to a newly allocated byte array for larger values.

    if (nextElemLength > MAX_SIZE_FOR_SMALL_BUFFER) {
      // Larger allocations will be special-cased and will not use the normal buffer.
      // buffer/nextFree will be set to a newly allocated array just for the current row.
      // The next row will require another call to increaseBufferSpace() since this new buffer should be used up.
      byte[] newBuffer = new byte[nextElemLength];
      ++bufferAllocationCount;
      // If the buffer was pointing to smallBuffer, then nextFree keeps track of the current state
      // of the free index for smallBuffer. We now need to save this value to smallBufferNextFree
      // so we don't lose this. A bit of a weird dance here.
      if (smallBuffer == buffer) {
        smallBufferNextFree = nextFree;
      }
      buffer = newBuffer;
      nextFree = 0;
    } else {
      // This value should go into smallBuffer.
      if (smallBuffer != buffer) {
        // Previous row was for a large bytes value ( > MAX_SIZE_FOR_SMALL_BUFFER).
        // Use smallBuffer if possible.
        buffer = smallBuffer;
        nextFree = smallBufferNextFree;
      }

      // smallBuffer might still be out of space
      if ((nextFree + nextElemLength) > buffer.length) {
        int newLength = smallBuffer.length * 2;
        while (newLength < nextElemLength) {
          if (newLength > 0) {
            newLength *= 2;
          } else { // integer overflow happened; maximize size of next smallBuffer
            newLength = Integer.MAX_VALUE;
          }
        }
        smallBuffer = new byte[newLength];
        ++bufferAllocationCount;
        smallBufferNextFree = 0;
        // Update buffer
        buffer = smallBuffer;
        nextFree = 0;
      }
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
    other.nextFree = nextFree;
    other.vector = vector;
    other.start = start;
    other.length = length;
    other.buffer = buffer;
  }
}
