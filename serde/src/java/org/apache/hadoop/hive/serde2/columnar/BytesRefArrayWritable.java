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

package org.apache.hadoop.hive.serde2.columnar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * <tt>BytesRefArrayWritable</tt> holds an array reference to BytesRefWritable,
 * and is able to resize without recreating new array if not necessary.
 * <p>
 * 
 * Each <tt>BytesRefArrayWritable holds</tt> instance has a <i>valid</i> field,
 * which is the desired valid number of <tt>BytesRefWritable</tt> it holds.
 * <tt>resetValid</tt> can reset the valid, but it will not care the underlying
 * BytesRefWritable.
 */

public class BytesRefArrayWritable implements Writable,
    Comparable<BytesRefArrayWritable> {

  private BytesRefWritable[] bytesRefWritables = null;

  private int valid = 0;

  /**
   * Constructs an empty array with the specified capacity.
   * 
   * @param capacity
   *          initial capacity
   * @exception IllegalArgumentException
   *              if the specified initial capacity is negative
   */
  public BytesRefArrayWritable(int capacity) {
    if (capacity < 0)
      throw new IllegalArgumentException("Capacity can not be negative.");
    bytesRefWritables = new BytesRefWritable[0];
    ensureCapacity(capacity);
  }

  /**
   * Constructs an empty array with a capacity of ten.
   */
  public BytesRefArrayWritable() {
    this(10);
  }

  /**
   * Returns the number of valid elements.
   * 
   * @return the number of valid elements
   */
  public int size() {
    return valid;
  }

  /**
   * Gets the BytesRefWritable at the specified position. Make sure the position
   * is valid by first call resetValid.
   * 
   * @param index
   *          the position index, starting from zero
   * @throws IndexOutOfBoundsException
   */
  public BytesRefWritable get(int index) {
    if (index >= valid)
      throw new IndexOutOfBoundsException(
          "This BytesRefArrayWritable only has " + valid
              + " valid values.");
    return bytesRefWritables[index];
  }

  /**
   * Gets the BytesRefWritable at the specified position without checking.
   * 
   * @param index
   *          the position index, starting from zero
   * @throws IndexOutOfBoundsException
   */
  public BytesRefWritable unCheckedGet(int index) {
    return bytesRefWritables[index];
  }

  /**
   * Set the BytesRefWritable at the specified position with the specified
   * BytesRefWritable.
   * 
   * @param index
   *          index position
   * @param bytesRefWritable
   *          the new element
   * @throws IllegalArgumentException
   *           if the specified new element is null
   */
  public void set(int index, BytesRefWritable bytesRefWritable) {
    if (bytesRefWritable == null)
      throw new IllegalArgumentException("Can not assign null.");
    ensureCapacity(index + 1);
    bytesRefWritables[index] = bytesRefWritable;
    if (valid <= index)
      valid = index + 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(BytesRefArrayWritable other) {
    if (other == null)
      throw new IllegalArgumentException("Argument can not be null.");
    if (this == other)
      return 0;
    int sizeDiff = valid - other.valid;
    if (sizeDiff != 0)
      return sizeDiff;
    for (int i = 0; i < valid; i++) {
      if (other.contains(bytesRefWritables[i]))
        continue;
      else
        return 1;
    }
    return 0;
  }

  /**
   * Returns <tt>true</tt> if this instance contains one or more the specified
   * BytesRefWritable.
   * 
   * @param bytesRefWritable
   *          BytesRefWritable element to be tested
   * @return <tt>true</tt> if contains the specified element
   * @throws IllegalArgumentException
   *           if the specified element is null
   */
  public boolean contains(BytesRefWritable bytesRefWritable) {
    if (bytesRefWritable == null)
      throw new IllegalArgumentException("Argument can not be null.");
    for (int i = 0; i < valid; i++) {
      if (bytesRefWritables[i].equals(bytesRefWritable))
        return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object o) {
    if (o == null || !(o instanceof BytesRefArrayWritable))
      return false;
    return compareTo((BytesRefArrayWritable) o) == 0;
  }

  /**
   * Removes all elements.
   */
  public void clear() {
    valid = 0;
  }

  /**
   * enlarge the capacity if necessary, to ensure that it can hold the number of
   * elements specified by newValidCapacity argument. It will also narrow the
   * valid capacity when needed. Notice: it only enlarge or narrow the valid
   * capacity with no care of the already stored invalid BytesRefWritable.
   * 
   * @param newValidCapacity
   *          the desired capacity
   */
  public void resetValid(int newValidCapacity) {
    ensureCapacity(newValidCapacity);
    valid = newValidCapacity;
  }

  protected void ensureCapacity(int newCapacity) {
    int size = bytesRefWritables.length;
    if (size < newCapacity) {
      bytesRefWritables = Arrays.copyOf(bytesRefWritables, newCapacity);
      while (size < newCapacity) {
        bytesRefWritables[size] = new BytesRefWritable();
        size++;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int count = in.readInt();
    ensureCapacity(count);
    for (int i = 0; i < count; i++) {
      bytesRefWritables[i].readFields(in);
    }
    valid = count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(valid);

    for (int i = 0; i < valid; i++) {
      BytesRefWritable cu = bytesRefWritables[i];
      cu.write(out);
    }
  }

  static {
    WritableFactories.setFactory(BytesRefArrayWritable.class,
        new WritableFactory() {

          @Override
          public Writable newInstance() {
            return new BytesRefArrayWritable();
          }

        });
  }
}
