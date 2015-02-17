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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.RecordInfo;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * LazyBinaryArray is serialized as follows: start A b b b b b b end bytes[] ->
 * |--------|---|---|---|---| ... |---|---|
 * 
 * Section A is the null-bytes. Suppose the list has N elements, then there are
 * (N+7)/8 bytes used as null-bytes. Each bit corresponds to an element and it
 * indicates whether that element is null (0) or not null (1).
 * 
 * After A, all b(s) represent the elements of the list. Each of them is again a
 * LazyBinaryObject.
 * 
 */

public class LazyBinaryArray extends
    LazyBinaryNonPrimitive<LazyBinaryListObjectInspector> {

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed = false;
  /**
   * The length of the array. Only valid when the data is parsed.
   */
  int arraySize = 0;

  /**
   * The start positions and lengths of array elements. Only valid when the data
   * is parsed.
   */
  int[] elementStart;
  int[] elementLength;

  /**
   * Whether an element is initialized or not.
   */
  boolean[] elementInited;

  /**
   * Whether an element is null or not. Because length is 0 does not means the
   * field is null. In particular, a 0-length string is not null.
   */
  boolean[] elementIsNull;

  /**
   * The elements of the array. Note that we call arrayElements[i].init(bytes,
   * begin, length) only when that element is accessed.
   */
  LazyBinaryObject[] arrayElements;

  /**
   * Construct a LazyBinaryArray object with the ObjectInspector.
   * 
   * @param oi
   *          the oi representing the type of this LazyBinaryArray
   */
  protected LazyBinaryArray(LazyBinaryListObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the row data for this LazyBinaryArray.
   * 
   * @see LazyObject#init(ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  /**
   * Enlarge the size of arrays storing information for the elements inside the
   * array.
   */
  private void adjustArraySize(int newSize) {
    if (elementStart == null || elementStart.length < newSize) {
      elementStart = new int[newSize];
      elementLength = new int[newSize];
      elementInited = new boolean[newSize];
      elementIsNull = new boolean[newSize];
      arrayElements = new LazyBinaryObject[newSize];
    }
  }

  VInt vInt = new LazyBinaryUtils.VInt();
  RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();

  /**
   * Parse the bytes and fill elementStart, elementLength, elementInited and
   * elementIsNull.
   */
  private void parse() {

    byte[] bytes = this.bytes.getData();

    // get the vlong that represents the map size
    LazyBinaryUtils.readVInt(bytes, start, vInt);
    arraySize = vInt.value;
    if (0 == arraySize) {
      parsed = true;
      return;
    }

    // adjust arrays
    adjustArraySize(arraySize);
    // find out the null-bytes
    int arryByteStart = start + vInt.length;
    int nullByteCur = arryByteStart;
    int nullByteEnd = arryByteStart + (arraySize + 7) / 8;
    // the begin the real elements
    int lastElementByteEnd = nullByteEnd;
    // the list element object inspector
    ObjectInspector listEleObjectInspector = ((ListObjectInspector) oi)
        .getListElementObjectInspector();
    // parsing elements one by one
    for (int i = 0; i < arraySize; i++) {
      elementIsNull[i] = true;
      if ((bytes[nullByteCur] & (1 << (i % 8))) != 0) {
        elementIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(listEleObjectInspector, bytes,
            lastElementByteEnd, recordInfo, vInt);
        elementStart[i] = lastElementByteEnd + recordInfo.elementOffset;
        elementLength[i] = recordInfo.elementSize;
        lastElementByteEnd = elementStart[i] + elementLength[i];
      }
      // move onto the next null byte
      if (7 == (i % 8)) {
        nullByteCur++;
      }
    }

    Arrays.fill(elementInited, 0, arraySize, false);
    parsed = true;
  }

  /**
   * Returns the actual primitive object at the index position inside the array
   * represented by this LazyBinaryObject.
   */
  public Object getListElementObject(int index) {
    if (!parsed) {
      parse();
    }
    if (index < 0 || index >= arraySize) {
      return null;
    }
    return uncheckedGetElement(index);
  }

  /**
   * Get the element without checking out-of-bound index.
   * 
   * @param index
   *          index to the array element
   */
  private Object uncheckedGetElement(int index) {

    if (elementIsNull[index]) {
      return null;
    } else {
      if (!elementInited[index]) {
        elementInited[index] = true;
        if (arrayElements[index] == null) {
          arrayElements[index] = LazyBinaryFactory.createLazyBinaryObject((oi)
              .getListElementObjectInspector());
        }
        arrayElements[index].init(bytes, elementStart[index],
            elementLength[index]);
      }
    }
    return arrayElements[index].getObject();
  }

  /**
   * Returns the array size.
   */
  public int getListLength() {
    if (!parsed) {
      parse();
    }
    return arraySize;
  }

  /**
   * cachedList is reused every time getList is called. Different
   * LazyBinaryArray instances cannot share the same cachedList.
   */
  ArrayList<Object> cachedList;

  /**
   * Returns the List of actual primitive objects. Returns null for null array.
   */
  public List<Object> getList() {
    if (!parsed) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>(arraySize);
    } else {
      cachedList.clear();
    }
    for (int index = 0; index < arraySize; index++) {
      cachedList.add(uncheckedGetElement(index));
    }
    return cachedList;
  }
}
