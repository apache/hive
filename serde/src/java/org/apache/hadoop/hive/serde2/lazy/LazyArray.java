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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * LazyArray stores an array of Lazy Objects.
 * 
 * LazyArray does not deal with the case of a NULL array. That is handled
 * by LazyArrayObjectInspector.
 */
public class LazyArray extends LazyNonPrimitive {

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed = false;
  /**
   * The length of the array.
   * Only valid when the data is parsed.
   * -1 when the array is NULL.
   */
  int arrayLength = 0;
  
  /**
   * The start positions of array elements.
   * Only valid when the data is parsed.
   * Note that startPosition[arrayLength] = begin + length + 1;
   * that makes sure we can use the same formula to compute the
   * length of each element of the array.
   */
  int[] startPosition;
  
  /**
   * Whether init() has been called on the element or not.
   */
  boolean[] elementInited;
  
  /**
   * The elements of the array. Note that we do arrayElements[i].
   * init(bytes, begin, length) only when that element is accessed.
   */
  LazyObject[] arrayElements;

  /**
   * Construct a LazyArray object with the TypeInfo.
   * @param typeInfo  the TypeInfo representing the type of this LazyArray.
   */
  protected LazyArray(TypeInfo typeInfo) {
    super(typeInfo);
  }

  /**
   * Set the row data for this LazyArray.
   * @see LazyObject#init(ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }
  
  /**
   * Enlarge the size of arrays storing information for the elements inside 
   * the array.
   */
  protected void enlargeArrays() {
    if (startPosition == null) {
      int initialSize = 2;
      startPosition = new int[initialSize]; 
      arrayElements = new LazyObject[initialSize];
      elementInited = new boolean[initialSize];
    } else {
      startPosition = Arrays.copyOf(startPosition, startPosition.length*2);
      arrayElements = Arrays.copyOf(arrayElements, arrayElements.length*2);
      elementInited = Arrays.copyOf(elementInited, elementInited.length*2);
    }
  }
  
  /**
   * Parse the bytes and fill arrayLength and startPosition.
   */
  private void parse(byte separator, Text nullSequence) {
    parsed = true;
    
    // empty array?
    if (length == 0) {
      arrayLength = 0;
      return;
    }
    
    byte[] bytes = this.bytes.getData();
    
    arrayLength = 0;
    int arrayByteEnd = start + length;
    int elementByteBegin = start;
    int elementByteEnd = start;
    
    // Go through all bytes in the byte[]
    while (elementByteEnd <= arrayByteEnd) {
      // Reached the end of a field?
      if (elementByteEnd == arrayByteEnd 
          || bytes[elementByteEnd] == separator) {
        // Array size not big enough?
        if (startPosition == null || arrayLength+1 == startPosition.length) {
          enlargeArrays();
        }
        startPosition[arrayLength] = elementByteBegin;
        arrayLength++;
        elementByteBegin = elementByteEnd + 1;
      }
      elementByteEnd++;
    }
    // Store arrayByteEnd+1 in startPosition[arrayLength]
    // so that we can use the same formula to compute the length of
    // each element in the array: startPosition[i+1] - startPosition[i] - 1
    startPosition[arrayLength] = elementByteEnd;
    
    if (arrayLength > 0) {
      Arrays.fill(elementInited, 0, arrayLength, false);
    }
    
  }
  
  /**
   * Returns the actual primitive object at the index position
   * inside the array represented by this LazyObject.
   */
  public Object getListElementObject(int index, byte separator, 
      Text nullSequence) {
    if (!parsed) {
      parse(separator, nullSequence);
    }
    if (index < 0 || index >= arrayLength) {
      return null;
    }
    return uncheckedGetElement(index, nullSequence);
  }
  
  /**
   * Get the element without checking parsed or out-of-bound index.
   */
  private Object uncheckedGetElement(int index, Text nullSequence) {
    int elementLength = startPosition[index+1] - startPosition[index] - 1;
    if (elementLength == nullSequence.getLength() 
        && 0 == LazyUtils.compare(bytes.getData(), startPosition[index], 
            elementLength, nullSequence.getBytes(), 0, 
            nullSequence.getLength())) {
      return null;
    } else {
      if (!elementInited[index]) {
        elementInited[index] = true;
        if (arrayElements[index] == null) {
          arrayElements[index] = LazyFactory.createLazyObject(
            ((ListTypeInfo)typeInfo).getListElementTypeInfo());
        }
        arrayElements[index].init(bytes, startPosition[index], 
            elementLength);
      }
    }
    return arrayElements[index].getObject();
  }
  
  /** Returns -1 for null array.
   */
  public int getListLength(byte separator, Text nullSequence) {
    if (!parsed) {
      parse(separator, nullSequence);
    }
    return arrayLength;
  }
  
  /** 
   * cachedList is reused every time getList is called.
   * Different LazyArray instances cannot share the same cachedList. 
   */
  ArrayList<Object> cachedList;
  /** Returns the List of actual primitive objects.
   *  Returns null for null array.
   */
  public List<Object> getList(byte separator, Text nullSequence) {
    if (!parsed) {
      parse(separator, nullSequence);
    }
    if (arrayLength == -1) {
      return null;
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>(arrayLength);
    } else {
      cachedList.clear();
    }
    for (int index=0; index<arrayLength; index++) {
      cachedList.add(uncheckedGetElement(index, nullSequence));
    }
    return cachedList;
  }
}
