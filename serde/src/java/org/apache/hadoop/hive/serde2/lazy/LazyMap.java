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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

/**
 * LazyMap stores a map of Primitive LazyObjects to LazyObjects.
 * Note that the keys of the map cannot contain null.
 * 
 * LazyMap does not deal with the case of a NULL map. That is handled
 * by LazyMapObjectInspector.
 */
public class LazyMap extends LazyNonPrimitive {
  
  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed = false;
  
  /**
   * The size of the map.
   * Only valid when the data is parsed.
   * -1 when the map is NULL.
   */
  int mapSize = 0;
  
  /**
   * The beginning position of key[i].
   * Only valid when the data is parsed.
   * Note that keyStart[mapSize] = begin + length + 1;
   * that makes sure we can use the same formula to compute the
   * length of each value in the map.
   */
  int[] keyStart;
  
  /**
   * The end position of key[i] (the position of the key-value separator).
   * Only valid when the data is parsed.
   */  
  int[] keyEnd;
  /**
   * The keys are stored in an array of LazyPrimitives.
   */
  LazyPrimitive<?>[] keyObjects;
  /**
   * Whether init() is called on keyObjects[i]. 
   */
  boolean[] keyInited;
  /**
   * The values are stored in an array of LazyObjects.
   * value[index] will start from KeyEnd[index] + 1,
   * and ends before KeyStart[index+1] - 1.
   */
  LazyObject[] valueObjects;
  /**
   * Whether init() is called on valueObjects[i]
   */
  boolean[] valueInited;
  
  /**
   * Construct a LazyMap object with the TypeInfo.
   * @param typeInfo  the TypeInfo representing the type of this LazyMap.
   */
  protected LazyMap(TypeInfo typeInfo) {
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
    if (keyStart == null) {
      int initialSize = 2;
      keyStart = new int[initialSize];
      keyEnd = new int[initialSize];
      keyObjects = new LazyPrimitive<?>[initialSize];
      valueObjects = new LazyObject[initialSize];
      keyInited = new boolean[initialSize];
      valueInited = new boolean[initialSize];
    } else {
      keyStart = Arrays.copyOf(keyStart, keyStart.length*2);
      keyEnd = Arrays.copyOf(keyEnd, keyEnd.length*2);
      keyObjects = Arrays.copyOf(keyObjects, keyObjects.length*2);
      valueObjects = Arrays.copyOf(valueObjects, valueObjects.length*2);
      keyInited = Arrays.copyOf(keyInited, keyInited.length*2);
      valueInited = Arrays.copyOf(valueInited, valueInited.length*2);
    }
  }

  /**
   * Parse the byte[] and fill keyStart, keyEnd.
   * @param itemSeparator     The separator between different entries.
   * @param keyValueSeparator The separator between key and value.
   * @param nullSequence      The byte sequence representing NULL.
   */
  private void parse(byte itemSeparator, byte keyValueSeparator, 
      Text nullSequence) {
    parsed = true;
    
    // empty array?
    if (length == 0) {
      mapSize = 0;
      return;
    }
    
    mapSize = 0;
    int arrayByteEnd = start + length;
    int elementByteBegin = start;
    int keyValueSeparatorPosition = -1;
    int elementByteEnd = start;
    byte[] bytes = this.bytes.getData();
    
    // Go through all bytes in the byte[]
    while (elementByteEnd <= arrayByteEnd) {
      // End of entry reached?
      if (elementByteEnd == arrayByteEnd 
          || bytes[elementByteEnd] == itemSeparator) {
        // Array full?
        if (keyStart == null || mapSize + 1 == keyStart.length) {
          enlargeArrays();
        }
        keyStart[mapSize] = elementByteBegin;
        // If no keyValueSeparator is seen, all bytes belong to key, and 
        // value will be NULL.
        keyEnd[mapSize] = (keyValueSeparatorPosition == -1 
            ? elementByteEnd: keyValueSeparatorPosition);
        // reset keyValueSeparatorPosition
        keyValueSeparatorPosition = -1;
        mapSize++;
        elementByteBegin = elementByteEnd + 1;
      }
      // Is this the first keyValueSeparator in this entry?
      if (keyValueSeparatorPosition == -1 && elementByteEnd != arrayByteEnd
          && bytes[elementByteEnd] == keyValueSeparator) {
        keyValueSeparatorPosition = elementByteEnd;
      }
      elementByteEnd++;
    }
    
    // This makes sure we can use the same formula to compute the
    // length of each value in the map.
    keyStart[mapSize] = arrayByteEnd + 1;

    if (mapSize > 0) {
      Arrays.fill(keyInited, 0, mapSize, false);
      Arrays.fill(valueInited, 0, mapSize, false);
    }
  }
  
  /**
   * Get the value in the map for the key.
   * 
   * If there are multiple matches (which is possible in the serialized 
   * format), only the first one is returned.
   * 
   * The most efficient way to get the value for the key is to serialize the 
   * key and then try to find it in the array.  We do linear search because in 
   * most cases, user only wants to get one or two values out of the map, and 
   * the cost of building up a HashMap is substantially higher.
   * 
   * @param itemSeparator     The separator between different entries.
   * @param keyValueSeparator The separator between key and value.
   * @param nullSequence      The byte sequence representing NULL.
   * @param key               The key object that we are looking for.
   * @return The corresponding value object, or NULL if not found
   */
  public Object getMapValueElement(byte itemSeparator, byte keyValueSeparator, 
      Text nullSequence, Object key) {
    if (!parsed) {
      parse(itemSeparator, keyValueSeparator, nullSequence);
    }
    
    // search for the key
    for (int i=0; i<mapSize; i++) {
      LazyPrimitive<?> lazyKeyI = uncheckedGetKey(i, nullSequence);
      if (lazyKeyI == null) continue;
      // getObject() will convert LazyPrimitive to actual primitive objects.
      Object keyI = lazyKeyI.getObject();
      if (keyI == null) continue;
      if (keyI.equals(key)) {
        // Got a match, return the value
        LazyObject v = uncheckedGetValue(i, nullSequence);
        return v == null ? v : v.getObject();
      }
    }
    
    return null;
  }

  /**
   * Get the value object with the index without checking parsed.
   * @param index  The index into the array starting from 0
   * @param nullSequence  The byte sequence representing the NULL value
   */
  private LazyObject uncheckedGetValue(int index, Text nullSequence) {
    int valueIBegin = keyEnd[index] + 1;
    int valueILength = keyStart[index+1] - 1 - valueIBegin;
    if (valueILength < 0 || 
         ((valueILength == nullSequence.getLength())
          && 0 == LazyUtils.compare(bytes.getData(), valueIBegin, valueILength, 
              nullSequence.getBytes(), 0, nullSequence.getLength()))) {
      return null; 
    }
    if (!valueInited[index]) {
      valueInited[index] = true;
      if (valueObjects[index] == null) {
        valueObjects[index] = LazyFactory.createLazyObject(
            ((MapTypeInfo)typeInfo).getMapValueTypeInfo());
      }
      valueObjects[index].init(bytes, valueIBegin, valueILength);
    }
    return valueObjects[index];
  }
  
  /**
   * Get the key object with the index without checking parsed.
   * @param index  The index into the array starting from 0
   * @param nullSequence  The byte sequence representing the NULL value
   */
  private LazyPrimitive<?> uncheckedGetKey(int index, Text nullSequence) {
    int keyIBegin = keyStart[index];
    int keyILength = keyEnd[index] - keyStart[index];
    if (keyILength < 0 || 
         ((keyILength == nullSequence.getLength())
          && 0 == LazyUtils.compare(bytes.getData(), keyIBegin, keyILength, 
              nullSequence.getBytes(), 0, nullSequence.getLength()))) {
      return null;
    }
    if (!keyInited[index]) {
      keyInited[index] = true;
      if (keyObjects[index] == null) {
        // Keys are always primitive
        keyObjects[index] = LazyFactory.createLazyPrimitiveClass(
            ((MapTypeInfo)typeInfo).getMapKeyTypeInfo().getTypeName());
      }
      keyObjects[index].init(bytes, keyIBegin, keyILength);
    }
    return keyObjects[index];
  }
  
  /**
   * cachedMap is reused for different calls to getMap().
   * But each LazyMap has a separate cachedMap so we won't overwrite the
   * data by accident.
   */
  HashMap<Object, Object> cachedMap;
  
  /**
   * Return the map object representing this LazyMap.
   * Note that the keyObjects will be Java primitive objects.
   * @param itemSeparator     The separator between different entries.
   * @param keyValueSeparator The separator between key and value.
   * @param nullSequence      The byte sequence representing NULL.
   * @return the map object
   */
  public Map<Object, Object> getMap(byte itemSeparator, byte keyValueSeparator,
      Text nullSequence) {
    if (!parsed) {
      parse(itemSeparator, keyValueSeparator, nullSequence);
    }
    if (cachedMap == null) {
      cachedMap = new HashMap<Object, Object>();
    }
    cachedMap.clear();
    
    // go through each element of the map
    for (int i = 0; i < mapSize; i++) {
      LazyPrimitive<?> lazyKey = uncheckedGetKey(i, nullSequence);
      if (lazyKey == null) continue;
      Object key = lazyKey.getObject();
      // do not overwrite if there are duplicate keys
      if (key != null && !cachedMap.containsKey(key)) {
        LazyObject lazyValue = uncheckedGetValue(i, nullSequence);
        Object value = (lazyValue == null ? null : lazyValue.getObject());
        cachedMap.put(key, value);
      }
    }
    return cachedMap;
  }

  /**
   * Get the size of the map represented by this LazyMap.
   * @param itemSeparator     The separator between different entries.
   * @param keyValueSeparator The separator between key and value.
   * @param nullSequence      The byte sequence representing NULL.
   * @return                  The size of the map, -1 for NULL map.
   */
  public int getMapSize(byte itemSeparator, byte keyValueSeparator,
      Text nullSequence) {
    if (!parsed) {
      parse(itemSeparator, keyValueSeparator, nullSequence);
    }
    return mapSize;
  }

}
