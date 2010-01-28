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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.RecordInfo;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * LazyBinaryMap is serialized as follows: start A b c b c b c end bytes[] ->
 * |--------|---|---|---|---| ... |---|---|
 * 
 * Section A is the null-bytes. Suppose the map has N key-value pairs, then
 * there are (N*2+7)/8 bytes used as null-bytes. Each bit corresponds to a key
 * or a value and it indicates whether that key or value is null (0) or not null
 * (1).
 * 
 * After A, all the bytes are actual serialized data of the map, which are
 * key-value pairs. b represent the keys and c represent the values. Each of
 * them is again a LazyBinaryObject.
 * 
 */

public class LazyBinaryMap extends
    LazyBinaryNonPrimitive<LazyBinaryMapObjectInspector> {

  private static Log LOG = LogFactory.getLog(LazyBinaryMap.class.getName());

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * The size of the map. Only valid when the data is parsed. -1 when the map is
   * NULL.
   */
  int mapSize = 0;

  /**
   * The beginning position and length of key[i] and value[i]. Only valid when
   * the data is parsed.
   */
  int[] keyStart;
  int[] keyLength;
  int[] valueStart;
  int[] valueLength;
  /**
   * Whether valueObjects[i]/keyObjects[i] is initialized or not.
   */
  boolean[] keyInited;
  boolean[] valueInited;

  /**
   * Whether valueObjects[i]/keyObjects[i] is null or not This could not be
   * inferred from the length of the object. In particular, a 0-length string is
   * not null.
   */
  boolean[] keyIsNull;
  boolean[] valueIsNull;

  /**
   * The keys are stored in an array of LazyPrimitives.
   */
  LazyBinaryPrimitive<?, ?>[] keyObjects;
  /**
   * The values are stored in an array of LazyObjects. value[index] will start
   * from KeyEnd[index] + 1, and ends before KeyStart[index+1] - 1.
   */
  LazyBinaryObject[] valueObjects;

  protected LazyBinaryMap(LazyBinaryMapObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the row data for this LazyBinaryMap.
   * 
   * @see LazyBinaryObject#init(ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  /**
   * Adjust the size of arrays: keyStart, keyLength valueStart, valueLength
   * keyInited, keyIsNull valueInited, valueIsNull
   */
  protected void adjustArraySize(int newSize) {
    if (keyStart == null || keyStart.length < newSize) {
      keyStart = new int[newSize];
      keyLength = new int[newSize];
      valueStart = new int[newSize];
      valueLength = new int[newSize];
      keyInited = new boolean[newSize];
      keyIsNull = new boolean[newSize];
      valueInited = new boolean[newSize];
      valueIsNull = new boolean[newSize];
      keyObjects = new LazyBinaryPrimitive<?, ?>[newSize];
      valueObjects = new LazyBinaryObject[newSize];
    }
  }

  boolean nullMapKey = false;
  VInt vInt = new LazyBinaryUtils.VInt();
  RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();

  /**
   * Parse the byte[] and fill keyStart, keyLength, keyIsNull valueStart,
   * valueLength and valueIsNull
   */
  private void parse() {

    byte[] bytes = this.bytes.getData();

    // get the VInt that represents the map size
    LazyBinaryUtils.readVInt(bytes, start, vInt);
    mapSize = vInt.value;
    if (0 == mapSize) {
      parsed = true;
      return;
    }

    // adjust arrays
    adjustArraySize(mapSize);

    // find out the null-bytes
    int mapByteStart = start + vInt.length;
    int nullByteCur = mapByteStart;
    int nullByteEnd = mapByteStart + (mapSize * 2 + 7) / 8;
    int lastElementByteEnd = nullByteEnd;

    // parsing the keys and values one by one
    for (int i = 0; i < mapSize; i++) {
      // parse a key
      keyIsNull[i] = true;
      if ((bytes[nullByteCur] & (1 << ((i * 2) % 8))) != 0) {
        keyIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(((MapObjectInspector) oi)
            .getMapKeyObjectInspector(), bytes, lastElementByteEnd, recordInfo);
        keyStart[i] = lastElementByteEnd + recordInfo.elementOffset;
        keyLength[i] = recordInfo.elementSize;
        lastElementByteEnd = keyStart[i] + keyLength[i];
      } else if (!nullMapKey) {
        nullMapKey = true;
        LOG.warn("Null map key encountered! Ignoring similar problems.");
      }

      // parse a value
      valueIsNull[i] = true;
      if ((bytes[nullByteCur] & (1 << ((i * 2 + 1) % 8))) != 0) {
        valueIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(((MapObjectInspector) oi)
            .getMapValueObjectInspector(), bytes, lastElementByteEnd,
            recordInfo);
        valueStart[i] = lastElementByteEnd + recordInfo.elementOffset;
        valueLength[i] = recordInfo.elementSize;
        lastElementByteEnd = valueStart[i] + valueLength[i];
      }

      // move onto the next null byte
      if (3 == (i % 4)) {
        nullByteCur++;
      }
    }

    Arrays.fill(keyInited, 0, mapSize, false);
    Arrays.fill(valueInited, 0, mapSize, false);
    parsed = true;
  }

  /**
   * Get the value object with the index without checking parsed.
   * 
   * @param index
   *          The index into the array starting from 0
   */
  private LazyBinaryObject uncheckedGetValue(int index) {
    if (valueIsNull[index]) {
      return null;
    }
    if (!valueInited[index]) {
      valueInited[index] = true;
      if (valueObjects[index] == null) {
        valueObjects[index] = LazyBinaryFactory
            .createLazyBinaryObject(((MapObjectInspector) oi)
                .getMapValueObjectInspector());
      }
      valueObjects[index].init(bytes, valueStart[index], valueLength[index]);
    }
    return valueObjects[index];
  }

  /**
   * Get the value in the map for the key.
   * 
   * If there are multiple matches (which is possible in the serialized format),
   * only the first one is returned.
   * 
   * The most efficient way to get the value for the key is to serialize the key
   * and then try to find it in the array. We do linear search because in most
   * cases, user only wants to get one or two values out of the map, and the
   * cost of building up a HashMap is substantially higher.
   * 
   * @param key
   *          The key object that we are looking for.
   * @return The corresponding value object, or NULL if not found
   */
  public Object getMapValueElement(Object key) {
    if (!parsed) {
      parse();
    }
    // search for the key
    for (int i = 0; i < mapSize; i++) {
      LazyBinaryPrimitive<?, ?> lazyKeyI = uncheckedGetKey(i);
      if (lazyKeyI == null) {
        continue;
      }
      // getWritableObject() will convert LazyPrimitive to actual primitive
      // writable objects.
      Object keyI = lazyKeyI.getWritableObject();
      if (keyI == null) {
        continue;
      }
      if (keyI.equals(key)) {
        // Got a match, return the value
        LazyBinaryObject v = uncheckedGetValue(i);
        return v == null ? v : v.getObject();
      }
    }
    return null;
  }

  /**
   * Get the key object with the index without checking parsed.
   * 
   * @param index
   *          The index into the array starting from 0
   */
  private LazyBinaryPrimitive<?, ?> uncheckedGetKey(int index) {
    if (keyIsNull[index]) {
      return null;
    }
    if (!keyInited[index]) {
      keyInited[index] = true;
      if (keyObjects[index] == null) {
        // Keys are always primitive
        keyObjects[index] = LazyBinaryFactory
            .createLazyBinaryPrimitiveClass((PrimitiveObjectInspector) ((MapObjectInspector) oi)
                .getMapKeyObjectInspector());
      }
      keyObjects[index].init(bytes, keyStart[index], keyLength[index]);
    }
    return keyObjects[index];
  }

  /**
   * cachedMap is reused for different calls to getMap(). But each LazyBinaryMap
   * has a separate cachedMap so we won't overwrite the data by accident.
   */
  LinkedHashMap<Object, Object> cachedMap;

  /**
   * Return the map object representing this LazyBinaryMap. Note that the
   * keyObjects will be Writable primitive objects.
   * 
   * @return the map object
   */
  public Map<Object, Object> getMap() {
    if (!parsed) {
      parse();
    }
    if (cachedMap == null) {
      // Use LinkedHashMap to provide deterministic order
      cachedMap = new LinkedHashMap<Object, Object>();
    } else {
      cachedMap.clear();
    }

    // go through each element of the map
    for (int i = 0; i < mapSize; i++) {
      LazyBinaryPrimitive<?, ?> lazyKey = uncheckedGetKey(i);
      if (lazyKey == null) {
        continue;
      }
      Object key = lazyKey.getObject();
      // do not overwrite if there are duplicate keys
      if (key != null && !cachedMap.containsKey(key)) {
        LazyBinaryObject lazyValue = uncheckedGetValue(i);
        Object value = (lazyValue == null ? null : lazyValue.getObject());
        cachedMap.put(key, value);
      }
    }
    return cachedMap;
  }

  /**
   * Get the size of the map represented by this LazyBinaryMap.
   * 
   * @return The size of the map
   */
  public int getMapSize() {
    if (!parsed) {
      parse();
    }
    return mapSize;
  }
}
