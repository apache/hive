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

package org.apache.hadoop.hive.hbase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * LazyHBaseCellMap refines LazyMap with HBase column mapping.
 */
public class LazyHBaseCellMap extends LazyMap {

  private Result result;
  private byte [] columnFamilyBytes;
  private List<Boolean> binaryStorage;

  /**
   * Construct a LazyCellMap object with the ObjectInspector.
   * @param oi
   */
  public LazyHBaseCellMap(LazyMapObjectInspector oi) {
    super(oi);
  }

  public void init(
      Result r,
      byte [] columnFamilyBytes,
      List<Boolean> binaryStorage) {

    result = r;
    this.columnFamilyBytes = columnFamilyBytes;
    this.binaryStorage = binaryStorage;
    setParsed(false);
  }

  private void parse() {
    if (cachedMap == null) {
      cachedMap = new LinkedHashMap<Object, Object>();
    } else {
      cachedMap.clear();
    }

    NavigableMap<byte [], byte []> familyMap = result.getFamilyMap(columnFamilyBytes);

    if (familyMap != null) {

      for (Entry<byte [], byte []> e : familyMap.entrySet()) {
        // null values and values of zero length are not added to the cachedMap
        if (e.getValue() == null || e.getValue().length == 0) {
          continue;
        }

        LazyMapObjectInspector lazyMoi = getInspector();

        // Keys are always primitive
        LazyPrimitive<? extends ObjectInspector, ? extends Writable> key =
          LazyFactory.createLazyPrimitiveClass(
              (PrimitiveObjectInspector) lazyMoi.getMapKeyObjectInspector(),
              binaryStorage.get(0));

        ByteArrayRef keyRef = new ByteArrayRef();
        keyRef.setData(e.getKey());
        key.init(keyRef, 0, keyRef.getData().length);

        // Value
        LazyObject<?> value =
          LazyFactory.createLazyObject(lazyMoi.getMapValueObjectInspector(),
              binaryStorage.get(1));

        ByteArrayRef valueRef = new ByteArrayRef();
        valueRef.setData(e.getValue());
        value.init(valueRef, 0, valueRef.getData().length);

        // Put the key/value into the map
        cachedMap.put(key.getObject(), value.getObject());
      }
    }

    setParsed(true);
  }


  /**
   * Get the value in the map for the given key.
   *
   * @param key
   * @return
   */
  @Override
  public Object getMapValueElement(Object key) {
    if (!getParsed()) {
      parse();
    }

    for (Map.Entry<Object, Object> entry : cachedMap.entrySet()) {
      LazyPrimitive<?, ?> lazyKeyI = (LazyPrimitive<?, ?>) entry.getKey();
      // getWritableObject() will convert LazyPrimitive to actual primitive
      // writable objects.
      Object keyI = lazyKeyI.getWritableObject();
      if (keyI == null) {
        continue;
      }
      if (keyI.equals(key)) {
        // Got a match, return the value
        LazyObject<?> v = (LazyObject<?>) entry.getValue();
        return v == null ? v : v.getObject();
      }
    }

    return null;
  }

  @Override
  public Map<Object, Object> getMap() {
    if (!getParsed()) {
      parse();
    }
    return cachedMap;
  }

  @Override
  public int getMapSize() {
    if (!getParsed()) {
      parse();
    }
    return cachedMap.size();
  }
}
