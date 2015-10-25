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
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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
  private byte[] qualPrefix;
  private List<Boolean> binaryStorage;
  private boolean hideQualPrefix;

	/**
   * Construct a LazyCellMap object with the ObjectInspector.
   * @param oi
   */
  public LazyHBaseCellMap(LazyMapObjectInspector oi) {
    super(oi);
  }

  public void init(
      Result r,
      byte[] columnFamilyBytes,
      List<Boolean> binaryStorage) {

    init(r, columnFamilyBytes, binaryStorage, null);
  }

	public void init(
			Result r,
			byte [] columnFamilyBytes,
			List<Boolean> binaryStorage, byte[] qualPrefix) {
		init(r, columnFamilyBytes, binaryStorage, qualPrefix, false);
	}

  public void init(
      Result r,
      byte [] columnFamilyBytes,
      List<Boolean> binaryStorage, byte[] qualPrefix, boolean hideQualPrefix) {
    this.isNull = false;
    this.result = r;
    this.columnFamilyBytes = columnFamilyBytes;
    this.binaryStorage = binaryStorage;
    this.qualPrefix = qualPrefix;
    this.hideQualPrefix = hideQualPrefix;
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

        if (qualPrefix != null && !Bytes.startsWith(e.getKey(), qualPrefix)) {
          // since we were provided a qualifier prefix, only accept qualifiers that start with this
          // prefix
          continue;
        }

        LazyMapObjectInspector lazyMoi = getInspector();

        // Keys are always primitive
        LazyPrimitive<? extends ObjectInspector, ? extends Writable> key =
          LazyFactory.createLazyPrimitiveClass(
              (PrimitiveObjectInspector) lazyMoi.getMapKeyObjectInspector(),
              binaryStorage.get(0));

        ByteArrayRef keyRef = new ByteArrayRef();
		  if (qualPrefix!=null && hideQualPrefix){
			  keyRef.setData(Bytes.tail(e.getKey(), e.getKey().length-qualPrefix.length)); //cut prefix from hive's map key
		  }else{
			  keyRef.setData(e.getKey()); //for non-prefix maps
		  }
        key.init(keyRef, 0, keyRef.getData().length);

        // Value
        LazyObject<?> value =
          LazyFactory.createLazyObject(lazyMoi.getMapValueObjectInspector(),
              binaryStorage.get(1));

        byte[] bytes = e.getValue();

        if (isNull(oi.getNullSequence(), bytes, 0, bytes.length)) {
          value.setNull();
        } else {
          ByteArrayRef valueRef = new ByteArrayRef();
          valueRef.setData(bytes);
          value.init(valueRef, 0, valueRef.getData().length);
        }

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
        Object _value = entry.getValue();

        // If the given value is a type of LazyObject, then only try and convert it to that form.
        // Else return it as it is.
        if (_value instanceof LazyObject) {
          LazyObject<?> v = (LazyObject<?>) entry.getValue();
          return v == null ? v : v.getObject();
        } else {
          return _value;
        }
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
