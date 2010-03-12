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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * LazyHBaseCellMap refines LazyMap with HBase column mapping.
 */
public class LazyHBaseCellMap extends LazyMap {
  
  private RowResult rowResult;
  private String hbaseColumnFamily;
  
  /**
   * Construct a LazyCellMap object with the ObjectInspector.
   * @param oi
   */
  public LazyHBaseCellMap(LazyMapObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    // do nothing
  }
  
  public void init(RowResult rr, String columnFamily) {
    rowResult = rr;
    hbaseColumnFamily = columnFamily;
    setParsed(false);
  }
  
  private void parse() {
    if (cachedMap == null) {
      cachedMap = new LinkedHashMap<Object, Object>();
    } else {
      cachedMap.clear();
    }
    
    Iterator<byte[]> iter = rowResult.keySet().iterator();
    
    byte[] columnFamily = hbaseColumnFamily.getBytes();
    while (iter.hasNext()) {
      byte [] columnKey = iter.next();
      if (columnFamily.length > columnKey.length) {
        continue;
      }
      
      if (0 == LazyUtils.compare(
          columnFamily, 0, columnFamily.length, 
          columnKey, 0, columnFamily.length)) {

        byte [] columnValue = rowResult.get(columnKey).getValue();
        if (columnValue == null || columnValue.length == 0) {
          // an empty object
          continue;
        }
        
        // Keys are always primitive
        LazyPrimitive<?, ?> key = LazyFactory.createLazyPrimitiveClass(
            (PrimitiveObjectInspector)
            ((MapObjectInspector) getInspector()).getMapKeyObjectInspector());
        ByteArrayRef keyRef = new ByteArrayRef();
        keyRef.setData(columnKey);
        key.init(
          keyRef, columnFamily.length, columnKey.length - columnFamily.length);
        
        // Value
        LazyObject value = LazyFactory.createLazyObject(
          ((MapObjectInspector) getInspector()).getMapValueObjectInspector());
        ByteArrayRef valueRef = new ByteArrayRef();
        valueRef.setData(columnValue);
        value.init(valueRef, 0, columnValue.length);
        
        // Put it into the map
        cachedMap.put(key.getObject(), value.getObject());
      }
    }
  }
  
  /**
   * Get the value in the map for the given key.
   * 
   * @param key
   * @return
   */
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
        LazyObject v = (LazyObject) entry.getValue();
        return v == null ? v : v.getObject();
      }
    }
    
    return null;
  }
  
  public Map<Object, Object> getMap() {
    if (!getParsed()) {
      parse();
    }
    return cachedMap;
  }
  
  public int getMapSize() {
    if (!getParsed()) {
      parse();
    }
    return cachedMap.size();
  }

}
