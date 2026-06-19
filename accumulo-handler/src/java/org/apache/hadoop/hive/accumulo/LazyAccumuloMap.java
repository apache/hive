/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.accumulo.AccumuloHiveRow.ColumnTuple;
import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloMapColumnMapping;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Charsets;

/**
 * A Hive Map created from some collection of Key-Values from one to many column families with one
 * to many column qualifiers.
 */
public class LazyAccumuloMap extends LazyMap {

  protected AccumuloHiveRow sourceRow;
  protected HiveAccumuloMapColumnMapping columnMapping;

  public LazyAccumuloMap(LazyMapObjectInspector oi) {
    super(oi);
  }

  public void init(AccumuloHiveRow row, HiveAccumuloMapColumnMapping columnMapping) {
    this.sourceRow = row;
    this.columnMapping = columnMapping;

    this.setParsed(false);
  }

  protected void parse() {
    if (null == this.cachedMap) {
      this.cachedMap = new LinkedHashMap<Object,Object>();
    } else {
      this.cachedMap.clear();
    }

    LazyMapObjectInspector lazyMoi = getInspector();

    Text cf = new Text(columnMapping.getColumnFamily());
    for (ColumnTuple tuple : sourceRow.getTuples()) {
      String cq = tuple.getCq().toString();

      if (!cf.equals(tuple.getCf()) || !cq.startsWith(columnMapping.getColumnQualifierPrefix())) {
        // A column family or qualifier we don't want to include in the map
        continue;
      }

      // Because we append the cq prefix when serializing the column
      // we should also remove it when pulling it from Accumulo
      cq = cq.substring(columnMapping.getColumnQualifierPrefix().length());

      // Keys are always primitive, respect the binary
      LazyPrimitive<? extends ObjectInspector,? extends Writable> key = LazyFactory
          .createLazyPrimitiveClass((PrimitiveObjectInspector) lazyMoi.getMapKeyObjectInspector(),
              ColumnEncoding.BINARY == columnMapping.getKeyEncoding());

      ByteArrayRef keyRef = new ByteArrayRef();
      keyRef.setData(cq.getBytes(Charsets.UTF_8));
      key.init(keyRef, 0, keyRef.getData().length);

      // Value can be anything, use the obj inspector and respect binary
      LazyObject<?> value = LazyFactory.createLazyObject(lazyMoi.getMapValueObjectInspector(),
          ColumnEncoding.BINARY == columnMapping.getValueEncoding());

      byte[] bytes = tuple.getValue();
      if (bytes == null || isNull(oi.getNullSequence(), bytes, 0, bytes.length)) {
        value.setNull();
      } else {
        ByteArrayRef valueRef = new ByteArrayRef();
        valueRef.setData(bytes);
        value.init(valueRef, 0, valueRef.getData().length);
      }

      cachedMap.put(key, value);
    }

    this.setParsed(true);
  }

  /**
   * Get the value in the map for the given key.
   *
   * @param key
   *          The key, a column qualifier, from the map
   * @return The object in the map at the given key
   */
  @Override
  public Object getMapValueElement(Object key) {
    if (!getParsed()) {
      parse();
    }

    for (Map.Entry<Object,Object> entry : cachedMap.entrySet()) {
      LazyPrimitive<?,?> lazyKey = (LazyPrimitive<?,?>) entry.getKey();

      // getWritableObject() will convert LazyPrimitive to actual primitive
      // writable objects.
      Object keyI = lazyKey.getWritableObject();
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
  public Map<Object,Object> getMap() {
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
