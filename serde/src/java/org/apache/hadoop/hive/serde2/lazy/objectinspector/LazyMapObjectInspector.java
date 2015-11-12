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

package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * LazyMapObjectInspector works on struct data that is stored in LazyStruct.
 *
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class LazyMapObjectInspector implements MapObjectInspector {

  public static final Logger LOG = LoggerFactory.getLogger(LazyMapObjectInspector.class
      .getName());

  private ObjectInspector mapKeyObjectInspector;
  private ObjectInspector mapValueObjectInspector;
  private byte itemSeparator;
  private byte keyValueSeparator;
  private LazyObjectInspectorParameters lazyParams;

  protected LazyMapObjectInspector() {
    super();
    lazyParams = new LazyObjectInspectorParametersImpl();
  }
  /**
   * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   */
  protected LazyMapObjectInspector(ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector, byte itemSeparator,
      byte keyValueSeparator, Text nullSequence, boolean escaped,
      byte escapeChar) {
    this.mapKeyObjectInspector = mapKeyObjectInspector;
    this.mapValueObjectInspector = mapValueObjectInspector;

    this.itemSeparator = itemSeparator;
    this.keyValueSeparator = keyValueSeparator;
    this.lazyParams = new LazyObjectInspectorParametersImpl(
        escaped, escapeChar, false, null, null, nullSequence);
  }

  protected LazyMapObjectInspector(ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector, byte itemSeparator,
      byte keyValueSeparator, LazyObjectInspectorParameters lazyParams) {
    this.mapKeyObjectInspector = mapKeyObjectInspector;
    this.mapValueObjectInspector = mapValueObjectInspector;

    this.itemSeparator = itemSeparator;
    this.keyValueSeparator = keyValueSeparator;
    this.lazyParams = lazyParams;
  }

  @Override
  public final Category getCategory() {
    return Category.MAP;
  }

  @Override
  public String getTypeName() {
    return org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME + "<"
        + mapKeyObjectInspector.getTypeName() + ","
        + mapValueObjectInspector.getTypeName() + ">";
  }

  @Override
  public ObjectInspector getMapKeyObjectInspector() {
    return mapKeyObjectInspector;
  }

  @Override
  public ObjectInspector getMapValueObjectInspector() {
    return mapValueObjectInspector;
  }

  @Override
  public Object getMapValueElement(Object data, Object key) {
    return ((data==null || key == null)? null : ((LazyMap) data).getMapValueElement(key));
  }

  @Override
  public Map<?, ?> getMap(Object data) {
    if (data == null) {
      return null;
    }
    return ((LazyMap) data).getMap();
  }

  @Override
  public int getMapSize(Object data) {
    if (data == null) {
      return -1;
    }
    return ((LazyMap) data).getMapSize();
  }

  // Called by LazyMap
  public byte getItemSeparator() {
    return itemSeparator;
  }

  public byte getKeyValueSeparator() {
    return keyValueSeparator;
  }

  public Text getNullSequence() {
    return lazyParams.getNullSequence();
  }

  public boolean isEscaped() {
    return lazyParams.isEscaped();
  }

  public byte getEscapeChar() {
    return lazyParams.getEscapeChar();
  }

  public LazyObjectInspectorParameters getLazyParams() {
    return lazyParams;
  }
}
