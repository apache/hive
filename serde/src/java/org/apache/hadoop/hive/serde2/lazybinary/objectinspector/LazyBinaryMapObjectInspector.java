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
package org.apache.hadoop.hive.serde2.lazybinary.objectinspector;

import java.util.Map;

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;

/**
 * ObjectInspector for LazyBinaryMap.
 * 
 * @see LazyBinaryMap
 */
public class LazyBinaryMapObjectInspector extends StandardMapObjectInspector {

  protected LazyBinaryMapObjectInspector() {
    super();
  }
  protected LazyBinaryMapObjectInspector(ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector) {
    super(mapKeyObjectInspector, mapValueObjectInspector);
  }

  @Override
  public Map<?, ?> getMap(Object data) {
    if (data == null) {
      return null;
    }
    return ((LazyBinaryMap) data).getMap();
  }

  @Override
  public int getMapSize(Object data) {
    if (data == null) {
      return -1;
    }
    return ((LazyBinaryMap) data).getMapSize();
  }

  @Override
  public Object getMapValueElement(Object data, Object key) {
    if (data == null) {
      return -1;
    }
    return ((LazyBinaryMap) data).getMapValueElement(key);
  }
}
