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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.Map;

public class DelegatedMapObjectInspector implements MapObjectInspector {

  private MapObjectInspector delegate;
  private ObjectInspector key;
  private ObjectInspector value;

  protected DelegatedMapObjectInspector() {
    super();
  }
  public DelegatedMapObjectInspector(MapObjectInspector delegate) {
    this.delegate = delegate;
  }

  public void reset(MapObjectInspector current) {
    this.delegate = current;
    if (key != null) {
      DelegatedObjectInspectorFactory.reset(key, current.getMapKeyObjectInspector());
    }
    if (value != null) {
      DelegatedObjectInspectorFactory.reset(value, current.getMapValueObjectInspector());
    }
  }

  public ObjectInspector getMapKeyObjectInspector() {
    return key != null ? key :
        (key = DelegatedObjectInspectorFactory.wrap(delegate.getMapKeyObjectInspector()));
  }

  public ObjectInspector getMapValueObjectInspector() {
    return value != null ? value :
        (value = DelegatedObjectInspectorFactory.wrap(delegate.getMapValueObjectInspector()));
  }

  public Object getMapValueElement(Object data, Object key) {
    return delegate.getMapValueElement(data, key);
  }

  public Map<?, ?> getMap(Object data) {
    return delegate.getMap(data);
  }

  public int getMapSize(Object data) {
    return delegate.getMapSize(data);
  }

  public String getTypeName() {
    return delegate.getTypeName();
  }

  public Category getCategory() {
    return delegate.getCategory();
  }
}
