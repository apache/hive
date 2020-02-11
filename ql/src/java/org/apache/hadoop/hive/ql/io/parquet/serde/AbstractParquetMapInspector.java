/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public abstract class AbstractParquetMapInspector implements SettableMapObjectInspector {

  protected final ObjectInspector keyInspector;
  protected final ObjectInspector valueInspector;

  public AbstractParquetMapInspector(final ObjectInspector keyInspector, final ObjectInspector valueInspector) {
    this.keyInspector = keyInspector;
    this.valueInspector = valueInspector;
  }

  @Override
  public String getTypeName() {
    return "map<" + keyInspector.getTypeName() + "," + valueInspector.getTypeName() + ">";
  }

  @Override
  public Category getCategory() {
    return Category.MAP;
  }

  @Override
  public ObjectInspector getMapKeyObjectInspector() {
    return keyInspector;
  }

  @Override
  public ObjectInspector getMapValueObjectInspector() {
    return valueInspector;
  }

  @Override
  public Map<?, ?> getMap(final Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final Writable[] mapArray = ((ArrayWritable) data).get();
      if (mapArray == null) {
        return null;
      }

      final Map<Writable, Writable> map = new LinkedHashMap<Writable, Writable>();
      for (final Writable obj : mapArray) {
        final ArrayWritable mapObj = (ArrayWritable) obj;
        final Writable[] arr = mapObj.get();
        map.put(arr[0], arr[1]);
      }

      return map;
    }

    if (data instanceof Map) {
      return (Map) data;
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public int getMapSize(final Object data) {
    if (data == null) {
      return -1;
    }

    if (data instanceof ArrayWritable) {
      final Writable[] mapArray = ((ArrayWritable) data).get();

      if (mapArray == null) {
        return -1;
      } else {
        return mapArray.length;
      }
    }

    if (data instanceof Map) {
      return ((Map) data).size();
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public Object create() {
    Map<Object, Object> m = new LinkedHashMap<Object, Object>();
    return m;
  }

  @Override
  public Object put(Object map, Object key, Object value) {
    Map<Object, Object> m = (Map<Object, Object>) map;
    m.put(key, value);
    return m;
  }

  @Override
  public Object remove(Object map, Object key) {
    Map<Object, Object> m = (Map<Object, Object>) map;
    m.remove(key);
    return m;
  }

  @Override
  public Object clear(Object map) {
    Map<Object, Object> m = (Map<Object, Object>) map;
    m.clear();
    return m;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((keyInspector == null) ? 0 : keyInspector.hashCode());
    result = prime * result
        + ((valueInspector == null) ? 0 : valueInspector.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final AbstractParquetMapInspector other = (AbstractParquetMapInspector) obj;
    if (keyInspector == null) {
      if (other.keyInspector != null) {
        return false;
      }
    } else if (!keyInspector.equals(other.keyInspector)) {
      return false;
    }
    if (valueInspector == null) {
      if (other.valueInspector != null) {
        return false;
      }
    } else if (!valueInspector.equals(other.valueInspector)) {
      return false;
    }
    return true;
  }
}
