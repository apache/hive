/**
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

import java.util.HashMap;
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
      final Writable[] mapContainer = ((ArrayWritable) data).get();

      if (mapContainer == null || mapContainer.length == 0) {
        return null;
      }

      final Writable[] mapArray = ((ArrayWritable) mapContainer[0]).get();
      final Map<Writable, Writable> map = new HashMap<Writable, Writable>();

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
      final Writable[] mapContainer = ((ArrayWritable) data).get();

      if (mapContainer == null || mapContainer.length == 0) {
        return -1;
      } else {
        return ((ArrayWritable) mapContainer[0]).get().length;
      }
    }

    if (data instanceof Map) {
      return ((Map) data).size();
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public Object create() {
    Map<Object, Object> m = new HashMap<Object, Object>();
    return m;
  }

  @Override
  public Object put(Object map, Object key, Object value) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.put(key, value);
    return m;
  }

  @Override
  public Object remove(Object map, Object key) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.remove(key);
    return m;
  }

  @Override
  public Object clear(Object map) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.clear();
    return m;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final StandardParquetHiveMapInspector other = (StandardParquetHiveMapInspector) obj;
    if (this.keyInspector != other.keyInspector &&
        (this.keyInspector == null || !this.keyInspector.equals(other.keyInspector))) {
      return false;
    }
    if (this.valueInspector != other.valueInspector &&
        (this.valueInspector == null || !this.valueInspector.equals(other.valueInspector))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 59 * hash + (this.keyInspector != null ? this.keyInspector.hashCode() : 0);
    hash = 59 * hash + (this.valueInspector != null ? this.valueInspector.hashCode() : 0);
    return hash;
  }
}
