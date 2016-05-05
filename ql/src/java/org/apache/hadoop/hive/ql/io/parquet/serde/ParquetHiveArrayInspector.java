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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * The ParquetHiveArrayInspector will inspect an ArrayWritable, considering it as an Hive array.<br />
 * It can also inspect a List if Hive decides to inspect the result of an inspection.
 *
 */
public class ParquetHiveArrayInspector implements SettableListObjectInspector {

  ObjectInspector arrayElementInspector;

  public ParquetHiveArrayInspector(final ObjectInspector arrayElementInspector) {
    this.arrayElementInspector = arrayElementInspector;
  }

  @Override
  public String getTypeName() {
    return "array<" + arrayElementInspector.getTypeName() + ">";
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return arrayElementInspector;
  }

  @Override
  public Object getListElement(final Object data, final int index) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final Writable[] array = ((ArrayWritable) data).get();
      if (array == null || array.length == 0) {
        return null;
      }

      if (index >= 0 && index < array.length) {
        return array[index];
      } else {
        return null;
      }
    }

    if (data instanceof List) {
      return ((List)data).get(index);
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public int getListLength(final Object data) {
    if (data == null) {
      return -1;
    }

    if (data instanceof ArrayWritable) {
      final Writable[] array = ((ArrayWritable) data).get();
      if (array == null) {
        return -1;
      }

      return array.length;
    }

    if (data instanceof List) {
      return ((List)data).size();
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public List<?> getList(final Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof ArrayWritable) {
      final Writable[] array = ((ArrayWritable) data).get();
      if (array == null) {
        return null;
      }

      final List<Writable> list = new ArrayList<Writable>(array.length);
      for (final Writable obj : array) {
        list.add(obj);
      }

      return list;
    }

    if (data instanceof List) {
      return (List<?>)data;
    }

    throw new UnsupportedOperationException("Cannot inspect " + data.getClass().getCanonicalName());
  }

  @Override
  public Object create(final int size) {
    final ArrayList<Object> result = new ArrayList<Object>(size);
    for (int i = 0; i < size; ++i) {
      result.add(null);
    }
    return result;
  }

  @Override
  public Object set(final Object list, final int index, final Object element) {
    final ArrayList l = (ArrayList) list;
    l.set(index, element);
    return list;
  }

  @Override
  public Object resize(final Object list, final int newSize) {
    final ArrayList l = (ArrayList) list;
    l.ensureCapacity(newSize);
    while (l.size() < newSize) {
      l.add(null);
    }
    while (l.size() > newSize) {
      l.remove(l.size() - 1);
    }
    return list;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      final ObjectInspector other = ((ParquetHiveArrayInspector) o).arrayElementInspector;
      return other.equals(arrayElementInspector);
    }
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 29 * hash + (this.arrayElementInspector != null ? this.arrayElementInspector.hashCode() : 0);
    return hash;
  }
}
