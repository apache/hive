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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.ObjectOutputStream;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public class FlatRowContainer extends AbstractCollection<Object>
    implements MapJoinRowContainer, AbstractRowContainer.RowIterator<List<Object>>, List<Object> {
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
  private static final int UNKNOWN = Integer.MAX_VALUE;

  private static Log LOG = LogFactory.getLog(FlatRowContainer.class);

  /**
   * In lazy mode, 0s element contains context for deserialization and all the other
   * elements contains byte arrays to be deserialized. After deserialization, the array
   * contains row count * row size elements - a matrix of rows stored.
   */
  private Object[] array;
  /**
   * This is kind of tricky. UNKNOWN number means unknown. Other positive numbers represent
   * row length (see array javadoc). Non-positive numbers mean row length is zero (thus,
   * array is empty); they represent (negated) number of rows (for joins w/o projections).
   */
  private int rowLength = UNKNOWN;
  private byte aliasFilter = (byte) 0xff;
  private boolean isAliasFilterSet = true; // by default assume no filter tag so we are good

  public FlatRowContainer() {
    this.array = EMPTY_OBJECT_ARRAY;
  }

  /** Called when loading the hashtable. */
  public void add(MapJoinObjectSerDeContext context,
      BytesWritable value) throws HiveException {
    SerDe serde = context.getSerDe();
    isAliasFilterSet = !context.hasFilterTag(); // has tag => need to set later
    if (rowLength == UNKNOWN) {
      try {
        rowLength = ObjectInspectorUtils.getStructSize(serde.getObjectInspector());
      } catch (SerDeException ex) {
        throw new HiveException("Get structure size error", ex);
      }
      if (rowLength == 0) {
        array = EMPTY_OBJECT_ARRAY;
      }
    }
    if (rowLength > 0) {
      int rowCount = (array.length / rowLength);
      listRealloc(array.length + rowLength);
      read(serde, value, rowCount);
    } else {
      --rowLength; // see rowLength javadoc
    }
  }

  // Implementation of AbstractRowContainer and assorted methods

  @Override
  public void addRow(List<Object> t) throws HiveException {
    LOG.debug("Add is called with " + t.size() + " objects");
    // This is not called when building HashTable; we don't expect it to be called ever.
    int offset = prepareForAdd(t.size());
    if (offset < 0) return;
    for (int i = 0; i < t.size(); ++i) {
      this.array[offset + i] = t.get(i);
    }
  }

  @Override
  public void addRow(Object[] value) throws HiveException {
    LOG.debug("Add is called with " + value.length + " objects");
    // This is not called when building HashTable; we don't expect it to be called ever.
    int offset = prepareForAdd(value.length);
    if (offset < 0) return;
    System.arraycopy(value, 0, this.array, offset, value.length);
  }

  private int prepareForAdd(int len) throws HiveException {
    if (rowLength < 0) {
      if (len != 0) {
        throw new HiveException("Different size rows: 0 and " + len);
      }
      --rowLength; // see rowLength javadoc
      return -1;
    }
    if (rowLength != len) {
      throw new HiveException("Different size rows: " + rowLength + " and " + len);
    }
    int oldLen = this.array.length;
    listRealloc(oldLen + len);
    return oldLen;
  }

  @Override
  public void write(MapJoinObjectSerDeContext valueContext, ObjectOutputStream out) {
    throw new UnsupportedOperationException(this.getClass().getName() + " cannot be serialized");
  }

  @Override
  public AbstractRowContainer.RowIterator<List<Object>> rowIter() throws HiveException {
    if (array.length == rowLength) {
      // optimize for common case - just one row for a key, container acts as iterator
      return this;
    }
    return rowLength > 0 ? new RowIterator() : new EmptyRowIterator(-rowLength);
  }

  @Override
  public List<Object> first() throws HiveException {
    if (array.length != rowLength) {
      throw new AssertionError("Incorrect iterator usage, not single-row");
    }
    return this; // optimize for common case - just one row for a key, container acts as row
  }

  @Override
  public List<Object> next() {
    return null; // single-row case, there's no next
  }

  /** Iterator for row length 0. */
  private static class EmptyRowIterator implements AbstractRowContainer.RowIterator<List<Object>> {
    private static final List<Object> EMPTY_ROW = new ArrayList<Object>();
    private int rowCount;
    public EmptyRowIterator(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public List<Object> first() throws HiveException {
      return next();
    }

    @Override
    public List<Object> next() throws HiveException {
      return (--rowCount < 0) ? null : EMPTY_ROW;
    }
  }

  /** Row iterator for non-zero-length rows. */
  private class RowIterator implements AbstractRowContainer.RowIterator<List<Object>> {
    private int index = 0;

    @Override
    public List<Object> first() throws HiveException {
      index = 0;
      if (array.length > 0) {
        return new ReadOnlySubList(0, rowLength);
      }
      return null;
    }

    @Override
    public List<Object> next() {
      index += rowLength;
      if (index < array.length) {
        return new ReadOnlySubList(index, rowLength);
      }
      return null;
    }
  }

  private void read(SerDe serde, Writable writable, int rowOffset) throws HiveException {
    try {
      ObjectInspectorUtils.copyStructToArray(
          serde.deserialize(writable), serde.getObjectInspector(),
          ObjectInspectorCopyOption.WRITABLE, this.array, rowOffset * rowLength);
    } catch (SerDeException ex) {
      throw new HiveException("Lazy deserialize error", ex);
    }
  }

  @Override
  public int rowCount() throws HiveException {
    return rowLength > 0 ? (array.length / rowLength) : -rowLength; // see rowLength javadoc
  }

  @Override
  public void clearRows() {
    array = EMPTY_OBJECT_ARRAY;
    rowLength = 0;
  }

  @Override
  public byte getAliasFilter() throws HiveException {
    ensureAliasFilter();
    return this.aliasFilter;
  }

  private void ensureAliasFilter() throws HiveException {
    if (!isAliasFilterSet && rowLength > 0) {
      for (int offset = rowLength - 1; offset < array.length; offset += rowLength) {
        aliasFilter &= ((ShortWritable)array[offset]).get();
      }
    }
    isAliasFilterSet = true;
  }

  @Override
  public MapJoinRowContainer copy() throws HiveException {
    FlatRowContainer result = new FlatRowContainer();
    result.array = new Object[this.array.length];
    System.arraycopy(this.array, 0, result.array, 0, this.array.length);
    result.rowLength = rowLength;
    result.aliasFilter = aliasFilter;
    return result;
  }

  // Implementation of List<Object> and assorted methods

  private void listRealloc(int length) {
    Object[] array = new Object[length];
    if (this.array.length > 0) {
      System.arraycopy(this.array, 0, array, 0, this.array.length);
    }
    this.array = array;
  }

  @Override
  public int size() {
    checkSingleRow();
    return array.length;
  }

  @Override
  public Object get(int index) {
    return array[index];
  }

  private class ReadOnlySubList extends AbstractList<Object> {
    private int offset;
    private int size;

    ReadOnlySubList(int from, int size) {
      this.offset = from;
      this.size = size;
    }

    public Object get(int index) {
      return array[index + offset];
    }

    public int size() {
      return size;
    }

    public Iterator<Object> iterator() {
      return listIterator();
    }

    public ListIterator<Object> listIterator(int index) {
      return listIteratorInternal(offset + index, offset, offset + size);
    }

    public List<Object> subList(int fromIndex, int toIndex) {
      return new ReadOnlySubList(offset + fromIndex, toIndex - fromIndex);
    }

    public Object[] toArray() {
      Object[] result = new Object[size];
      System.arraycopy(array, offset, result, 0, size);
      return result;
    }
  } // end ReadOnlySubList

  @Override
  public Object[] toArray() {
    checkSingleRow();
    return array;
  }

  @Override
  public Iterator<Object> iterator() {
    return listIterator();
  }

  @Override
  public ListIterator<Object> listIterator() {
    return listIterator(0);
  }

  @Override
  public ListIterator<Object> listIterator(final int index) {
    checkSingleRow();
    return listIteratorInternal(index, 0, array.length);
  }

  private ListIterator<Object> listIteratorInternal(
      final int index, final int iterMinPos, final int iterMaxPos) {
    return new ListIterator<Object>() {
      private int pos = index - 1;
      public int nextIndex() {
        return pos + 1;
      }
      public int previousIndex() {
        return pos - 1;
      }
      public boolean hasNext() {
        return nextIndex() < iterMaxPos;
      }
      public boolean hasPrevious() {
        return previousIndex() >= iterMinPos;
      }
      public Object next() {
        if (!hasNext()) throw new NoSuchElementException();
        return get(++pos);
      }
      public Object previous() {
        if (!hasPrevious()) throw new NoSuchElementException();
        return get(--pos);
      }

      public void remove() { throw new UnsupportedOperationException(); }
      public void set(Object e) { throw new UnsupportedOperationException(); }
      public void add(Object e) { throw new UnsupportedOperationException(); }
    }; // end ListIterator
  }

  @Override
  public int indexOf(Object o) {
    checkSingleRow();
    for (int i = 0; i < array.length; ++i) {
      if (o == null) {
        if (array[i] == null) return i;
      } else {
        if (o.equals(array[i])) return i;
      }
    }
    return -1;
  }

  private void checkSingleRow() throws AssertionError {
    if (array.length != rowLength) {
      throw new AssertionError("Incorrect list usage, not single-row");
    }
  }

  @Override
  public int lastIndexOf(Object o) {
    checkSingleRow();
    for (int i = array.length - 1; i >= 0; --i) {
      if (o == null) {
        if (array[i] == null) return i;
      } else {
        if (o.equals(array[i])) return i;
      }
    }
    return -1;
  }

  @Override
  public List<Object> subList(int fromIndex, int toIndex) {
    checkSingleRow();
    return new ReadOnlySubList(fromIndex, toIndex - fromIndex);
  }

  public boolean addAll(int index, Collection<? extends Object> c) {
    throw new UnsupportedOperationException();
  }
  public Object set(int index, Object element) { throw new UnsupportedOperationException(); }
  public void add(int index, Object element) { throw new UnsupportedOperationException(); }
  public Object remove(int index) { throw new UnsupportedOperationException(); }
}

