/*
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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public class MapJoinEagerRowContainer
    implements MapJoinRowContainer, AbstractRowContainer.RowIterator<List<Object>> {
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  private final List<List<Object>> list;
  private byte aliasFilter = (byte) 0xff;
  private int index = 0;

  public MapJoinEagerRowContainer() {
    index = 0;
    list = new ArrayList<List<Object>>(1);
  } 

  @Override
  public void addRow(List<Object> t) {
    list.add(t);
  }

  @Override
  public void addRow(Object[] t) {
    addRow(toList(t));
  }

  @Override
  public AbstractRowContainer.RowIterator<List<Object>> rowIter() {
    return this;
  }

  @Override
  public List<Object> first() {
    index = 0;
    if (index < list.size()) {
      return list.get(index);
    }
    return null;
  }

  @Override
  public List<Object> next() {
    index++;
    if (index < list.size()) {
      return list.get(index);
    }
    return null;
  }

  @Override
  public boolean hasRows() {
    return list.size() > 0;
  }

  @Override
  public boolean isSingleRow() {
    return list.size() == 1;
  }

  /**
   * Get the number of elements in the RowContainer.
   *
   * @return number of elements in the RowContainer
   */
  @Override
  public int rowCount() {
    return list.size();
  }

  /**
   * Remove all elements in the RowContainer.
   */
  @Override
  public void clearRows() {
    list.clear();
  }

  @Override
  public byte getAliasFilter() {
    return aliasFilter;
  }

  @Override
  public MapJoinRowContainer copy() {
    MapJoinEagerRowContainer result = new MapJoinEagerRowContainer();
    for(List<Object> item : list) {
      result.addRow(item);
    }
    return result;
  }

  public void read(MapJoinObjectSerDeContext context, ObjectInputStream in, Writable container)
  throws IOException, SerDeException {
    long numRows = in.readLong();
    for (long rowIndex = 0L; rowIndex < numRows; rowIndex++) {
      container.readFields(in);
      read(context, container);
    }
  }

  @SuppressWarnings("unchecked")
  public void read(MapJoinObjectSerDeContext context, Writable currentValue) throws SerDeException {
    AbstractSerDe serde = context.getSerDe();
    List<Object> value = (List<Object>)ObjectInspectorUtils.copyToStandardObject(serde.deserialize(currentValue),
        serde.getObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
    if(value == null) {
      addRow(toList(EMPTY_OBJECT_ARRAY));
    } else {
      Object[] valuesArray = value.toArray();
      if (context.hasFilterTag()) {
        aliasFilter &= ((ShortWritable)valuesArray[valuesArray.length - 1]).get();
      }
      addRow(toList(valuesArray));
    }
  }

  @Override
  public void write(MapJoinObjectSerDeContext context, ObjectOutputStream out)
  throws IOException, SerDeException {
    AbstractSerDe serde = context.getSerDe();
    ObjectInspector valueObjectInspector = context.getStandardOI();
    long numRows = rowCount();
    long numRowsWritten = 0L;
    out.writeLong(numRows);
    for (List<Object> row = first(); row != null; row = next()) {
      serde.serialize(row.toArray(), valueObjectInspector).write(out);
      ++numRowsWritten;      
    }
    if(numRows != rowCount()) {
      throw new ConcurrentModificationException("Values was modified while persisting");
    }
    if(numRowsWritten != numRows) {
      throw new IllegalStateException("Expected to write " + numRows + " but wrote " + numRowsWritten);
    }
  }
  
  private List<Object> toList(Object[] array) {
    return new NoCopyingArrayList(array);
  }
  /**
   * In this use case our objects will not be modified
   * so we don't care about copying in and out.
   */
  private static class NoCopyingArrayList extends AbstractList<Object> {
    private Object[] array;
    public NoCopyingArrayList(Object[] array) {
      this.array = array;
    }
    @Override
    public Object get(int index) {
      return array[index];
    }

    @Override
    public int size() {
      return array.length;
    }
    
    public Object[] toArray() {
      return array;
    }    
  }
}
