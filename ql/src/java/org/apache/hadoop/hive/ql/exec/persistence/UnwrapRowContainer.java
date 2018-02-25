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
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;

/**
 * Unwraps values from current key with valueIndex in mapjoin desc
 */
public class UnwrapRowContainer
    implements MapJoinRowContainer, AbstractRowContainer.RowIterator<List<Object>> {

  private final byte alias;
  private final int[] valueIndex;
  private final Converter[] converters;
  private final boolean tagged;
  private final List<Object> unwrapped;

  private transient Object[] currentKey;
  private transient MapJoinRowContainer internal;

  private transient RowIterator<List<Object>> iterator;

  public UnwrapRowContainer(byte alias, int[] valueIndex, Converter[] converters, boolean tagged)  {
    this.alias = alias;
    this.valueIndex = valueIndex;
    this.converters = converters;
    this.tagged = tagged;
    this.unwrapped = new ArrayList<Object>();
  }

  public MapJoinRowContainer setInternal(MapJoinRowContainer internal, Object[] currentKey) {
    this.internal = internal;
    this.currentKey = currentKey;
    return this;
  }

  @Override
  public List<Object> first() throws HiveException {
    iterator = internal.rowIter();
    return unwrap(iterator.first());
  }

  @Override
  public List<Object> next() throws HiveException {
    return unwrap(iterator.next());
  }

  private List<Object> unwrap(List<Object> values) {
    if (values == null) {
      return null;
    }
    unwrapped.clear();
    for (int pos = 0; pos < valueIndex.length; pos++) {
      int index = valueIndex[pos];
      if (index >= 0) {
        if (currentKey == null) {
          unwrapped.add(null);
        } else if (converters[pos] != null) {
          unwrapped.add(converters[pos].convert(currentKey[index]));
        } else {
          unwrapped.add(currentKey[index]);
        }
      } else {
        unwrapped.add(values.get(-index - 1));
      }
    }
    if (tagged) {
      unwrapped.add(values.get(values.size() - 1)); // append filter tag
    }
    return unwrapped;
  }

  @Override
  public RowIterator<List<Object>> rowIter() throws HiveException {
    return this;
  }

  @Override
  public void addRow(List<Object> t) throws HiveException {
    internal.addRow(t);
  }


  @Override
  public boolean hasRows() throws HiveException {
    return internal.hasRows();
  }

  @Override
  public boolean isSingleRow() throws HiveException {
    return internal.isSingleRow();
  }

  @Override
  public int rowCount() throws HiveException {
    return internal.rowCount();
  }

  @Override
  public void clearRows() throws HiveException {
    internal.clearRows();
  }

  @Override
  public byte getAliasFilter() throws HiveException {
    return internal.getAliasFilter();
  }

  @Override
  public MapJoinRowContainer copy() throws HiveException {
    internal = internal.copy();
    return this;
  }

  @Override
  public void addRow(Object[] value) throws HiveException {
    internal.addRow(value);
  }

  @Override
  public void write(MapJoinObjectSerDeContext valueContext, ObjectOutputStream out)
      throws IOException, SerDeException {
    internal.write(valueContext, out);
  }

  @Override
  public String toString() {
    return alias + (tagged ? ":TAGGED" : "") + Arrays.toString(valueIndex);
  }
}
