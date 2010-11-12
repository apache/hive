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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class MapJoinRowContainer<Row> extends AbstractRowContainer<Row> {

  private List<Row> list;

  private int index;

  public MapJoinRowContainer() {
    index = 0;
    list = new ArrayList<Row>(1);
  }

  @Override
  public void add(Row t) throws HiveException {
    list.add(t);
  }


  @Override
  public Row first() throws HiveException {
    index = 0;
    if (index < list.size()) {
      return list.get(index);
    }
    return null;
  }

  @Override
  public Row next() throws HiveException {
    index++;
    if (index < list.size()) {
      return list.get(index);
    }
    return null;

  }

  /**
   * Get the number of elements in the RowContainer.
   *
   * @return number of elements in the RowContainer
   */
  @Override
  public int size() {
    return list.size();
  }

  /**
   * Remove all elements in the RowContainer.
   */
  @Override
  public void clear() throws HiveException {
    list.clear();
    index = 0;
  }

  public List<Row> getList() {
    return list;
  }

  public void setList(List<Row> list) {
    this.list = list;
  }

  public void reset(MapJoinRowContainer<Object[]> other) throws HiveException {
    list.clear();
    Object[] obj;
    for (obj = other.first(); obj != null; obj = other.next()) {
      ArrayList<Object> ele = new ArrayList(obj.length);
      for (int i = 0; i < obj.length; i++) {
        ele.add(obj[i]);
      }
      list.add((Row) ele);
    }
  }
}
