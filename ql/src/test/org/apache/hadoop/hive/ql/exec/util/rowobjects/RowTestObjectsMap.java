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

package org.apache.hadoop.hive.ql.exec.util.rowobjects;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.util.rowobjects.RowTestObjects;

public class RowTestObjectsMap {
  private SortedMap<RowTestObjects, Object> sortedMap;

  public RowTestObjectsMap() {
    sortedMap = new TreeMap<RowTestObjects, Object>();
  }

  public Object find(RowTestObjects testRow) {
    return sortedMap.get(testRow);
  }

  public void put(RowTestObjects testRow, Object object) {
    sortedMap.put(testRow, object);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RowTestObjectsMap)) {
      return false;
    }
    final RowTestObjectsMap other = (RowTestObjectsMap) obj;
    final int thisSize = this.sortedMap.size();
    final int otherSize = other.sortedMap.size();
    Iterator<Entry<RowTestObjects, Object>> thisIterator = this.sortedMap.entrySet().iterator();
    Iterator<Entry<RowTestObjects, Object>> otherIterator = other.sortedMap.entrySet().iterator();
    for (int i = 0; i < thisSize; i++) {
      Entry<RowTestObjects, Object> thisEntry = thisIterator.next();
      Entry<RowTestObjects, Object> otherEntry = otherIterator.next();
      if (!thisEntry.getKey().equals(otherEntry.getKey())) {
        return false;
      }
      // Check object.
      if (!thisEntry.getValue().equals(otherEntry.getValue())) {
        return false;
      }
    }
    if (thisSize != otherSize) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return sortedMap.toString();
  }
}