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

public class RowTestObjectsMultiSet {
  private SortedMap<RowTestObjects, Integer> sortedMap;
  private int rowCount;
  private int totalCount;

  public RowTestObjectsMultiSet() {
    sortedMap = new TreeMap<RowTestObjects, Integer>();
    rowCount = 0;
    totalCount = 0;
  }

  public int getRowCount() {
    return rowCount;
  }

  public int getTotalCount() {
    return totalCount;
  }

  public void add(RowTestObjects testRow) {
    if (sortedMap.containsKey(testRow)) {
      Integer count = sortedMap.get(testRow);
      count++;
    } else {
      sortedMap.put(testRow, 1);
      rowCount++;
    }
    totalCount++;
  }

  public boolean verify(RowTestObjectsMultiSet other) {

    final int thisSize = this.sortedMap.size();
    final int otherSize = other.sortedMap.size();
    if (thisSize != otherSize) {
      System.out.println("*VERIFY* count " + thisSize + " doesn't match otherSize " + otherSize);
      return false;
    }
    Iterator<Entry<RowTestObjects, Integer>> thisIterator = this.sortedMap.entrySet().iterator();
    Iterator<Entry<RowTestObjects, Integer>> otherIterator = other.sortedMap.entrySet().iterator();
    for (int i = 0; i < thisSize; i++) {
      Entry<RowTestObjects, Integer> thisEntry = thisIterator.next();
      Entry<RowTestObjects, Integer> otherEntry = otherIterator.next();
      if (!thisEntry.getKey().equals(otherEntry.getKey())) {
        System.out.println("*VERIFY* thisEntry.getKey() " + thisEntry.getKey() + " doesn't match otherEntry.getKey() " + otherEntry.getKey());
        return false;
      }
      // Check multi-set count.
      if (!thisEntry.getValue().equals(otherEntry.getValue())) {
        System.out.println("*VERIFY* key " + thisEntry.getKey() + " count " + thisEntry.getValue() + " doesn't match " + otherEntry.getValue());
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