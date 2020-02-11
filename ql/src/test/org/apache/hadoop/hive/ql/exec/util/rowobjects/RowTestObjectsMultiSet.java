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

  public enum RowFlag {
    NONE        (0),
    REGULAR     (0x01),
    LEFT_OUTER  (0x02),
    FULL_OUTER  (0x04);

    public final long value;
    RowFlag(long value) {
      this.value = value;
    }
  }

  private static class Value {

    // Mutable.
    public int count;
    public long rowFlags;

    public final int initialKeyCount;
    public final int initialValueCount;
    public final RowFlag initialRowFlag;

    public Value(int count, RowFlag rowFlag, int totalKeyCount, int totalValueCount) {
      this.count = count;
      this.rowFlags = rowFlag.value;

      initialKeyCount = totalKeyCount;
      initialValueCount = totalValueCount;
      initialRowFlag = rowFlag;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("count ");
      sb.append(count);
      return sb.toString();
    }
  }

  private SortedMap<RowTestObjects, Value> sortedMap;
  private int totalKeyCount;
  private int totalValueCount;

  public RowTestObjectsMultiSet() {
    sortedMap = new TreeMap<RowTestObjects, Value>();
    totalKeyCount = 0;
    totalValueCount = 0;
  }

  public int getTotalKeyCount() {
    return totalKeyCount;
  }

  public int getTotalValueCount() {
    return totalValueCount;
  }

  public void add(RowTestObjects testRow, RowFlag rowFlag) {
    if (sortedMap.containsKey(testRow)) {
      Value value = sortedMap.get(testRow);
      value.count++;
      value.rowFlags |= rowFlag.value;
      totalValueCount++;
    } else {
      sortedMap.put(testRow, new Value(1, rowFlag, ++totalKeyCount, ++totalValueCount));
    }

  }

  public void add(RowTestObjects testRow, int count) {
    if (sortedMap.containsKey(testRow)) {
      throw new RuntimeException();
    }
    sortedMap.put(testRow, new Value(count, RowFlag.NONE, ++totalKeyCount, ++totalValueCount));
  }

  public String displayRowFlags(long rowFlags) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (RowFlag rowFlag : RowFlag.values()) {
      if ((rowFlags & rowFlag.value) != 0) {
        if (sb.length() > 1) {
          sb.append(", ");
        }
        sb.append(rowFlag.name());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  public boolean verify(RowTestObjectsMultiSet other, String left, String right) {

    final int thisSize = this.sortedMap.size();
    final int otherSize = other.sortedMap.size();
    if (thisSize != otherSize) {
      System.out.println("*BENCHMARK* " + left + " count " + thisSize + " doesn't match " + right + " " + otherSize);
      return false;
    }
    Iterator<Entry<RowTestObjects, Value>> thisIterator = this.sortedMap.entrySet().iterator();
    Iterator<Entry<RowTestObjects, Value>> otherIterator = other.sortedMap.entrySet().iterator();
    for (int i = 0; i < thisSize; i++) {
      Entry<RowTestObjects, Value> thisEntry = thisIterator.next();
      Entry<RowTestObjects, Value> otherEntry = otherIterator.next();
      if (!thisEntry.getKey().equals(otherEntry.getKey())) {
        System.out.println("*BENCHMARK* " + left + " row " + thisEntry.getKey().toString() +
            " (rowFlags " + displayRowFlags(thisEntry.getValue().rowFlags) +
            " count " + thisEntry.getValue().count + ")" +
            " but found " + right + " row " + otherEntry.getKey().toString() +
            " (initialKeyCount " + + otherEntry.getValue().initialKeyCount +
            " initialValueCount " + otherEntry.getValue().initialValueCount + ")");
        return false;
      }
      // Check multi-set count.
      if (thisEntry.getValue().count != otherEntry.getValue().count) {
        System.out.println("*BENCHMARK* " + left + " row " + thisEntry.getKey().toString() +
            " count " + thisEntry.getValue().count +
            " (rowFlags " + displayRowFlags(thisEntry.getValue().rowFlags) + ")" +
            " doesn't match " + right + " row count " + otherEntry.getValue().count +
            " (initialKeyCount " + + otherEntry.getValue().initialKeyCount +
            " initialValueCount " + otherEntry.getValue().initialValueCount + ")");
        return false;
      }
    }
    if (thisSize != otherSize) {
      return false;
    }
    return true;
  }

  public RowTestObjectsMultiSet subtract(RowTestObjectsMultiSet other) {
    RowTestObjectsMultiSet result = new RowTestObjectsMultiSet();

    Iterator<Entry<RowTestObjects, Value>> thisIterator = this.sortedMap.entrySet().iterator();
    while (thisIterator.hasNext()) {
      Entry<RowTestObjects, Value> thisEntry = thisIterator.next();

      if (other.sortedMap.containsKey(thisEntry.getKey())) {
        Value thisValue = thisEntry.getValue();
        Value otherValue = other.sortedMap.get(thisEntry.getKey());
        if (thisValue.count == otherValue.count) {
          continue;
        }
      }
      result.add(thisEntry.getKey(), thisEntry.getValue().count);
    }

    return result;
  }

  public void displayDifferences(RowTestObjectsMultiSet other, String left, String right) {

    RowTestObjectsMultiSet leftOnly = this.subtract(other);
    Iterator<Entry<RowTestObjects, Value>> leftOnlyIterator =
        leftOnly.sortedMap.entrySet().iterator();
    while (leftOnlyIterator.hasNext()) {
      Entry<RowTestObjects, Value> leftOnlyEntry = leftOnlyIterator.next();
      System.out.println(
          "*BENCHMARK* " + left + " only row " + leftOnlyEntry.getKey().toString() +
          " count " + leftOnlyEntry.getValue().count +
          " (initialRowFlag " + leftOnlyEntry.getValue().initialRowFlag.name() + ")");
    }

    RowTestObjectsMultiSet rightOnly = other.subtract(this);
    Iterator<Entry<RowTestObjects, Value>> rightOnlyIterator =
        rightOnly.sortedMap.entrySet().iterator();
    while (rightOnlyIterator.hasNext()) {
      Entry<RowTestObjects, Value> rightOnlyEntry = rightOnlyIterator.next();
      System.out.println(
          "*BENCHMARK* " + right + " only row " + rightOnlyEntry.getKey().toString() +
          " count " + rightOnlyEntry.getValue().count +
          " (initialRowFlag " + rightOnlyEntry.getValue().initialRowFlag.name() + ")");
    }
  }

  @Override
  public String toString() {
    return sortedMap.toString();
  }
}