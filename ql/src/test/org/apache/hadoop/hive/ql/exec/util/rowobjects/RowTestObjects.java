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

import java.util.Arrays;

public final class RowTestObjects implements Comparable<RowTestObjects>{

    private final Object[] row;

    // Not included in equals.
    private int index;

    public RowTestObjects(Object[] row) {
      this.row = row;
      index = -1;   // Not used value.
    }

    public Object[] getRow() {
      return row;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public int hashCode() {
      int hashCode = Arrays.hashCode(row);
      return hashCode;
    }

    @Override
    public Object clone() {
      return new RowTestObjects(row);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof RowTestObjects)) {
        return false;
      }
      final RowTestObjects other = (RowTestObjects) obj;
      return Arrays.equals(this.row, other.row);
    }

    @Override
    public String toString() {
      return Arrays.toString(row);
    }

    @Override
    public int compareTo(RowTestObjects obj) {
      final RowTestObjects other = (RowTestObjects) obj;
      int thisLength = this.row.length;
      int otherLength = other.row.length;
      if (thisLength != otherLength) {
        return (thisLength < otherLength ? -1 : 1);
      }
      for (int i = 0; i < thisLength; i++) {
        Object thisObject = this.row[i];
        Object otherObject = other.row[i];
        if (thisObject == null || otherObject == null) {
          if (thisObject == null && otherObject == null) {
            continue;
          }
          // Does this make sense?
          return (thisObject == null ? -1 : 1);
        }
        int compareTo = ((Comparable) thisObject).compareTo((Comparable) otherObject);
        if (compareTo != 0) {
          return compareTo;
        }
      }
      return 0;
    }
  }
