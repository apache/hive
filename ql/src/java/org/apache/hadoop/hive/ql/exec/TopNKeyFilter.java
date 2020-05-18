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
package org.apache.hadoop.hive.ql.exec;

import com.google.common.annotations.VisibleForTesting;

import static java.util.Arrays.binarySearch;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of filtering out keys.
 * An instance of this class is wrapped in {@link TopNKeyOperator} and
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorTopNKeyOperator}
 */
public final class TopNKeyFilter {
  private final int topN;
  private Comparator<? extends KeyWrapper> comparator;
  private KeyWrapper[] sortedTopItems;
  private int size = 0;
  private long repeated = 0;
  private long added = 0;
  private long total = 0;

  //to track effectiveness of boundary check
  private long eff = 0;
  private final Set<KeyWrapper> topNKeySet;

  public TopNKeyFilter(int topN, Comparator<? extends KeyWrapper> comparator) {
    this.comparator = comparator;
    this.sortedTopItems = new KeyWrapper[topN +1];
    this.topN = topN;
    this.topNKeySet = new HashSet<>();
  }

  private int compareWithBoundary(KeyWrapper kw) {
    return ((Comparator<? super KeyWrapper>) comparator).compare(kw, sortedTopItems[topN - 1]);
  }

  public final boolean canForward(KeyWrapper kw) {
    total++;
    if (topN > 0 && (size == topN)) {
      int comp = compareWithBoundary(kw);
      if (comp == 0) {
        // special case, if last element is same as kw;
        // Avoids duplicate comparison later
        eff++;
        return true;
      }

      if (comp > 0) {
        eff++;
        return false;
      }
    }

    if (topNKeySet.contains(kw)) {
      repeated++;
      return true;
    }

    int pos = binarySearch(sortedTopItems, 0, size, kw, (Comparator<? super KeyWrapper>) comparator);
    if (pos >= 0) { // found
      repeated++;
      return true;
    }
    pos = -pos -1; // not found, calculate insertion point
    if (pos >= topN) { // would be inserted to the end, there are topN elements which are smaller/larger
      return false;
    }
    KeyWrapper oldElement = sortedTopItems[pos];
    System.arraycopy(sortedTopItems, pos, sortedTopItems, pos +1, size - pos); // make space by shifting
    sortedTopItems[pos] = kw.copyKey();

    topNKeySet.remove(oldElement);
    topNKeySet.add(sortedTopItems[pos]);

    added++;
    if (size < topN) {
      size++;
    }
    return true;
  }

  public void clear() {
    this.size = 0;
    this.repeated = 0;
    this.added = 0;
    this.total = 0;
    Arrays.fill(sortedTopItems, null);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("TopNKeyFilter{");
    sb.append("id=").append(super.toString());
    sb.append(", topN=").append(topN);
    sb.append(", repeated=").append(repeated);
    sb.append(", added=").append(added);
    sb.append(", total=").append(total);
    sb.append(", eff=").append(eff);
    sb.append(", forwardingRatio=").append(forwardingRatio());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Ratio between the forwarded rows and the total incoming rows.
   * The higher the number is, the less is the efficiency of the filter.
   * 1 means all rows should be forwarded.
   * @return
   */
  public float forwardingRatio() {
    if (total == 0) {
      return 0;
    }
    return ((repeated + added) / (float)total);
  }

  public long getTotal() {
    return total;
  }

  @VisibleForTesting
  long getEffectiveBoundaryChecks() {
    return eff;
  }

  @VisibleForTesting
  long getRepeated() {
    return repeated;
  }

  @VisibleForTesting
  long getKeySetSize() {
    return topNKeySet.size();
  }
}
