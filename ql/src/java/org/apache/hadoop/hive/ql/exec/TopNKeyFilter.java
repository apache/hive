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

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Implementation of filtering out keys.
 * An instance of this class is wrapped in {@link TopNKeyOperator} and
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorTopNKeyOperator}
 * @param <T> - Type of {@link KeyWrapper}. Each key is stored in a KeyWrapper instance.
 */
public class TopNKeyFilter<T extends KeyWrapper> {
  private final PriorityQueue<T> priorityQueue;
  private final int topN;

  public TopNKeyFilter(int topN, Comparator<T> comparator) {
    // We need a reversed comparator because the PriorityQueue.poll() method is used for filtering out keys.
    // Ex.: When ORDER BY key1 ASC then call of poll() should remove the largest key.
    this.priorityQueue = new PriorityQueue<>(topN + 1, comparator.reversed());
    this.topN = topN;
  }

  public boolean canForward(T kw) {
    if (!priorityQueue.contains(kw)) {
      priorityQueue.offer((T) kw.copyKey());
    }
    if (priorityQueue.size() > topN) {
      priorityQueue.poll();
    }

    return priorityQueue.contains(kw);
  }

  public void clear() {
    priorityQueue.clear();
  }
}
