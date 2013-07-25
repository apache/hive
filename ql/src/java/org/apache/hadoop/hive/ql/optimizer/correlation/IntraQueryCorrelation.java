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

package org.apache.hadoop.hive.ql.optimizer.correlation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;

/**
 * IntraQueryCorrelation records a sub-tree of the query plan tree which can be
 * evaluated in a single MR job. The boundary of this sub-tree is recorded by
 * the ReduceSinkOperators the the bottom of this sub-tree.
 * Also, allReduceSinkOperators in IntraQueryCorrelation contains all
 * ReduceSinkOperators of this sub-tree.
 */
public class IntraQueryCorrelation {
  private boolean jobFlowCorrelation;

  // The bottom layer ReduceSinkOperators. These ReduceSinkOperators are used
  // to record the boundary of this sub-tree which can be evaluated in a single MR
  // job.
  private List<ReduceSinkOperator> bottomReduceSinkOperators;

  // The number of reducer(s) should be used for those bottom layer ReduceSinkOperators
  private int numReducers;
  // This is the min number of reducer(s) for the bottom layer ReduceSinkOperators to avoid query
  // executed on too small number of reducers.
  private final int minReducers;

  // All ReduceSinkOperators in this sub-tree. This set is used when we start to remove unnecessary
  // ReduceSinkOperators.
  private final Set<ReduceSinkOperator> allReduceSinkOperators;

  // Since we merge multiple operation paths, we assign new tags to bottom layer
  // ReduceSinkOperatos. This mapping is used to map new tags to original tags associated
  // to these bottom layer ReduceSinkOperators.
  private final Map<Integer, Integer> newTagToOldTag;

  // A map from new tags to indices of children of DemuxOperator (the first Operator at the
  // Reduce side of optimized plan)
  private final Map<Integer, Integer> newTagToChildIndex;

  public IntraQueryCorrelation(int minReducers) {
    this.jobFlowCorrelation = false;
    this.numReducers = -1;
    this.minReducers = minReducers;
    this.allReduceSinkOperators = new HashSet<ReduceSinkOperator>();
    this.newTagToOldTag = new HashMap<Integer, Integer>();
    this.newTagToChildIndex = new HashMap<Integer, Integer>();
  }

  public Map<Integer, Integer> getNewTagToOldTag() {
    return newTagToOldTag;
  }

  public Map<Integer, Integer> getNewTagToChildIndex() {
    return newTagToChildIndex;
  }

  public void setNewTag(Integer newTag, Integer oldTag, Integer childIndex) {
    newTagToOldTag.put(newTag, oldTag);
    newTagToChildIndex.put(newTag, childIndex);
  }
  public void addToAllReduceSinkOperators(ReduceSinkOperator rsop) {
    allReduceSinkOperators.add(rsop);
  }

  public Set<ReduceSinkOperator> getAllReduceSinkOperators() {
    return allReduceSinkOperators;
  }

  public void setJobFlowCorrelation(boolean jobFlowCorrelation,
      List<ReduceSinkOperator> bottomReduceSinkOperators) {
    this.jobFlowCorrelation = jobFlowCorrelation;
    this.bottomReduceSinkOperators = bottomReduceSinkOperators;
  }

  public boolean hasJobFlowCorrelation() {
    return jobFlowCorrelation;
  }

  public List<ReduceSinkOperator> getBottomReduceSinkOperators() {
    return bottomReduceSinkOperators;
  }

  public int getNumReducers() {
    return numReducers;
  }

  public boolean adjustNumReducers(int newNumReducers) {
    assert newNumReducers != 0;
    if (newNumReducers > 0) {
      // If the new numReducer is less than minReducer, we will not consider
      // ReduceSinkOperator with this newNumReducer as a correlated ReduceSinkOperator
      if (newNumReducers < minReducers) {
        return false;
      }
      if (numReducers > 0) {
        if (newNumReducers != numReducers) {
          // If (numReducers > 0 && newNumReducers > 0 && newNumReducers != numReducers),
          // we will not consider ReduceSinkOperator with this newNumReducer as a correlated
          // ReduceSinkOperator
          return false;
        }
      } else {
        // if numReducers < 0 and newNumReducers > 0
        numReducers = newNumReducers;
      }
    }

    return true;
  }

}
