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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This class has methods for generating vectorized join results for left semi joins.
 *
 * The big difference between inner joins and left semi joins is existence testing.
 *
 * Inner joins use a hash map to lookup the 1 or more small table values.
 *
 * Left semi joins are a specialized join for outputting big table rows whose key exists
 * in the small table.
 *
 * No small table values are needed for left semi join since they would be empty.  So,
 * we use a hash set as the hash table.  Hash sets just report whether a key exists.  This
 * is a big performance optimization.
 */
public abstract class VectorMapJoinLeftSemiGenerateResultOperator
        extends VectorMapJoinGenerateResultOperator {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorMapJoinLeftSemiGenerateResultOperator.class.getName());

  //---------------------------------------------------------------------------
  // Semi join specific members.
  //

  // An array of hash set results so we can do lookups on the whole batch before output result
  // generation.
  protected transient VectorMapJoinHashSetResult hashSetResults[];

  // Pre-allocated member for storing the (physical) batch index of matching row (single- or
  // multi-small-table-valued) indexes during a process call.
  protected transient int[] allMatchs;

  // Pre-allocated member for storing the (physical) batch index of rows that need to be spilled.
  protected transient int[] spills;

  // Pre-allocated member for storing index into the hashSetResults for each spilled row.
  protected transient int[] spillHashMapResultIndices;

  public VectorMapJoinLeftSemiGenerateResultOperator() {
    super();
  }

  public VectorMapJoinLeftSemiGenerateResultOperator(VectorizationContext vContext, OperatorDesc conf)
              throws HiveException {
    super(vContext, conf);
  }

  /*
   * Setup our left semi join specific members.
   */
  protected void commonSetup(VectorizedRowBatch batch) throws HiveException {
    super.commonSetup(batch);

    // Semi join specific.
    VectorMapJoinHashSet baseHashSet = (VectorMapJoinHashSet) vectorMapJoinHashTable;

    hashSetResults = new VectorMapJoinHashSetResult[batch.DEFAULT_SIZE];
    for (int i = 0; i < hashSetResults.length; i++) {
      hashSetResults[i] = baseHashSet.createHashSetResult();
    }

    allMatchs = new int[batch.DEFAULT_SIZE];

    spills = new int[batch.DEFAULT_SIZE];
    spillHashMapResultIndices = new int[batch.DEFAULT_SIZE];
  }

  //-----------------------------------------------------------------------------------------------

  /*
   * Left semi join (hash set).
   */

  /**
   * Generate the left semi join output results for one vectorized row batch.
   *
   * @param batch
   *          The big table batch with any matching and any non matching rows both as
   *          selected in use.
   * @param allMatchCount
   *          Number of matches in allMatchs.
   * @param spillCount
   *          Number of spills in spills.
   * @param hashTableResults
   *          The array of all hash table results for the batch. We need the
   *          VectorMapJoinHashTableResult for the spill information.
   */
  protected void finishLeftSemi(VectorizedRowBatch batch,
      int allMatchCount, int spillCount,
      VectorMapJoinHashTableResult[] hashTableResults) throws HiveException, IOException {

    // Get rid of spills before we start modifying the batch.
    if (spillCount > 0) {
      spillHashMapBatch(batch, hashTableResults,
          spills, spillHashMapResultIndices, spillCount);
    }

    /*
     * Optimize by running value expressions only over the matched rows.
     */
    if (allMatchCount > 0 && bigTableValueExpressions != null) {
      performValueExpressions(batch, allMatchs, allMatchCount);
    }

    int numSel = generateHashSetResults(batch, allMatchs, allMatchCount);
    batch.size = numSel;
    batch.selectedInUse = true;
  }

  /**
   * Generate the matching left semi join output results of a vectorized row batch.
   *
   * @param batch
   *          The big table batch.
   * @param allMatchs
   *          A subset of the rows of the batch that are matches.
   * @param allMatchCount
   *          Number of matches in allMatchs.
   */
  private int generateHashSetResults(VectorizedRowBatch batch,
      int[] allMatchs, int allMatchCount)
          throws HiveException, IOException {

    int numSel = 0;

    // Generate result within big table batch itself.

    for (int i = 0; i < allMatchCount; i++) {

      int batchIndex = allMatchs[i];

      // Use the big table row as output.
      batch.selected[numSel++] = batchIndex;
    }

    return numSel;
  }

  /**
   * Generate the left semi join output results for one vectorized row batch with a repeated key.
   *
   * @param batch
   *          The big table batch whose repeated key matches.
   */
  protected int generateHashSetResultRepeatedAll(VectorizedRowBatch batch) throws HiveException {

    if (batch.selectedInUse) {
      // The selected array is already filled in as we want it.
    } else {
      int[] selected = batch.selected;
      for (int i = 0; i < batch.size; i++) {
        selected[i] = i;
      }
      batch.selectedInUse = true;
    }

    return batch.size;
  }

  protected void finishLeftSemiRepeated(VectorizedRowBatch batch, JoinUtil.JoinResult joinResult,
      VectorMapJoinHashTableResult hashSetResult) throws HiveException, IOException {

    switch (joinResult) {
    case MATCH:

      if (bigTableValueExpressions != null) {
        // Run our value expressions over whole batch.
        for(VectorExpression ve: bigTableValueExpressions) {
          ve.evaluate(batch);
        }
      }

      // Generate special repeated case.
      int numSel = generateHashSetResultRepeatedAll(batch);
      batch.size = numSel;
      batch.selectedInUse = true;
      break;

    case SPILL:
      // Whole batch is spilled.
      spillBatchRepeated(batch, (VectorMapJoinHashTableResult) hashSetResult);
      batch.size = 0;
      break;

    case NOMATCH:
      // No match for entire batch.
      batch.size = 0;
      break;
    }
  }
}