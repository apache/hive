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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// TODO : This class is duplicate of semi join. Need to do a refactoring to merge it with semi join.
/**
 * This class has methods for generating vectorized join results for Anti joins.
 * The big difference between inner joins and anti joins is existence testing.
 * Inner joins use a hash map to lookup the 1 or more small table values.
 * Anti joins are a specialized join for outputting big table rows whose key does not
 * exists in the small table.
 *
 * No small table values are needed for anti since they would be empty.  So,
 * we use a hash set as the hash table.  Hash sets just report whether a key exists.  This
 * is a big performance optimization.
 */
public abstract class VectorMapJoinAntiJoinGenerateResultOperator
        extends VectorMapJoinGenerateResultOperator {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinAntiJoinGenerateResultOperator.class.getName());

  // Anti join specific members.

  // An array of hash set results so we can do lookups on the whole batch before output result
  // generation.
  protected transient VectorMapJoinHashSetResult hashSetResults[];

  // Pre-allocated member for storing the (physical) batch index of matching row (single- or
  // multi-small-table-valued) indexes during a process call.
  protected transient int[] allMatches;

  // Pre-allocated member for storing the (physical) batch index of rows that need to be spilled.
  protected transient int[] spills;

  // Pre-allocated member for storing index into the hashSetResults for each spilled row.
  protected transient int[] spillHashMapResultIndices;

  /** Kryo ctor. */
  protected VectorMapJoinAntiJoinGenerateResultOperator() {
    super();
  }

  public VectorMapJoinAntiJoinGenerateResultOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinAntiJoinGenerateResultOperator(CompilationOpContext ctx, OperatorDesc conf,
                                                     VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  /*
   * Setup our anti join specific members.
   */
  protected void commonSetup() throws HiveException {
    super.commonSetup();

    // Anti join specific.
    VectorMapJoinHashSet baseHashSet = (VectorMapJoinHashSet) vectorMapJoinHashTable;

    hashSetResults = new VectorMapJoinHashSetResult[VectorizedRowBatch.DEFAULT_SIZE];
    for (int i = 0; i < hashSetResults.length; i++) {
      hashSetResults[i] = baseHashSet.createHashSetResult();
    }

    allMatches = new int[VectorizedRowBatch.DEFAULT_SIZE];

    spills = new int[VectorizedRowBatch.DEFAULT_SIZE];
    spillHashMapResultIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
  }

  //-----------------------------------------------------------------------------------------------

  /*
   * Anti join (hash set).
   */

  /**
   * Generate the anti join output results for one vectorized row batch. The result is modified during hash
   * table match to reverse the result for anti join. So here matching means, the row can be emitted as the row
   * is actually not matching.
   *
   * @param batch
   *          The big table batch with any matching and any non matching rows both as
   *          selected in use.
   * @param allMatchCount
   *          Number of matches in allMatches.
   * @param spillCount
   *          Number of spills in spills.
   * @param hashTableResults
   *          The array of all hash table results for the batch. We need the
   *          VectorMapJoinHashTableResult for the spill information.
   */
  protected void finishAnti(VectorizedRowBatch batch,
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
      performValueExpressions(batch, allMatches, allMatchCount);
    }

    batch.size = generateHashSetResults(batch, allMatches, allMatchCount);
    batch.selectedInUse = true;
  }

  /**
   * Generate the matching anti join output results of a vectorized row batch.
   *
   * @param batch
   *          The big table batch.
   * @param allMatches
   *          A subset of the rows of the batch that are matches.
   * @param allMatchCount
   *          Number of matches in allMatches.
   */
  private int generateHashSetResults(VectorizedRowBatch batch,
      int[] allMatches, int allMatchCount) {
    int numSel = 0;
    // Generate result within big table batch itself.
    for (int i = 0; i < allMatchCount; i++) {
      int batchIndex = this.allMatches[i];
      // Use the big table row as output.
      batch.selected[numSel++] = batchIndex;
    }
    return numSel;
  }

  protected JoinUtil.JoinResult inverseResultForAntiJoin(JoinUtil.JoinResult joinResult) {
    if (joinResult == JoinUtil.JoinResult.NOMATCH) {
      return JoinUtil.JoinResult.MATCH;
    } else if (joinResult == JoinUtil.JoinResult.MATCH) {
      return JoinUtil.JoinResult.NOMATCH;
    }
    return joinResult;
  }

  /**
   * Generate the anti join output results for one vectorized row batch with a repeated key.
   *
   * @param batch
   *          The big table batch whose repeated key matches.
   */
  protected int generateHashSetResultRepeatedAll(VectorizedRowBatch batch) {
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

  protected void finishAntiRepeated(VectorizedRowBatch batch, JoinUtil.JoinResult joinResult,
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
      batch.size = generateHashSetResultRepeatedAll(batch);
      batch.selectedInUse = true;
      break;

    case SPILL:
      // Whole batch is spilled.
      spillBatchRepeated(batch, hashSetResult);
      batch.size = 0;
      break;

    case NOMATCH:
      // No match for entire batch.
      batch.size = 0;
      break;
    }
  }
}
