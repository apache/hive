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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;

/**
 * This class has methods for generating vectorized join results for inner joins.
 *
 * Inner joins use a hash map to lookup the 1 or more small table values.
 *
 * One vector inner join optimization is projecting inner keys.  When a key appears
 * in the small table results area, instead of copying or referencing key we just include
 * that key again in the output projection.
 *
 * Another optimization is when an inner join does not have any small table columns in the
 * join result, we use a different variation call inner big only.  That variation uses
 * a hash multi-set instead of hash map since there are no values (just a count).
 */
public abstract class VectorMapJoinInnerGenerateResultOperator
        extends VectorMapJoinGenerateResultOperator {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinInnerGenerateResultOperator.class.getName());

  //---------------------------------------------------------------------------
  // Inner join specific members.
  //

  // An array of hash map results so we can do lookups on the whole batch before output result
  // generation.
  protected transient VectorMapJoinHashMapResult hashMapResults[];

  // Pre-allocated member for storing the (physical) batch index of matching row (single- or
  // multi-small-table-valued) indexes during a process call.
  protected transient int[] allMatches;

  /*
   *  Pre-allocated members for storing information equal key series for small-table matches.
   *
   *  ~HashMapResultIndices
   *                Index into the hashMapResults array for the match.
   *  ~AllMatchIndices
   *                (Logical) indices into allMatches to the first row of a match of a
   *                possible series of duplicate keys.
   *  ~IsSingleValue
   *                Whether there is 1 or multiple small table values.
   *  ~DuplicateCounts
   *                The duplicate count for each matched key.
   *
   */
  protected transient int[] equalKeySeriesHashMapResultIndices;
  protected transient int[] equalKeySeriesAllMatchIndices;
  protected transient boolean[] equalKeySeriesIsSingleValue;
  protected transient int[] equalKeySeriesDuplicateCounts;

  // Pre-allocated member for storing the (physical) batch index of rows that need to be spilled.
  protected transient int[] spills;

  // Pre-allocated member for storing index into the hashMapResults for each spilled row.
  protected transient int[] spillHashMapResultIndices;

  /** Kryo ctor. */
  protected VectorMapJoinInnerGenerateResultOperator() {
    super();
  }

  public VectorMapJoinInnerGenerateResultOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinInnerGenerateResultOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  /*
   * Setup our inner join specific members.
   */
  protected void commonSetup() throws HiveException {
    super.commonSetup();

    // Inner join specific.
    VectorMapJoinHashMap baseHashMap = (VectorMapJoinHashMap) vectorMapJoinHashTable;

    hashMapResults = new VectorMapJoinHashMapResult[VectorizedRowBatch.DEFAULT_SIZE];
    for (int i = 0; i < hashMapResults.length; i++) {
      hashMapResults[i] = baseHashMap.createHashMapResult();
    }

    allMatches = new int[VectorizedRowBatch.DEFAULT_SIZE];

    equalKeySeriesHashMapResultIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
    equalKeySeriesAllMatchIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
    equalKeySeriesIsSingleValue = new boolean[VectorizedRowBatch.DEFAULT_SIZE];
    equalKeySeriesDuplicateCounts = new int[VectorizedRowBatch.DEFAULT_SIZE];

    spills = new int[VectorizedRowBatch.DEFAULT_SIZE];
    spillHashMapResultIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
  }

  /*
   * Inner join (hash map).
   */

  /**
   * Do the per-batch setup for an inner join.
   */
  protected void innerPerBatchSetup(VectorizedRowBatch batch) {

    // For join operators that can generate small table results, reset their
    // (target) scratch columns.

    for (int column : smallTableValueColumnMap) {
      ColumnVector smallTableColumn = batch.cols[column];
      smallTableColumn.reset();
    }
  }

  /**
   * Generate the inner join output results for one vectorized row batch.
   *
   * @param batch
   *          The big table batch with any matching and any non matching rows both as
   *          selected in use.
   * @param allMatchCount
   *          Number of matches in allMatches.
   * @param equalKeySeriesCount
   *          Number of single value matches.
   * @param spillCount
   *          Number of spills in spills.
   * @param hashMapResultCount
   *          Number of entries in hashMapResults.
   */
  protected void finishInner(VectorizedRowBatch batch,
      int allMatchCount, int equalKeySeriesCount, int spillCount, int hashMapResultCount)
          throws HiveException, IOException {

    int numSel = 0;

    /*
     * Optimize by running value expressions only over the matched rows.
     */
    if (allMatchCount > 0 && bigTableValueExpressions != null) {
      performValueExpressions(batch, allMatches, allMatchCount);
    }

    for (int i = 0; i < equalKeySeriesCount; i++) {
      int hashMapResultIndex = equalKeySeriesHashMapResultIndices[i];
      VectorMapJoinHashMapResult hashMapResult = hashMapResults[hashMapResultIndex];
      int allMatchesIndex = equalKeySeriesAllMatchIndices[i];
      boolean isSingleValue = equalKeySeriesIsSingleValue[i];
      int duplicateCount = equalKeySeriesDuplicateCounts[i];

      if (isSingleValue) {
        numSel = generateHashMapResultSingleValue(
                    batch, hashMapResult, allMatches, allMatchesIndex, duplicateCount, numSel);
      } else {
        generateHashMapResultMultiValue(
            batch, hashMapResult, allMatches, allMatchesIndex, duplicateCount);
      }
    }

    if (spillCount > 0) {
      spillHashMapBatch(batch, (VectorMapJoinHashTableResult[]) hashMapResults,
          spills, spillHashMapResultIndices, spillCount);
    }

    batch.size = numSel;
    batch.selectedInUse = true;
  }

  protected void finishInnerRepeated(VectorizedRowBatch batch, JoinUtil.JoinResult joinResult,
      VectorMapJoinHashTableResult hashMapResult) throws HiveException, IOException {

    int numSel = 0;

    switch (joinResult) {
    case MATCH:

      if (bigTableValueExpressions != null) {
        // Run our value expressions over whole batch.
        for(VectorExpression ve: bigTableValueExpressions) {
          ve.evaluate(batch);
        }
      }

      // Generate special repeated case.
      generateHashMapResultRepeatedAll(batch, hashMapResults[0]);
      break;

    case SPILL:
      // Whole batch is spilled.
      spillBatchRepeated(batch, (VectorMapJoinHashTableResult) hashMapResults[0]);
      batch.size = 0;
      break;

    case NOMATCH:
      // No match for entire batch.
      batch.size = 0;
      break;
    }
  }
}
