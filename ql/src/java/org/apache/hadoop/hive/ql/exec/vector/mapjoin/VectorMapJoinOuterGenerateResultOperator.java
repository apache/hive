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
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;

/**
 * This class has methods for generating vectorized join results for outer joins.
 *
 * The big difference between inner joins and outer joins is the treatment of null and non-matching
 * keys.
 *
 * Inner joins ignore null keys.  Outer joins include big table rows with null keys in the result.
 *
 * (Left non-full) outer joins include big table rows that do not match the small table.  Small
 * table columns for non-matches will be NULL.
 *
 * Another important difference is filtering.  For outer joins to include the necessary rows,
 * filtering must be done after the hash table lookup.  That is because filtering does not
 * eliminate rows, but changes them from match to non-matching rows.  They will still appear in
 * the join result.
 *
 * One vector outer join optimization is referencing bytes outer keys.  When a bytes key appears
 * in the small table results area, instead of copying the bytes key we reference the big table key.
 * Bytes column vectors allow a by reference entry to bytes.  It is safe to do a by reference
 * since it is within the same row.
 *
 * Outer join uses a hash map since small table columns can be included in the join result.
 */
public abstract class VectorMapJoinOuterGenerateResultOperator
        extends VectorMapJoinGenerateResultOperator {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinOuterGenerateResultOperator.class.getName());

  //---------------------------------------------------------------------------
  // Outer join specific members.
  //

  // An array of hash map results so we can do lookups on the whole batch before output result
  // generation.
  protected transient VectorMapJoinHashMapResult hashMapResults[];

  // Pre-allocated member for remembering the big table's selected array at the beginning of
  // the process method before applying any filter.  For outer join we need to remember which
  // rows did not match since they will appear the in outer join result with NULLs for the
  // small table.
  protected transient int[] inputSelected;

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

  // Pre-allocated member for storing index into the hashSetResults for each spilled row.
  protected transient int[] spillHashMapResultIndices;

  // Pre-allocated member for storing any non-spills, non-matches, or merged row indexes during a
  // process method call.
  protected transient int[] nonSpills;
  protected transient int[] noMatchs;
  protected transient int[] merged;

  /** Kryo ctor. */
  protected VectorMapJoinOuterGenerateResultOperator() {
    super();
  }

  public VectorMapJoinOuterGenerateResultOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinOuterGenerateResultOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  /*
   * Setup our outer join specific members.
   */
  protected void commonSetup() throws HiveException {
    super.commonSetup();

    // Outer join specific.
    VectorMapJoinHashMap baseHashMap = (VectorMapJoinHashMap) vectorMapJoinHashTable;

    hashMapResults = new VectorMapJoinHashMapResult[VectorizedRowBatch.DEFAULT_SIZE];
    for (int i = 0; i < hashMapResults.length; i++) {
      hashMapResults[i] = baseHashMap.createHashMapResult();
    }

    inputSelected = new int[VectorizedRowBatch.DEFAULT_SIZE];

    allMatches = new int[VectorizedRowBatch.DEFAULT_SIZE];

    equalKeySeriesHashMapResultIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
    equalKeySeriesAllMatchIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];
    equalKeySeriesIsSingleValue = new boolean[VectorizedRowBatch.DEFAULT_SIZE];
    equalKeySeriesDuplicateCounts = new int[VectorizedRowBatch.DEFAULT_SIZE];

    spills = new int[VectorizedRowBatch.DEFAULT_SIZE];
    spillHashMapResultIndices = new int[VectorizedRowBatch.DEFAULT_SIZE];

    nonSpills = new int[VectorizedRowBatch.DEFAULT_SIZE];
    noMatchs = new int[VectorizedRowBatch.DEFAULT_SIZE];
    merged = new int[VectorizedRowBatch.DEFAULT_SIZE];

    matchTracker = null;

  }

  //-----------------------------------------------------------------------------------------------

  /*
   * Outer join (hash map).
   */

  /**
   * Do the per-batch setup for an outer join.
   */
  protected void outerPerBatchSetup(VectorizedRowBatch batch) {

    // For join operators that can generate small table results, reset their
    // (target) scratch columns.

    for (int column : outerSmallTableKeyColumnMap) {
      ColumnVector bigTableOuterKeyColumn = batch.cols[column];
      bigTableOuterKeyColumn.reset();
    }

    for (int column : smallTableValueColumnMap) {
      ColumnVector smallTableColumn = batch.cols[column];
      smallTableColumn.reset();
    }

  }

  /**
   * Apply the value expression to rows in the (original) input selected array.
   *
   * @param batch
   *          The vectorized row batch.
   * @param inputSelectedInUse
   *          Whether the (original) input batch is selectedInUse.
   * @param inputLogicalSize
   *          The (original) input batch size.
   */
  private void doValueExprOnInputSelected(VectorizedRowBatch batch,
      boolean inputSelectedInUse, int inputLogicalSize) throws HiveException {

    int saveBatchSize = batch.size;
    int[] saveSelected = batch.selected;
    boolean saveSelectedInUse = batch.selectedInUse;

    batch.size = inputLogicalSize;
    batch.selected = inputSelected;
    batch.selectedInUse = inputSelectedInUse;

    if (bigTableValueExpressions != null) {
      for(VectorExpression ve: bigTableValueExpressions) {
        ve.evaluate(batch);
      }
    }

    batch.size = saveBatchSize;
    batch.selected = saveSelected;
    batch.selectedInUse = saveSelectedInUse;
  }

  /**
   * Apply the value expression to rows specified by a selected array.
   *
   * @param batch
   *          The vectorized row batch.
   * @param selected
   *          The (physical) batch indices to apply the expression to.
   * @param size
   *          The size of selected.
   */
  private void doValueExpr(VectorizedRowBatch batch,
      int[] selected, int size) throws HiveException {

    int saveBatchSize = batch.size;
    int[] saveSelected = batch.selected;
    boolean saveSelectedInUse = batch.selectedInUse;

    batch.size = size;
    batch.selected = selected;
    batch.selectedInUse = true;

    if (bigTableValueExpressions != null) {
      for(VectorExpression ve: bigTableValueExpressions) {
        ve.evaluate(batch);
      }
    }

    batch.size = saveBatchSize;
    batch.selected = saveSelected;
    batch.selectedInUse = saveSelectedInUse;
  }

  /**
   * Remove (subtract) members from the input selected array and produce the results into
   * a difference array.
   *
   * @param inputSelectedInUse
   *          Whether the (original) input batch is selectedInUse.
   * @param inputLogicalSize
   *          The (original) input batch size.
   * @param remove
   *          The indices to remove.  They must all be present in input selected array.
   * @param removeSize
   *          The size of remove.
   * @param difference
   *          The resulting difference -- the input selected array indices not in the
   *          remove array.
   * @return
   *          The resulting size of the difference array.
   * @throws HiveException 
   */
  private int subtractFromInputSelected(boolean inputSelectedInUse, int inputLogicalSize,
      int[] remove, int removeSize, int[] difference) throws HiveException {

    // if (!verifyMonotonicallyIncreasing(remove, removeSize)) {
    //   throw new HiveException("remove is not in sort order and unique");
    // }

   int differenceCount = 0;

   // Determine which rows are left.
   int removeIndex = 0;
   if (inputSelectedInUse) {
     for (int i = 0; i < inputLogicalSize; i++) {
       int candidateIndex = inputSelected[i];
       if (removeIndex < removeSize && candidateIndex == remove[removeIndex]) {
         removeIndex++;
       } else {
         difference[differenceCount++] = candidateIndex;
       }
     }
   } else {
     for (int candidateIndex = 0; candidateIndex < inputLogicalSize; candidateIndex++) {
       if (removeIndex < removeSize && candidateIndex == remove[removeIndex]) {
         removeIndex++;
       } else {
         difference[differenceCount++] = candidateIndex;
       }
     }
   }

   if (removeIndex != removeSize) {
     throw new HiveException("Not all batch indices removed");
   }

   // if (!verifyMonotonicallyIncreasing(difference, differenceCount)) {
   //   throw new HiveException("difference is not in sort order and unique");
   // }

   return differenceCount;
 }

  /**
   * Remove (subtract) members from an array and produce the results into
   * a difference array.

   * @param all
   *          The selected array containing all members.
   * @param allSize
   *          The size of all.
   * @param remove
   *          The indices to remove.  They must all be present in input selected array.
   * @param removeSize
   *          The size of remove.
   * @param difference
   *          The resulting difference -- the all array indices not in the
   *          remove array.
   * @return
   *          The resulting size of the difference array.
   * @throws HiveException 
   */
  private int subtract(int[] all, int allSize,
      int[] remove, int removeSize, int[] difference) throws HiveException {

    // if (!verifyMonotonicallyIncreasing(remove, removeSize)) {
    //   throw new HiveException("remove is not in sort order and unique");
    // }

    int differenceCount = 0;

    // Determine which rows are left.
    int removeIndex = 0;
    for (int i = 0; i < allSize; i++) {
      int candidateIndex = all[i];
      if (removeIndex < removeSize && candidateIndex == remove[removeIndex]) {
        removeIndex++;
      } else {
        difference[differenceCount++] = candidateIndex;
      }
    }

    if (removeIndex != removeSize) {
      throw new HiveException("Not all batch indices removed");
    }

    return differenceCount;
  }

  /**
   * Sort merge two select arrays so the resulting array is ordered by (batch) index.
   *
   * @param selected1
   * @param selected1Count
   * @param selected2
   * @param selected2Count
   * @param sortMerged
   *          The resulting sort merge of selected1 and selected2.
   * @return
   *          The resulting size of the sortMerged array.
   * @throws HiveException 
   */
  private int sortMerge(int[] selected1, int selected1Count,
          int[] selected2, int selected2Count, int[] sortMerged) throws HiveException {

    // if (!verifyMonotonicallyIncreasing(selected1, selected1Count)) {
    //   throw new HiveException("selected1 is not in sort order and unique");
    // }

    // if (!verifyMonotonicallyIncreasing(selected2, selected2Count)) {
    //   throw new HiveException("selected1 is not in sort order and unique");
    // }


    int sortMergeCount = 0;

    int selected1Index = 0;
    int selected2Index = 0;
    for (int i = 0; i < selected1Count + selected2Count; i++) {
      if (selected1Index < selected1Count && selected2Index < selected2Count) {
        if (selected1[selected1Index] < selected2[selected2Index]) {
          sortMerged[sortMergeCount++] = selected1[selected1Index++];
        } else {
          sortMerged[sortMergeCount++] = selected2[selected2Index++];
        }
      } else if (selected1Index < selected1Count) {
        sortMerged[sortMergeCount++] = selected1[selected1Index++];
      } else {
        sortMerged[sortMergeCount++] = selected2[selected2Index++];
      }
    }

    // if (!verifyMonotonicallyIncreasing(sortMerged, sortMergeCount)) {
    //   throw new HiveException("sortMerged is not in sort order and unique");
    // }

    return sortMergeCount;
  }

  /**
   * Generate the outer join output results for one vectorized row batch.
   *
   * @param batch
   *          The big table batch with any matching and any non matching rows both as
   *          selected in use.
   * @param allMatchCount
   *          Number of matches in allMatches.
   * @param equalKeySeriesCount
   *          Number of single value matches.
   * @param atLeastOneNonMatch
   *          Whether at least one row was a non-match.
   * @param inputSelectedInUse
   *          A copy of the batch's selectedInUse flag on input to the process method.
   * @param inputLogicalSize
   *          The batch's size on input to the process method.
   * @param spillCount
   *          Number of spills in spills.
   * @param hashMapResultCount
   *          Number of entries in hashMapResults.
   */
  public void finishOuter(VectorizedRowBatch batch,
      int allMatchCount, int equalKeySeriesCount, boolean atLeastOneNonMatch,
      boolean inputSelectedInUse, int inputLogicalSize,
      int spillCount, int hashMapResultCount) throws IOException, HiveException {

    // Get rid of spills before we start modifying the batch.
    if (spillCount > 0) {
      spillHashMapBatch(batch, (VectorMapJoinHashTableResult[]) hashMapResults,
          spills, spillHashMapResultIndices, spillCount);
    }

    int noMatchCount = 0;
    if (spillCount > 0) {

      // Subtract the spills to get all match and non-match rows.
      int nonSpillCount = subtractFromInputSelected(
              inputSelectedInUse, inputLogicalSize, spills, spillCount, nonSpills);

      if (LOG.isDebugEnabled()) {
        LOG.debug("finishOuter spillCount > 0" +
            " nonSpills " + intArrayToRangesString(nonSpills, nonSpillCount));
      }
  
      // Big table value expressions apply to ALL matching and non-matching rows.
      if (bigTableValueExpressions != null) {
  
        doValueExpr(batch, nonSpills, nonSpillCount);
  
      }
  
      if (atLeastOneNonMatch) {
        noMatchCount = subtract(nonSpills, nonSpillCount, allMatches, allMatchCount,
                noMatchs);

        if (LOG.isDebugEnabled()) {
          LOG.debug("finishOuter spillCount > 0" +
              " noMatchs " + intArrayToRangesString(noMatchs, noMatchCount));
        }

      }
    } else {

      // Run value expressions over original (whole) input batch.
      doValueExprOnInputSelected(batch, inputSelectedInUse, inputLogicalSize);

      if (atLeastOneNonMatch) {
        noMatchCount = subtractFromInputSelected(
            inputSelectedInUse, inputLogicalSize, allMatches, allMatchCount, noMatchs);

        if (LOG.isDebugEnabled()) {
          LOG.debug("finishOuter spillCount == 0" +
              " noMatchs " + intArrayToRangesString(noMatchs, noMatchCount));
        }
      }
    }

    // When we generate results into the overflow batch, we may still end up with fewer rows
    // in the big table batch.  So, nulSel and the batch's selected array will be rebuilt with
    // just the big table rows that need to be forwarded, minus any rows processed with the
    // overflow batch.
    if (allMatchCount > 0) {

      int numSel = 0;
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

      // The number of single value rows that were generated in the big table batch.
      batch.size = numSel;
      batch.selectedInUse = true;

      if (LOG.isDebugEnabled()) {
        LOG.debug("finishOuter allMatchCount > 0" +
            " batch.selected " + intArrayToRangesString(batch.selected, batch.size));
      }

    } else {
      batch.size = 0;
    }

    if (noMatchCount > 0) {
      if (batch.size > 0) {

        generateOuterNulls(batch, noMatchs, noMatchCount);
  
        // Merge noMatchs and (match) selected.
        int mergeCount = sortMerge(
                noMatchs, noMatchCount, batch.selected, batch.size, merged);
    
        if (LOG.isDebugEnabled()) {
          LOG.debug("finishOuter noMatchCount > 0 && batch.size > 0" +
              " merged " + intArrayToRangesString(merged, mergeCount));
        }

        System.arraycopy(merged, 0, batch.selected, 0, mergeCount);
        batch.size = mergeCount;
        batch.selectedInUse = true;
      } else {

        // We can use the whole batch for output of no matches.

        generateOuterNullsRepeatedAll(batch);

        System.arraycopy(noMatchs, 0, batch.selected, 0, noMatchCount);
        batch.size = noMatchCount;
        batch.selectedInUse = true;

        if (LOG.isDebugEnabled()) {
          LOG.debug("finishOuter noMatchCount > 0 && batch.size == 0" +
              " batch.selected " + intArrayToRangesString(batch.selected, batch.size));
        }
      }
    }
  }

   /**
    * Generate the non matching outer join output results for one vectorized row batch.
    *
    * For each non matching row specified by parameter, generate nulls for the small table results.
    *
    * @param batch
    *          The big table batch with any matching and any non matching rows both as
    *          selected in use.
    * @param noMatchs
    *          A subset of the rows of the batch that are non matches.
    * @param noMatchSize
    *          Number of non matches in noMatchs.
    */
   protected void generateOuterNulls(VectorizedRowBatch batch, int[] noMatchs,
       int noMatchSize) throws IOException, HiveException {

    // Set null information in the small table results area.

    for (int i = 0; i < noMatchSize; i++) {
      int batchIndex = noMatchs[i];

      // Mark any scratch small table scratch columns that would normally receive a copy of the
      // key as null, too.
      //
      for (int column : outerSmallTableKeyColumnMap) {
        ColumnVector colVector = batch.cols[column];
        colVector.noNulls = false;
        colVector.isNull[batchIndex] = true;
      }

      // Small table values are set to null.
      for (int column : smallTableValueColumnMap) {
        ColumnVector colVector = batch.cols[column];
        colVector.noNulls = false;
        colVector.isNull[batchIndex] = true;
      }
    }
  }

  /**
   * Generate the outer join output results for one vectorized row batch with a repeated key.
   *
   * Any filter expressions will apply now since hash map lookup for outer join is complete.
   *
   * @param batch
   *          The big table batch with any matching and any non matching rows both as
   *          selected in use.
   * @param joinResult
   *          The hash map lookup result for the repeated key.
   * @param hashMapResult
   *          The array of all hash map results for the batch.
   * @param someRowsFilteredOut
   *          Whether some rows of the repeated key batch were knocked out by the filter.
   * @param inputSelectedInUse
   *          A copy of the batch's selectedInUse flag on input to the process method.
   * @param inputLogicalSize
   *          The batch's size on input to the process method.
   */
  public void finishOuterRepeated(VectorizedRowBatch batch, JoinUtil.JoinResult joinResult,
      VectorMapJoinHashMapResult hashMapResult, boolean someRowsFilteredOut,
      boolean inputSelectedInUse, int inputLogicalSize)
          throws IOException, HiveException {

    // LOG.debug("finishOuterRepeated batch #" + batchCounter + " " + joinResult.name() + " batch.size " + batch.size + " someRowsFilteredOut " + someRowsFilteredOut);

    switch (joinResult) {
    case MATCH:

      // Rows we looked up as one repeated key are a match.  But filtered out rows
      // need to be generated as non-matches, too.

      if (someRowsFilteredOut) {

        // For the filtered out rows that didn't (logically) get looked up in the hash table,
        // we need to generate no match results for those too...

        // Run value expressions over original (whole) input batch.
        doValueExprOnInputSelected(batch, inputSelectedInUse, inputLogicalSize);

        // Now calculate which rows were filtered out (they are logically no matches).

        // Determine which rows are non matches by determining the delta between inputSelected and
        // (current) batch selected.

        int noMatchCount = subtractFromInputSelected(
                inputSelectedInUse, inputLogicalSize, batch.selected, batch.size, noMatchs);

        generateOuterNulls(batch, noMatchs, noMatchCount);

        // Now generate the matchs.  Single small table values will be put into the big table
        // batch and come back in matchs.  Any multiple small table value results will go into
        // the overflow batch.
        generateHashMapResultRepeatedAll(batch, hashMapResult);

        // Merge noMatchs and (match) selected.
        int mergeCount = sortMerge(
                noMatchs, noMatchCount, batch.selected, batch.size, merged);

        System.arraycopy(merged, 0, batch.selected, 0, mergeCount);
        batch.size = mergeCount;
        batch.selectedInUse = true;
      } else {

        // Just run our value expressions over input batch.

        if (bigTableValueExpressions != null) {
          for(VectorExpression ve: bigTableValueExpressions) {
            ve.evaluate(batch);
          }
        }

        generateHashMapResultRepeatedAll(batch, hashMapResult);
      }
      break;

    case SPILL:

      // Rows we looked up as one repeated key need to spill.  But filtered out rows
      // need to be generated as non-matches, too.

      spillBatchRepeated(batch, (VectorMapJoinHashTableResult) hashMapResult);

      // After using selected to generate spills, generate non-matches, if any.
      if (someRowsFilteredOut) {

        // Determine which rows are non matches by determining the delta between inputSelected and
        // (current) batch selected.

        int noMatchCount = subtractFromInputSelected(
                inputSelectedInUse, inputLogicalSize, batch.selected, batch.size, noMatchs);

        System.arraycopy(noMatchs, 0, batch.selected, 0, noMatchCount);
        batch.size = noMatchCount;
        batch.selectedInUse = true;

        generateOuterNullsRepeatedAll(batch);
      } else {
        batch.size = 0;
      }

      break;

    case NOMATCH:

      if (someRowsFilteredOut) {

        // When the repeated no match is due to filtering, we need to restore the
        // selected information.

        if (inputSelectedInUse) {
          System.arraycopy(inputSelected, 0, batch.selected, 0, inputLogicalSize);
        }
        batch.selectedInUse = inputSelectedInUse;
        batch.size = inputLogicalSize;
      }

      // Run our value expressions over whole batch.
      if (bigTableValueExpressions != null) {
        for(VectorExpression ve: bigTableValueExpressions) {
          ve.evaluate(batch);
        }
      }

      generateOuterNullsRepeatedAll(batch);
      break;
    }
  }

  /**
   * Generate the non-match outer join output results for the whole repeating vectorized
   * row batch.
   *
   * Each row will get nulls for all small table values.
   *
   * @param batch
   *          The big table batch.
   */
  protected void generateOuterNullsRepeatedAll(VectorizedRowBatch batch) throws HiveException {

    // Mark any scratch small table scratch columns that would normally receive a copy of the
    // key as null, too.
    //
    for (int column : outerSmallTableKeyColumnMap) {
      ColumnVector colVector = batch.cols[column];
      colVector.noNulls = false;
      colVector.isNull[0] = true;
      colVector.isRepeating = true;
    }

    for (int column : smallTableValueColumnMap) {
      ColumnVector colVector = batch.cols[column];
      colVector.noNulls = false;
      colVector.isNull[0] = true;
      colVector.isRepeating = true;
    }
  }

  private void markBigTableColumnsAsNullRepeating() {

    /*
     * For non-match FULL OUTER Small Table results, the Big Table columns are all NULL.
     */
    for (int column : bigTableRetainColumnMap) {
      ColumnVector colVector = overflowBatch.cols[column];
      colVector.isRepeating = true;
      colVector.noNulls = false;
      colVector.isNull[0] = true;
    }
  }

  /*
   * For FULL OUTER MapJoin, find the non matched Small Table keys and values and odd them to the
   * join output result.
   */
  @Override
  protected void generateFullOuterSmallTableNoMatches(byte smallTablePos,
      MapJoinTableContainer substituteSmallTable) throws HiveException {

    /*
     * For dynamic partition hash join, both the Big Table and Small Table are partitioned (sent)
     * to the Reducer using the key hash code.  So, we can generate the non-match Small Table
     * results locally.
     *
     * Scan the Small Table for keys that didn't match and generate the non-matchs into the
     * overflowBatch.
     */

    /*
     * If there were no matched keys sent, we need to do our common initialization.
     */
    if (needCommonSetup) {

      // Our one time process method initialization.
      commonSetup();

      needCommonSetup = false;
    }

    if (needHashTableSetup) {

      // Setup our hash table specialization.  It will be the first time the process
      // method is called, or after a Hybrid Grace reload.

      hashTableSetup();

      needHashTableSetup = false;
    }

    /*
     * To support fancy NULL repeating columns, let's flush the overflowBatch if it has anything.
     */
    if (overflowBatch.size > 0) {
      forwardOverflow();
    }
    markBigTableColumnsAsNullRepeating();

    switch (hashTableKeyType) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      generateFullOuterLongKeySmallTableNoMatches();
      break;
    case STRING:
      generateFullOuterStringKeySmallTableNoMatches();
      break;
    case MULTI_KEY:
      generateFullOuterMultiKeySmallTableNoMatches();
      break;
    default:
      throw new RuntimeException("Unexpected hash table key type " + hashTableKeyType);
    }
  }

  /*
   * For FULL OUTER MapJoin, find the non matched Small Table Long keys and values and odd them to
   * the join output result.
   */
  protected void generateFullOuterLongKeySmallTableNoMatches()
      throws HiveException {

    final LongColumnVector singleSmallTableKeyOutputColumnVector;
    if (allSmallTableKeyColumnIncluded[0]) {
      singleSmallTableKeyOutputColumnVector =
        (LongColumnVector) overflowBatch.cols[allSmallTableKeyColumnNums[0]];
    } else {
      singleSmallTableKeyOutputColumnVector = null;
    }

    VectorMapJoinLongHashMap hashMap = (VectorMapJoinLongHashMap) vectorMapJoinHashTable;

    VectorMapJoinNonMatchedIterator nonMatchedIterator =
        hashMap.createNonMatchedIterator(matchTracker);
    nonMatchedIterator.init();
    while (nonMatchedIterator.findNextNonMatched()) {

      final long longKey;
      boolean isKeyNull = !nonMatchedIterator.readNonMatchedLongKey();
      if (!isKeyNull) {
        longKey = nonMatchedIterator.getNonMatchedLongKey();
      } else {
        longKey = 0;
      }

      VectorMapJoinHashMapResult hashMapResult = nonMatchedIterator.getNonMatchedHashMapResult();

      ByteSegmentRef byteSegmentRef = hashMapResult.first();
      while (byteSegmentRef != null) {

        // NOTE: Big Table result columns were marked repeating NULL already.

        if (singleSmallTableKeyOutputColumnVector != null) {
          if (isKeyNull) {
            singleSmallTableKeyOutputColumnVector.isNull[overflowBatch.size] = true;
            singleSmallTableKeyOutputColumnVector.noNulls = false;
          } else {
            singleSmallTableKeyOutputColumnVector.vector[overflowBatch.size] = longKey;
            singleSmallTableKeyOutputColumnVector.isNull[overflowBatch.size] = false;
          }
        }

        if (smallTableValueVectorDeserializeRow != null) {

          doSmallTableValueDeserializeRow(overflowBatch, overflowBatch.size,
              byteSegmentRef, hashMapResult);
        }

        overflowBatch.size++;
        if (overflowBatch.size == overflowBatch.DEFAULT_SIZE) {
          forwardOverflow();
          markBigTableColumnsAsNullRepeating();
        }
        byteSegmentRef = hashMapResult.next();
      }
    }
  }

  private void doSmallTableKeyDeserializeRow(VectorizedRowBatch batch, int batchIndex,
      byte[] keyBytes, int keyOffset, int keyLength)
          throws HiveException {

    smallTableKeyOuterVectorDeserializeRow.setBytes(keyBytes, keyOffset, keyLength);

    try {
      // Our hash tables are immutable.  We can safely do by reference STRING, CHAR/VARCHAR, etc.
      smallTableKeyOuterVectorDeserializeRow.deserializeByRef(batch, batchIndex);
    } catch (Exception e) {
      throw new HiveException(
          "\nDeserializeRead detail: " +
              smallTableKeyOuterVectorDeserializeRow.getDetailedReadPositionString(),
          e);
    }
  }

  /*
   * For FULL OUTER MapJoin, find the non matched Small Table Multi-Keys and values and odd them to
   * the join output result.
   */
  protected void generateFullOuterMultiKeySmallTableNoMatches() throws HiveException {

    VectorMapJoinBytesHashMap hashMap = (VectorMapJoinBytesHashMap) vectorMapJoinHashTable;

    VectorMapJoinNonMatchedIterator nonMatchedIterator =
        hashMap.createNonMatchedIterator(matchTracker);
    nonMatchedIterator.init();
    while (nonMatchedIterator.findNextNonMatched()) {

      nonMatchedIterator.readNonMatchedBytesKey();
      byte[] keyBytes = nonMatchedIterator.getNonMatchedBytes();
      final int keyOffset = nonMatchedIterator.getNonMatchedBytesOffset();
      final int keyLength = nonMatchedIterator.getNonMatchedBytesLength();

      VectorMapJoinHashMapResult hashMapResult = nonMatchedIterator.getNonMatchedHashMapResult();

      ByteSegmentRef byteSegmentRef = hashMapResult.first();
      while (byteSegmentRef != null) {

        // NOTE: Big Table result columns were marked repeating NULL already.

        if (smallTableKeyOuterVectorDeserializeRow != null) {
          doSmallTableKeyDeserializeRow(overflowBatch, overflowBatch.size,
              keyBytes, keyOffset, keyLength);
        }

        if (smallTableValueVectorDeserializeRow != null) {

          doSmallTableValueDeserializeRow(overflowBatch, overflowBatch.size,
              byteSegmentRef, hashMapResult);
        }

        overflowBatch.size++;
        if (overflowBatch.size == overflowBatch.DEFAULT_SIZE) {
          forwardOverflow();
          markBigTableColumnsAsNullRepeating();
        }
        byteSegmentRef = hashMapResult.next();
      }
    }

    // NOTE: We don't have to deal with FULL OUTER All-NULL key values like we do for single-column
    // LONG and STRING because we do store them in the hash map...
  }

  /*
   * For FULL OUTER MapJoin, find the non matched Small Table String keys and values and odd them to
   * the join output result.
   */
  protected void generateFullOuterStringKeySmallTableNoMatches() throws HiveException {

    final BytesColumnVector singleSmallTableKeyOutputColumnVector;
    if (allSmallTableKeyColumnIncluded[0]) {
      singleSmallTableKeyOutputColumnVector =
        (BytesColumnVector) overflowBatch.cols[allSmallTableKeyColumnNums[0]];
    } else {
      singleSmallTableKeyOutputColumnVector = null;
    }

    VectorMapJoinBytesHashMap hashMap = (VectorMapJoinBytesHashMap) vectorMapJoinHashTable;

    VectorMapJoinNonMatchedIterator nonMatchedIterator =
        hashMap.createNonMatchedIterator(matchTracker);
    nonMatchedIterator.init();
    while (nonMatchedIterator.findNextNonMatched()) {

      final byte[] keyBytes;
      final int keyOffset;
      final int keyLength;
      boolean isKeyNull = !nonMatchedIterator.readNonMatchedBytesKey();
      if (!isKeyNull) {
        keyBytes = nonMatchedIterator.getNonMatchedBytes();
        keyOffset = nonMatchedIterator.getNonMatchedBytesOffset();
        keyLength = nonMatchedIterator.getNonMatchedBytesLength();
      } else {
        keyBytes = null;
        keyOffset = 0;
        keyLength = 0;
      }

      VectorMapJoinHashMapResult hashMapResult = nonMatchedIterator.getNonMatchedHashMapResult();

      ByteSegmentRef byteSegmentRef = hashMapResult.first();
      while (byteSegmentRef != null) {

        // NOTE: Big Table result columns were marked repeating NULL already.

        if (singleSmallTableKeyOutputColumnVector != null) {
          if (isKeyNull) {
            singleSmallTableKeyOutputColumnVector.isNull[overflowBatch.size] = true;
            singleSmallTableKeyOutputColumnVector.noNulls = false;
          } else {
            singleSmallTableKeyOutputColumnVector.setVal(
                overflowBatch.size,
                keyBytes, keyOffset, keyLength);
            singleSmallTableKeyOutputColumnVector.isNull[overflowBatch.size] = false;
          }
        }

        if (smallTableValueVectorDeserializeRow != null) {

          doSmallTableValueDeserializeRow(overflowBatch, overflowBatch.size,
              byteSegmentRef, hashMapResult);
        }

        overflowBatch.size++;
        if (overflowBatch.size == overflowBatch.DEFAULT_SIZE) {
          forwardOverflow();
          markBigTableColumnsAsNullRepeating();
        }
        byteSegmentRef = hashMapResult.next();
      }
    }
  }

  protected void fullOuterHashTableSetup() {

    // Always track key matches for FULL OUTER.
    matchTracker = vectorMapJoinHashTable.createMatchTracker();

  }

  protected void fullOuterIntersectHashTableSetup() {

    matchTracker = vectorMapJoinHashTable.createMatchTracker();
  }
}
