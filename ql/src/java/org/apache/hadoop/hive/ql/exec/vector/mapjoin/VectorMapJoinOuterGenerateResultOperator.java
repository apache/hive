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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
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
  private static final Log LOG = LogFactory.getLog(VectorMapJoinOuterGenerateResultOperator.class.getName());

  //---------------------------------------------------------------------------
  // Outer join specific members.
  //

  // An array of hash map results so we can do lookups on the whole batch before output result
  // generation.
  protected transient VectorMapJoinHashMapResult hashMapResults[];

  // Pre-allocated member for storing any matching row indexes during a processOp call.
  protected transient int[] matchs;

  // Pre-allocated member for storing the mapping to the row batchIndex of the first of a series of
  // equal keys that was looked up during a processOp call.
  protected transient int[] matchHashMapResultIndices;

  // All matching and non-matching big table rows.
  protected transient int[] nonSpills;

  // Pre-allocated member for storing the (physical) batch index of rows that need to be spilled.
  protected transient int[] spills;

  // Pre-allocated member for storing index into the hashSetResults for each spilled row.
  protected transient int[] spillHashMapResultIndices;

  // Pre-allocated member for storing any non-matching row indexes during a processOp call.
  protected transient int[] scratch1;

  public VectorMapJoinOuterGenerateResultOperator() {
    super();
  }

  public VectorMapJoinOuterGenerateResultOperator(VectorizationContext vContext, OperatorDesc conf)
              throws HiveException {
    super(vContext, conf);
  }

  /*
   * Setup our outer join specific members.
   */
  protected void commonSetup(VectorizedRowBatch batch) throws HiveException {
    super.commonSetup(batch);

    // Outer join specific.
    VectorMapJoinHashMap baseHashMap = (VectorMapJoinHashMap) vectorMapJoinHashTable;

    hashMapResults = new VectorMapJoinHashMapResult[batch.DEFAULT_SIZE];
    for (int i = 0; i < hashMapResults.length; i++) {
      hashMapResults[i] = baseHashMap.createHashMapResult();
    }
    matchs = new int[batch.DEFAULT_SIZE];
    matchHashMapResultIndices = new int[batch.DEFAULT_SIZE];
    nonSpills = new int[batch.DEFAULT_SIZE];
    spills = new int[batch.DEFAULT_SIZE];
    spillHashMapResultIndices = new int[batch.DEFAULT_SIZE];
    scratch1 = new int[batch.DEFAULT_SIZE];
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

    for (int column : smallTableOutputVectorColumns) {
      ColumnVector smallTableColumn = batch.cols[column];
      smallTableColumn.reset();
    }

    for (int column : bigTableOuterKeyOutputVectorColumns) {
      ColumnVector bigTableOuterKeyColumn = batch.cols[column];
      bigTableOuterKeyColumn.reset();
    }
  }

  /**
   * Generate the outer join output results for one vectorized row batch.
   *
   * Any filter expressions will apply now since hash map lookup for outer join is complete.
   *
   * @param batch
   *          The big table batch with any matching and any non matching rows both as
   *          selected in use.
   * @param matchs
   *          A subset of the rows of the batch that are matches.
   * @param matchHashMapResultIndices
   *          For each entry in matches, the index into the hashMapResult.
   * @param matchSize
   *          Number of matches in matchs.
   * @param nonSpills
   *          The rows of the batch that are both matches and non-matches.
   * @param nonspillCount
   *          Number of rows in nonSpills.
   * @param spills
   *          A subset of the rows of the batch that are spills.
   * @param spillHashMapResultIndices
   *          For each entry in spills, the index into the hashMapResult.
   * @param spillCount
   *          Number of spills in spills.
   * @param hashMapResults
   *          The array of all hash map results for the batch.
   * @param hashMapResultCount
   *          Number of entries in hashMapResults.
   * @param scratch1
   *          Pre-allocated storage to internal use.
   */
  public int finishOuter(VectorizedRowBatch batch,
      int[] matchs, int[] matchHashMapResultIndices, int matchCount,
      int[] nonSpills, int nonSpillCount,
      int[] spills, int[] spillHashMapResultIndices, int spillCount,
      VectorMapJoinHashMapResult[] hashMapResults, int hashMapResultCount,
      int[] scratch1) throws IOException, HiveException {

     int numSel = 0;

    // At this point we have determined the matching rows only for the ON equality condition(s).
    // Implicitly, non-matching rows are those in the selected array minus matchs.

    // Next, for outer join, apply any ON predicates to filter down the matches.
    if (matchCount > 0 && bigTableFilterExpressions.length > 0) {

      System.arraycopy(matchs, 0, batch.selected, 0, matchCount);
      batch.size = matchCount;

      // Non matches will be removed from the selected array.
      for (VectorExpression ve : bigTableFilterExpressions) {
        ve.evaluate(batch);
      }

      // LOG.info("finishOuter" +
      //     " filtered batch.selected " + Arrays.toString(Arrays.copyOfRange(batch.selected, 0, batch.size)));

      // Fixup the matchHashMapResultIndices array.
      if (batch.size < matchCount) {
        int numMatch = 0;
        int[] selected = batch.selected;
        for (int i = 0; i < batch.size; i++) {
          if (selected[i] == matchs[numMatch]) {
            matchHashMapResultIndices[numMatch] = matchHashMapResultIndices[i];
            numMatch++;
            if (numMatch == matchCount) {
              break;
            }
          }
        }
        System.arraycopy(batch.selected, 0, matchs, 0, matchCount);
      }
    }
    // LOG.info("finishOuter" +
    //     " matchs[" + matchCount + "] " + intArrayToRangesString(matchs, matchCount) +
    //     " matchHashMapResultIndices " + Arrays.toString(Arrays.copyOfRange(matchHashMapResultIndices, 0, matchCount)));

    // Big table value expressions apply to ALL matching and non-matching rows.
    if (bigTableValueExpressions != null) {

      System.arraycopy(nonSpills, 0, batch.selected, 0, nonSpillCount);
      batch.size = nonSpillCount;

      for (VectorExpression ve: bigTableValueExpressions) {
        ve.evaluate(batch);
      }
    }

    // Determine which rows are non matches by determining the delta between selected and
    // matchs.
    int[] noMatchs = scratch1;
    int noMatchCount = 0;
    if (matchCount < nonSpillCount) {
      // Determine which rows are non matches.
      int matchIndex = 0;
      for (int i = 0; i < nonSpillCount; i++) {
        int candidateIndex = nonSpills[i];
        if (matchIndex < matchCount && candidateIndex == matchs[matchIndex]) {
          matchIndex++;
        } else {
          noMatchs[noMatchCount++] = candidateIndex;
        }
      }
    }
    // LOG.info("finishOuter" +
    //     " noMatchs[" + noMatchCount + "] " + intArrayToRangesString(noMatchs, noMatchCount));


    // When we generate results into the overflow batch, we may still end up with fewer rows
    // in the big table batch.  So, nulSel and the batch's selected array will be rebuilt with
    // just the big table rows that need to be forwarded, minus any rows processed with the
    // overflow batch.
    if (matchCount > 0) {
      numSel = generateOuterHashMapMatchResults(batch,
          matchs, matchHashMapResultIndices, matchCount,
          hashMapResults, numSel);
    }

    if (noMatchCount > 0) {
      numSel = generateOuterHashMapNoMatchResults(batch, noMatchs, noMatchCount, numSel);
    }

    if (spillCount > 0) {
      spillHashMapBatch(batch, (VectorMapJoinHashTableResult[]) hashMapResults,
          spills, spillHashMapResultIndices, spillCount);
    }

    return numSel;
  }

   /**
    * Generate the matching outer join output results for one row of a vectorized row batch into
    * the overflow batch.
    *
    * @param batch
    *          The big table batch.
    * @param batchIndex
    *          Index of the big table row.
    * @param hashMapResult
    *          The hash map result with the small table values.
    */
   private void copyOuterHashMapResultToOverflow(VectorizedRowBatch batch, int batchIndex,
               VectorMapJoinHashMapResult hashMapResult) throws HiveException, IOException {

     // if (hashMapResult.isCappedCountAvailable()) {
     //   LOG.info("copyOuterHashMapResultToOverflow cappedCount " + hashMapResult.cappedCount());
     // }
     ByteSegmentRef byteSegmentRef = hashMapResult.first();
     while (byteSegmentRef != null) {

       // Copy the BigTable values into the overflow batch. Since the overflow batch may
       // not get flushed here, we must copy by value.
       if (bigTableRetainedVectorCopy != null) {
         bigTableRetainedVectorCopy.copyByValue(batch, batchIndex,
                                                overflowBatch, overflowBatch.size);
       }

       // Reference the keys we just copied above.
       if (bigTableVectorCopyOuterKeys != null) {
         bigTableVectorCopyOuterKeys.copyByReference(overflowBatch, overflowBatch.size,
                                                     overflowBatch, overflowBatch.size);
       }

       if (smallTableVectorDeserializeRow != null) {

         byte[] bytes = byteSegmentRef.getBytes();
         int offset = (int) byteSegmentRef.getOffset();
         int length = byteSegmentRef.getLength();
         smallTableVectorDeserializeRow.setBytes(bytes, offset, length);

         smallTableVectorDeserializeRow.deserializeByValue(overflowBatch, overflowBatch.size);
       }

       ++overflowBatch.size;
       if (overflowBatch.size == VectorizedRowBatch.DEFAULT_SIZE) {
         forwardOverflow();
       }

       byteSegmentRef = hashMapResult.next();
      }
     // LOG.info("copyOuterHashMapResultToOverflow overflowBatch.size " + overflowBatch.size);

   }

   /**
    * Generate the matching outer join output results for one vectorized row batch.
    *
    * For each matching row specified by parameter, get the one or more small table values and
    * form join results.
    *
    * (Note: Since all matching and non-matching rows are selected and output for outer joins,
    * we cannot use selected as the matching rows).
    *
    * @param batch
    *          The big table batch with any matching and any non matching rows both as
    *          selected in use.
    * @param matchs
    *          A subset of the rows of the batch that are matches.
    * @param matchHashMapResultIndices
    *          For each entry in matches, the index into the hashMapResult.
    * @param matchSize
    *          Number of matches in matchs.
    * @param hashMapResults
    *          The array of all hash map results for the batch.
    * @param numSel
    *          The current count of rows in the rebuilding of the selected array.
    *
    * @return
    *          The new count of selected rows.
    */
   protected int generateOuterHashMapMatchResults(VectorizedRowBatch batch,
       int[] matchs, int[] matchHashMapResultIndices, int matchSize,
       VectorMapJoinHashMapResult[] hashMapResults, int numSel)
               throws IOException, HiveException {

     int[] selected = batch.selected;

     // Generate result within big table batch when single small table value.  Otherwise, copy
     // to overflow batch.

     for (int i = 0; i < matchSize; i++) {
       int batchIndex = matchs[i];

       int hashMapResultIndex = matchHashMapResultIndices[i];
       VectorMapJoinHashMapResult hashMapResult = hashMapResults[hashMapResultIndex];

       if (!hashMapResult.isSingleRow()) {

         // Multiple small table rows require use of the overflow batch.
         copyOuterHashMapResultToOverflow(batch, batchIndex, hashMapResult);
       } else {

         // Generate join result in big table batch.
         ByteSegmentRef byteSegmentRef = hashMapResult.first();

         if (bigTableVectorCopyOuterKeys != null) {
           bigTableVectorCopyOuterKeys.copyByReference(batch, batchIndex, batch, batchIndex);
         }

         if (smallTableVectorDeserializeRow != null) {

           byte[] bytes = byteSegmentRef.getBytes();
           int offset = (int) byteSegmentRef.getOffset();
           int length = byteSegmentRef.getLength();
           smallTableVectorDeserializeRow.setBytes(bytes, offset, length);

           smallTableVectorDeserializeRow.deserializeByValue(batch, batchIndex);
         }

         // Remember this big table row was used for an output result.
         selected[numSel++] = batchIndex;
       }
     }
     return numSel;
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
    * @param numSel
    *          The current count of rows in the rebuilding of the selected array.
    *
    * @return
    *          The new count of selected rows.
    */
   protected int generateOuterHashMapNoMatchResults(VectorizedRowBatch batch, int[] noMatchs,
       int noMatchSize, int numSel) throws IOException, HiveException {
     int[] selected = batch.selected;

     // Generate result within big table batch with null small table results, using isRepeated
     // if possible.

     if (numSel == 0) {

       // There were 0 matching rows -- so we can use the isRepeated optimization for the non
       // matching rows.

       // Mark any scratch small table scratch columns that would normally receive a copy of the
       // key as null and repeating.
       for (int column : bigTableOuterKeyOutputVectorColumns) {
         ColumnVector colVector = batch.cols[column];
         colVector.isRepeating = true;
         colVector.noNulls = false;
         colVector.isNull[0] = true;
       }

       // Small table values are set to null and repeating.
       for (int column : smallTableOutputVectorColumns) {
         ColumnVector colVector = batch.cols[column];
         colVector.isRepeating = true;
         colVector.noNulls = false;
         colVector.isNull[0] = true;
       }

       // Rebuild the selected array.
       for (int i = 0; i < noMatchSize; i++) {
         int batchIndex = noMatchs[i];
         selected[numSel++] = batchIndex;
       }
     } else {

       // Set null information in the small table results area.

       for (int i = 0; i < noMatchSize; i++) {
         int batchIndex = noMatchs[i];

         // Mark any scratch small table scratch columns that would normally receive a copy of the
         // key as null, too.
         for (int column : bigTableOuterKeyOutputVectorColumns) {
           ColumnVector colVector = batch.cols[column];
           colVector.noNulls = false;
           colVector.isNull[batchIndex] = true;
         }

         // Small table values are set to null.
         for (int column : smallTableOutputVectorColumns) {
           ColumnVector colVector = batch.cols[column];
           colVector.noNulls = false;
           colVector.isNull[batchIndex] = true;
         }

         selected[numSel++] = batchIndex;
       }
     }
     return numSel;
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
   * @param hashMapResults
   *          The array of all hash map results for the batch.
   * @param scratch1
   *          Pre-allocated storage to internal use.
   */
  public int finishOuterRepeated(VectorizedRowBatch batch, JoinUtil.JoinResult joinResult,
      VectorMapJoinHashMapResult hashMapResult, int[] scratch1)
          throws IOException, HiveException {

    int numSel = 0;

    if (joinResult == JoinUtil.JoinResult.MATCH && bigTableFilterExpressions.length > 0) {

      // Since it is repeated, the evaluation of the filter will knock the whole batch out.
      // But since we are doing outer join, we want to keep non-matches.

      // First, remember selected;
      int[] rememberSelected = scratch1;
      int rememberBatchSize = batch.size;
      if (batch.selectedInUse) {
        System.arraycopy(batch.selected, 0, rememberSelected, 0, batch.size);
      }

      // Filter.
      for (VectorExpression ve : bigTableFilterExpressions) {
        ve.evaluate(batch);
      }

      // Convert a filter out to a non match.
      if (batch.size == 0) {
        joinResult = JoinUtil.JoinResult.NOMATCH;
        if (batch.selectedInUse) {
          System.arraycopy(rememberSelected, 0, batch.selected, 0, rememberBatchSize);
          // LOG.info("finishOuterRepeated batch #" + batchCounter + " filter out converted to no matchs " +
          //     Arrays.toString(Arrays.copyOfRange(batch.selected, 0, rememberBatchSize)));
        } else {
          // LOG.info("finishOuterRepeated batch #" + batchCounter + " filter out converted to no matchs batch size " +
          //     rememberBatchSize);
        }
        batch.size = rememberBatchSize;
      }
    }

    // LOG.info("finishOuterRepeated batch #" + batchCounter + " " + joinResult.name() + " batch.size " + batch.size);
    switch (joinResult) {
    case MATCH:
      // Run our value expressions over whole batch.
      if (bigTableValueExpressions != null) {
        for(VectorExpression ve: bigTableValueExpressions) {
          ve.evaluate(batch);
        }
      }

      // Use a common method applicable for inner and outer.
      numSel = generateHashMapResultRepeatedAll(batch, hashMapResult);
      break;
    case SPILL:
      // Whole batch is spilled.
      spillBatchRepeated(batch, (VectorMapJoinHashTableResult) hashMapResult);
      break;
    case NOMATCH:
      // Run our value expressions over whole batch.
      if (bigTableValueExpressions != null) {
        for(VectorExpression ve: bigTableValueExpressions) {
          ve.evaluate(batch);
        }
      }

      numSel = generateOuterNullsRepeatedAll(batch);
      break;
    }

    return numSel;
  }

  /**
   * Generate the non-match outer join output results for the whole repeating vectorized
   * row batch.
   *
   * Each row will get nulls for all small table values.
   *
   * @param batch
   *          The big table batch.
   * @return
   *          The new count of selected rows.
   */
  protected int generateOuterNullsRepeatedAll(VectorizedRowBatch batch) throws HiveException {

    int[] selected = batch.selected;
    boolean selectedInUse = batch.selectedInUse;

    // Generate result within big table batch using is repeated for null small table results.

    if (batch.selectedInUse) {
      // The selected array is already filled in as we want it.
    } else {
      for (int i = 0; i < batch.size; i++) {
        selected[i] = i;
      }
      batch.selectedInUse = true;
    }

    for (int column : smallTableOutputVectorColumns) {
      ColumnVector colVector = batch.cols[column];
      colVector.noNulls = false;
      colVector.isNull[0] = true;
      colVector.isRepeating = true;
    }

    // Mark any scratch small table scratch columns that would normally receive a copy of the key
    // as null, too.
   for (int column : bigTableOuterKeyOutputVectorColumns) {
      ColumnVector colVector = batch.cols[column];
      colVector.noNulls = false;
      colVector.isNull[0] = true;
      colVector.isRepeating = true;
    }

    // for (int i = 0; i < batch.size; i++) {
    //   int bigTableIndex = selected[i];
    //   VectorizedBatchUtil.debugDisplayOneRow(batch, bigTableIndex, taskName + ", " + getOperatorId() + " VectorMapJoinCommonOperator generate generateOuterNullsRepeatedAll batch");
    // }

    return batch.size;
  }
}