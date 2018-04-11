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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import org.apache.hadoop.hive.ql.plan.VectorDesc;
// Single-Column String hash table import.
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;

// Single-Column String specific imports.
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;

/*
 * Specialized class for doing a vectorized map join that is an outer join on a Single-Column String
 * using a hash map.
 */
public class VectorMapJoinOuterStringOperator extends VectorMapJoinOuterGenerateResultOperator {

  private static final long serialVersionUID = 1L;

  //------------------------------------------------------------------------------------------------

  private static final String CLASS_NAME = VectorMapJoinOuterStringOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  protected String getLoggingPrefix() {
    return super.getLoggingPrefix(CLASS_NAME);
  }

  //------------------------------------------------------------------------------------------------

  // (none)

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  // The hash map for this specialized class.
  private transient VectorMapJoinBytesHashMap hashMap;

  //---------------------------------------------------------------------------
  // Single-Column String specific members.
  //

  // The column number for this one column join specialization.
  private transient int singleJoinColumn;

  //---------------------------------------------------------------------------
  // Pass-thru constructors.
  //

  /** Kryo ctor. */
  protected VectorMapJoinOuterStringOperator() {
    super();
  }

  public VectorMapJoinOuterStringOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinOuterStringOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  //---------------------------------------------------------------------------
  // Process Single-Column String Outer Join on a vectorized row batch.
  //

  @Override
  public void process(Object row, int tag) throws HiveException {

    try {
      VectorizedRowBatch batch = (VectorizedRowBatch) row;

      alias = (byte) tag;

      if (needCommonSetup) {
        // Our one time process method initialization.
        commonSetup(batch);

        /*
         * Initialize Single-Column String members for this specialized class.
         */

        singleJoinColumn = bigTableKeyColumnMap[0];

        needCommonSetup = false;
      }

      if (needHashTableSetup) {
        // Setup our hash table specialization.  It will be the first time the process
        // method is called, or after a Hybrid Grace reload.

        /*
         * Get our Single-Column String hash map information for this specialized class.
         */

        hashMap = (VectorMapJoinBytesHashMap) vectorMapJoinHashTable;

        needHashTableSetup = false;
      }

      batchCounter++;

      final int inputLogicalSize = batch.size;

      if (inputLogicalSize == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " empty");
        }
        return;
      }

      // Do the per-batch setup for an outer join.

      outerPerBatchSetup(batch);

      // For outer join, remember our input rows before ON expression filtering or before
      // hash table matching so we can generate results for all rows (matching and non matching)
      // later.
      boolean inputSelectedInUse = batch.selectedInUse;
      if (inputSelectedInUse) {
        // if (!verifyMonotonicallyIncreasing(batch.selected, batch.size)) {
        //   throw new HiveException("batch.selected is not in sort order and unique");
        // }
        System.arraycopy(batch.selected, 0, inputSelected, 0, inputLogicalSize);
      }

      // Filtering for outer join just removes rows available for hash table matching.
      boolean someRowsFilteredOut =  false;
      if (bigTableFilterExpressions.length > 0) {
        // Since the input
        for (VectorExpression ve : bigTableFilterExpressions) {
          ve.evaluate(batch);
        }
        someRowsFilteredOut = (batch.size != inputLogicalSize);
        if (LOG.isDebugEnabled()) {
          if (batch.selectedInUse) {
            if (inputSelectedInUse) {
              LOG.debug(CLASS_NAME +
                  " inputSelected " + intArrayToRangesString(inputSelected, inputLogicalSize) +
                  " filtered batch.selected " + intArrayToRangesString(batch.selected, batch.size));
            } else {
              LOG.debug(CLASS_NAME +
                " inputLogicalSize " + inputLogicalSize +
                " filtered batch.selected " + intArrayToRangesString(batch.selected, batch.size));
            }
          }
        }
      }

      // Perform any key expressions.  Results will go into scratch columns.
      if (bigTableKeyExpressions != null) {
        for (VectorExpression ve : bigTableKeyExpressions) {
          ve.evaluate(batch);
        }
      }

      /*
       * Single-Column String specific declarations.
       */

      // The one join column for this specialized class.
      BytesColumnVector joinColVector = (BytesColumnVector) batch.cols[singleJoinColumn];
      byte[][] vector = joinColVector.vector;
      int[] start = joinColVector.start;
      int[] length = joinColVector.length;

      /*
       * Single-Column String check for repeating.
       */

      // Check single column for repeating.
      boolean allKeyInputColumnsRepeating = joinColVector.isRepeating;

      if (allKeyInputColumnsRepeating) {

        /*
         * Repeating.
         */

        // All key input columns are repeating.  Generate key once.  Lookup once.
        // Since the key is repeated, we must use entry 0 regardless of selectedInUse.

        /*
         * Single-Column String specific repeated lookup.
         */

        JoinUtil.JoinResult joinResult;
        if (batch.size == 0) {
          // Whole repeated key batch was filtered out.
          joinResult = JoinUtil.JoinResult.NOMATCH;
        } else if (!joinColVector.noNulls && joinColVector.isNull[0]) {
          // Any (repeated) null key column is no match for whole batch.
          joinResult = JoinUtil.JoinResult.NOMATCH;
        } else {
          // Handle *repeated* join key, if found.
          byte[] keyBytes = vector[0];
          int keyStart = start[0];
          int keyLength = length[0];
          joinResult = hashMap.lookup(keyBytes, keyStart, keyLength, hashMapResults[0]);
        }

        /*
         * Common repeated join result processing.
         */

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " repeated joinResult " + joinResult.name());
        }
        finishOuterRepeated(batch, joinResult, hashMapResults[0], someRowsFilteredOut,
            inputSelectedInUse, inputLogicalSize);
      } else {

        /*
         * NOT Repeating.
         */

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " non-repeated");
        }

        int selected[] = batch.selected;
        boolean selectedInUse = batch.selectedInUse;

        int hashMapResultCount = 0;
        int allMatchCount = 0;
        int equalKeySeriesCount = 0;
        int spillCount = 0;

        boolean atLeastOneNonMatch = someRowsFilteredOut;

        /*
         * Single-Column String specific variables.
         */

        int saveKeyBatchIndex = -1;

        // We optimize performance by only looking up the first key in a series of equal keys.
        boolean haveSaveKey = false;
        JoinUtil.JoinResult saveJoinResult = JoinUtil.JoinResult.NOMATCH;

        // Logical loop over the rows in the batch since the batch may have selected in use.
        for (int logical = 0; logical < batch.size; logical++) {
          int batchIndex = (selectedInUse ? selected[logical] : logical);

          // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, taskName + ", " + getOperatorId() + " candidate " + CLASS_NAME + " batch");

          /*
           * Single-Column String outer null detection.
           */

          boolean isNull = !joinColVector.noNulls && joinColVector.isNull[batchIndex];

          if (isNull) {

            // Have that the NULL does not interfere with the current equal key series, if there
            // is one. We do not set saveJoinResult.
            //
            //    Let a current MATCH equal key series keep going, or
            //    Let a current SPILL equal key series keep going, or
            //    Let a current NOMATCH keep not matching.

            atLeastOneNonMatch = true;

            // LOG.debug(CLASS_NAME + " logical " + logical + " batchIndex " + batchIndex + " NULL");
          } else {

            /*
             * Single-Column String outer get key.
             */

            // Implicit -- use batchIndex.

            /*
             * Equal key series checking.
             */

            if (!haveSaveKey ||
                StringExpr.equal(vector[saveKeyBatchIndex], start[saveKeyBatchIndex], length[saveKeyBatchIndex],
                                   vector[batchIndex], start[batchIndex], length[batchIndex]) == false) {
              // New key.

              if (haveSaveKey) {
                // Move on with our counts.
                switch (saveJoinResult) {
                case MATCH:
                  hashMapResultCount++;
                  equalKeySeriesCount++;
                  break;
                case SPILL:
                  hashMapResultCount++;
                  break;
                case NOMATCH:
                  break;
                }
              }

              // Regardless of our matching result, we keep that information to make multiple use
              // of it for a possible series of equal keys.
              haveSaveKey = true;

              /*
               * Single-Column String specific save key.
               */

              saveKeyBatchIndex = batchIndex;

              /*
               * Single-Column Long specific lookup key.
               */

              byte[] keyBytes = vector[batchIndex];
              int keyStart = start[batchIndex];
              int keyLength = length[batchIndex];
              saveJoinResult = hashMap.lookup(keyBytes, keyStart, keyLength, hashMapResults[hashMapResultCount]);

              /*
               * Common outer join result processing.
               */

              switch (saveJoinResult) {
              case MATCH:
                equalKeySeriesHashMapResultIndices[equalKeySeriesCount] = hashMapResultCount;
                equalKeySeriesAllMatchIndices[equalKeySeriesCount] = allMatchCount;
                equalKeySeriesIsSingleValue[equalKeySeriesCount] = hashMapResults[hashMapResultCount].isSingleRow();
                equalKeySeriesDuplicateCounts[equalKeySeriesCount] = 1;
                allMatchs[allMatchCount++] = batchIndex;
                // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " MATCH isSingleValue " + equalKeySeriesIsSingleValue[equalKeySeriesCount] + " currentKey " + currentKey);
                break;

              case SPILL:
                spills[spillCount] = batchIndex;
                spillHashMapResultIndices[spillCount] = hashMapResultCount;
                spillCount++;
                break;

              case NOMATCH:
                atLeastOneNonMatch = true;
                // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " NOMATCH" + " currentKey " + currentKey);
                break;
              }
            } else {
              // LOG.debug(CLASS_NAME + " logical " + logical + " batchIndex " + batchIndex + " Key Continues " + saveKey + " " + saveJoinResult.name());

              // Series of equal keys.

              switch (saveJoinResult) {
              case MATCH:
                equalKeySeriesDuplicateCounts[equalKeySeriesCount]++;
                allMatchs[allMatchCount++] = batchIndex;
                // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " MATCH duplicate");
                break;

              case SPILL:
                spills[spillCount] = batchIndex;
                spillHashMapResultIndices[spillCount] = hashMapResultCount;
                spillCount++;
                break;

              case NOMATCH:
                // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " NOMATCH duplicate");
                break;
              }
            }
            // if (!verifyMonotonicallyIncreasing(allMatchs, allMatchCount)) {
            //   throw new HiveException("allMatchs is not in sort order and unique");
            // }
          }
        }

        if (haveSaveKey) {
          // Update our counts for the last key.
          switch (saveJoinResult) {
          case MATCH:
            hashMapResultCount++;
            equalKeySeriesCount++;
            break;
          case SPILL:
            hashMapResultCount++;
            break;
          case NOMATCH:
            break;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter +
              " allMatchs " + intArrayToRangesString(allMatchs,allMatchCount) +
              " equalKeySeriesHashMapResultIndices " + intArrayToRangesString(equalKeySeriesHashMapResultIndices, equalKeySeriesCount) +
              " equalKeySeriesAllMatchIndices " + intArrayToRangesString(equalKeySeriesAllMatchIndices, equalKeySeriesCount) +
              " equalKeySeriesIsSingleValue " + Arrays.toString(Arrays.copyOfRange(equalKeySeriesIsSingleValue, 0, equalKeySeriesCount)) +
              " equalKeySeriesDuplicateCounts " + Arrays.toString(Arrays.copyOfRange(equalKeySeriesDuplicateCounts, 0, equalKeySeriesCount)) +
              " atLeastOneNonMatch " + atLeastOneNonMatch +
              " inputSelectedInUse " + inputSelectedInUse +
              " inputLogicalSize " + inputLogicalSize +
              " spills " + intArrayToRangesString(spills, spillCount) +
              " spillHashMapResultIndices " + intArrayToRangesString(spillHashMapResultIndices, spillCount) +
              " hashMapResults " + Arrays.toString(Arrays.copyOfRange(hashMapResults, 0, hashMapResultCount)));
        }

        // We will generate results for all matching and non-matching rows.
        finishOuter(batch,
            allMatchCount, equalKeySeriesCount, atLeastOneNonMatch,
            inputSelectedInUse, inputLogicalSize,
            spillCount, hashMapResultCount);
      }

      if (batch.size > 0) {
        // Forward any remaining selected rows.
        forwardBigTableBatch(batch);
      }

    } catch (IOException e) {
      throw new HiveException(e);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
