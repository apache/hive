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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

// Single-Column Long hash table import.
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMultiSet;

// Single-Column Long specific imports.
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/*
 * Specialized class for doing a vectorized map join that is an inner join on a Single-Column Long
 * and only big table columns appear in the join result so a hash multi-set is used.
 */
public class VectorMapJoinInnerBigOnlyLongOperator extends VectorMapJoinInnerBigOnlyGenerateResultOperator {

  private static final long serialVersionUID = 1L;

  //------------------------------------------------------------------------------------------------

  private static final String CLASS_NAME = VectorMapJoinInnerBigOnlyLongOperator.class.getName();
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
  private transient VectorMapJoinLongHashMultiSet hashMultiSet;

  //---------------------------------------------------------------------------
  // Single-Column Long specific members.
  //

  // For integers, we have optional min/max filtering.
  private transient boolean useMinMax;
  private transient long min;
  private transient long max;

  // The column number for this one column join specialization.
  private transient int singleJoinColumn;

  //---------------------------------------------------------------------------
  // Pass-thru constructors.
  //

  /** Kryo ctor. */
  protected VectorMapJoinInnerBigOnlyLongOperator() {
    super();
  }

  public VectorMapJoinInnerBigOnlyLongOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorMapJoinInnerBigOnlyLongOperator(CompilationOpContext ctx,
      VectorizationContext vContext, OperatorDesc conf) throws HiveException {
    super(ctx, vContext, conf);
  }

  //---------------------------------------------------------------------------
  // Process Single-Column Long Inner Big-Only Join on a vectorized row batch.
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
         * Initialize Single-Column Long members for this specialized class.
         */

        singleJoinColumn = bigTableKeyColumnMap[0];

        needCommonSetup = false;
      }

      if (needHashTableSetup) {
        // Setup our hash table specialization.  It will be the first time the process
        // method is called, or after a Hybrid Grace reload.

        /*
         * Get our Single-Column Long hash multi-set information for this specialized class.
         */

        hashMultiSet = (VectorMapJoinLongHashMultiSet) vectorMapJoinHashTable;
        useMinMax = hashMultiSet.useMinMax();
        if (useMinMax) {
          min = hashMultiSet.min();
          max = hashMultiSet.max();
        }

        needHashTableSetup = false;
      }

      batchCounter++;

      // Do the per-batch setup for an inner big-only join.

      // (Currently none)
      // innerBigOnlyPerBatchSetup(batch);

      // For inner joins, we may apply the filter(s) now.
      for(VectorExpression ve : bigTableFilterExpressions) {
        ve.evaluate(batch);
      }

      final int inputLogicalSize = batch.size;

      if (inputLogicalSize == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " empty");
        }
        return;
      }

      // Perform any key expressions.  Results will go into scratch columns.
      if (bigTableKeyExpressions != null) {
        for (VectorExpression ve : bigTableKeyExpressions) {
          ve.evaluate(batch);
        }
      }

      /*
       * Single-Column Long specific declarations.
       */

      // The one join column for this specialized class.
      LongColumnVector joinColVector = (LongColumnVector) batch.cols[singleJoinColumn];
      long[] vector = joinColVector.vector;

      /*
       * Single-Column Long check for repeating.
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
         * Single-Column Long specific repeated lookup.
         */

        JoinUtil.JoinResult joinResult;
        if (!joinColVector.noNulls && joinColVector.isNull[0]) {
          joinResult = JoinUtil.JoinResult.NOMATCH;
        } else {
          long key = vector[0];
          if (useMinMax && (key < min || key > max)) {
            // Out of range for whole batch.
            joinResult = JoinUtil.JoinResult.NOMATCH;
          } else {
            joinResult = hashMultiSet.contains(key, hashMultiSetResults[0]);
          }
        }

        /*
         * Common repeated join result processing.
         */

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " repeated joinResult " + joinResult.name());
        }
        finishInnerBigOnlyRepeated(batch, joinResult, hashMultiSetResults[0]);
      } else {

        /*
         * NOT Repeating.
         */

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " non-repeated");
        }

        // We remember any matching rows in matchs / matchSize.  At the end of the loop,
        // selected / batch.size will represent both matching and non-matching rows for outer join.
        // Only deferred rows will have been removed from selected.
        int selected[] = batch.selected;
        boolean selectedInUse = batch.selectedInUse;

        int hashMultiSetResultCount = 0;
        int allMatchCount = 0;
        int equalKeySeriesCount = 0;
        int spillCount = 0;

        /*
         * Single-Column Long specific variables.
         */

        long saveKey = 0;

        // We optimize performance by only looking up the first key in a series of equal keys.
        boolean haveSaveKey = false;
        JoinUtil.JoinResult saveJoinResult = JoinUtil.JoinResult.NOMATCH;

        // Logical loop over the rows in the batch since the batch may have selected in use.
        for (int logical = 0; logical < inputLogicalSize; logical++) {
          int batchIndex = (selectedInUse ? selected[logical] : logical);

          /*
           * Single-Column Long get key.
           */

          long currentKey;
          boolean isNull;
          if (!joinColVector.noNulls && joinColVector.isNull[batchIndex]) {
            currentKey = 0;
            isNull = true;
          } else {
            currentKey = vector[batchIndex];
            isNull = false;
          }

          /*
           * Equal key series checking.
           */

          if (isNull || !haveSaveKey || currentKey != saveKey) {

            // New key.

            if (haveSaveKey) {
              // Move on with our counts.
              switch (saveJoinResult) {
              case MATCH:
                // We have extracted the count from the hash multi-set result, so we don't keep it.
                equalKeySeriesCount++;
                break;
              case SPILL:
                // We keep the hash multi-set result for its spill information.
                hashMultiSetResultCount++;
                break;
              case NOMATCH:
                break;
              }
            }

            if (isNull) {
              saveJoinResult = JoinUtil.JoinResult.NOMATCH;
              haveSaveKey = false;
            } else {
              // Regardless of our matching result, we keep that information to make multiple use
              // of it for a possible series of equal keys.
              haveSaveKey = true;
  
              /*
               * Single-Column Long specific save key.
               */
  
              saveKey = currentKey;
  
              /*
               * Single-Column Long specific lookup key.
               */
  
              if (useMinMax && (currentKey < min || currentKey > max)) {
                // Key out of range for whole hash table.
                saveJoinResult = JoinUtil.JoinResult.NOMATCH;
              } else {
                saveJoinResult = hashMultiSet.contains(currentKey, hashMultiSetResults[hashMultiSetResultCount]);
              }
            }

            /*
             * Common inner big-only join result processing.
             */

            switch (saveJoinResult) {
            case MATCH:
              equalKeySeriesValueCounts[equalKeySeriesCount] = hashMultiSetResults[hashMultiSetResultCount].count();
              equalKeySeriesAllMatchIndices[equalKeySeriesCount] = allMatchCount;
              equalKeySeriesDuplicateCounts[equalKeySeriesCount] = 1;
              allMatchs[allMatchCount++] = batchIndex;
              // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " MATCH isSingleValue " + equalKeySeriesIsSingleValue[equalKeySeriesCount] + " currentKey " + currentKey);
              break;

            case SPILL:
              spills[spillCount] = batchIndex;
              spillHashMapResultIndices[spillCount] = hashMultiSetResultCount;
              spillCount++;
              break;

            case NOMATCH:
              // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " NOMATCH" + " currentKey " + currentKey);
              break;
            }
          } else {
            // Series of equal keys.

            switch (saveJoinResult) {
            case MATCH:
              equalKeySeriesDuplicateCounts[equalKeySeriesCount]++;
              allMatchs[allMatchCount++] = batchIndex;
              // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " MATCH duplicate");
              break;

            case SPILL:
              spills[spillCount] = batchIndex;
              spillHashMapResultIndices[spillCount] = hashMultiSetResultCount;
              spillCount++;
              break;

            case NOMATCH:
              // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " NOMATCH duplicate");
              break;
            }
          }
        }

        if (haveSaveKey) {
          // Update our counts for the last key.
          switch (saveJoinResult) {
          case MATCH:
            // We have extracted the count from the hash multi-set result, so we don't keep it.
            equalKeySeriesCount++;
            break;
          case SPILL:
            // We keep the hash multi-set result for its spill information.
            hashMultiSetResultCount++;
            break;
          case NOMATCH:
            break;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME +
              " allMatchs " + intArrayToRangesString(allMatchs, allMatchCount) +
              " equalKeySeriesValueCounts " + longArrayToRangesString(equalKeySeriesValueCounts, equalKeySeriesCount) +
              " equalKeySeriesAllMatchIndices " + intArrayToRangesString(equalKeySeriesAllMatchIndices, equalKeySeriesCount) +
              " equalKeySeriesDuplicateCounts " + intArrayToRangesString(equalKeySeriesDuplicateCounts, equalKeySeriesCount) +
              " spills " + intArrayToRangesString(spills, spillCount) +
              " spillHashMapResultIndices " + intArrayToRangesString(spillHashMapResultIndices, spillCount) +
              " hashMapResults " + Arrays.toString(Arrays.copyOfRange(hashMultiSetResults, 0, hashMultiSetResultCount)));
        }

        finishInnerBigOnly(batch,
            allMatchCount, equalKeySeriesCount, spillCount,
            (VectorMapJoinHashTableResult[]) hashMultiSetResults, hashMultiSetResultCount);
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
