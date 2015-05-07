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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

// Single-Column Long hash table import.
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;

// Single-Column Long specific imports.
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/*
 * Specialized class for doing a vectorized map join that is an outer join on a Single-Column Long
 * using a hash map.
 */
public class VectorMapJoinOuterLongOperator extends VectorMapJoinOuterGenerateResultOperator {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorMapJoinOuterLongOperator.class.getName());
  private static final String CLASS_NAME = VectorMapJoinOuterLongOperator.class.getName();

  // (none)

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  // The hash map for this specialized class.
  private transient VectorMapJoinLongHashMap hashMap;

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

  public VectorMapJoinOuterLongOperator() {
    super();
  }

  public VectorMapJoinOuterLongOperator(VectorizationContext vContext, OperatorDesc conf) throws HiveException {
    super(vContext, conf);
  }

  //---------------------------------------------------------------------------
  // Process Single-Column Long Outer Join on a vectorized row batch.
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
         * Get our Single-Column Long hash map information for this specialized class.
         */

        hashMap = (VectorMapJoinLongHashMap) vectorMapJoinHashTable;
        useMinMax = hashMap.useMinMax();
        if (useMinMax) {
          min = hashMap.min();
          max = hashMap.max();
        }

        needHashTableSetup = false;
      }

      batchCounter++;

      // Do the per-batch setup for an outer join.

      outerPerBatchSetup(batch);

      // For outer join, DO NOT apply filters yet.  It is incorrect for outer join to
      // apply the filter before hash table matching.

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

      // We rebuild in-place the selected array with rows destine to be forwarded.
      int numSel = 0;

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
          // Null key is no match for whole batch.
          joinResult = JoinUtil.JoinResult.NOMATCH;
        } else {
          // Handle *repeated* join key, if found.
          long key = vector[0];
          if (useMinMax && (key < min || key > max)) {
            // Out of range for whole batch.
            joinResult = JoinUtil.JoinResult.NOMATCH;
          } else {
            joinResult = hashMap.lookup(key, hashMapResults[0]);
          }
        }

        /*
         * Common repeated join result processing.
         */

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " repeated joinResult " + joinResult.name());
        }
        numSel = finishOuterRepeated(batch, joinResult, hashMapResults[0], scratch1);
      } else {

        /*
         * NOT Repeating.
         */

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " non-repeated");
        }

        int selected[] = batch.selected;
        boolean selectedInUse = batch.selectedInUse;

        // For outer join we must apply the filter after match and cause some matches to become
        // non-matches, we do not track non-matches here.  Instead we remember all non spilled rows
        // and compute non matches later in finishOuter.
        int hashMapResultCount = 0;
        int matchCount = 0;
        int nonSpillCount = 0;
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
           * Single-Column Long outer null detection.
           */

          boolean isNull = !joinColVector.noNulls && joinColVector.isNull[batchIndex];

          if (isNull) {

            // Have that the NULL does not interfere with the current equal key series, if there
            // is one. We do not set saveJoinResult.
            //
            //    Let a current MATCH equal key series keep going, or
            //    Let a current SPILL equal key series keep going, or
            //    Let a current NOMATCH keep not matching.

            // Remember non-matches for Outer Join.
            nonSpills[nonSpillCount++] = batchIndex;
            // LOG.debug(CLASS_NAME + " logical " + logical + " batchIndex " + batchIndex + " NULL");
          } else {

            /*
             * Single-Column Long outer get key.
             */

            long currentKey = vector[batchIndex];

            /*
             * Equal key series checking.
             */

            if (!haveSaveKey || currentKey != saveKey) {
              // New key.

              if (haveSaveKey) {
                // Move on with our count(s).
                switch (saveJoinResult) {
                case MATCH:
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
                saveJoinResult = hashMap.lookup(currentKey, hashMapResults[hashMapResultCount]);
              }
              // LOG.debug(CLASS_NAME + " logical " + logical + " batchIndex " + batchIndex + " New Key " + saveJoinResult.name());
            } else {
              // LOG.debug(CLASS_NAME + " logical " + logical + " batchIndex " + batchIndex + " Key Continues " + saveJoinResult.name());
            }

            /*
             * Common outer join result processing.
             */

            switch (saveJoinResult) {
            case MATCH:
              matchs[matchCount] = batchIndex;
              matchHashMapResultIndices[matchCount] = hashMapResultCount;
              matchCount++;
              nonSpills[nonSpillCount++] = batchIndex;
              break;

            case SPILL:
              spills[spillCount] = batchIndex;
              spillHashMapResultIndices[spillCount] = hashMapResultCount;
              spillCount++;
              break;

            case NOMATCH:
              nonSpills[nonSpillCount++] = batchIndex;
              // VectorizedBatchUtil.debugDisplayOneRow(batch, batchIndex, CLASS_NAME + " NOMATCH duplicate");
              break;
            }
          }
        }

        if (haveSaveKey) {
          // Account for last equal key sequence.
          switch (saveJoinResult) {
          case MATCH:
          case SPILL:
            hashMapResultCount++;
            break;
          case NOMATCH:
            break;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter +
              " matchs " + intArrayToRangesString(matchs, matchCount) +
              " matchHashMapResultIndices " + intArrayToRangesString(matchHashMapResultIndices, matchCount) +
              " nonSpills " + intArrayToRangesString(nonSpills, nonSpillCount) +
              " spills " + intArrayToRangesString(spills, spillCount) +
              " spillHashMapResultIndices " + intArrayToRangesString(spillHashMapResultIndices, spillCount) +
              " hashMapResults " + Arrays.toString(Arrays.copyOfRange(hashMapResults, 0, hashMapResultCount)));
        }

        // We will generate results for all matching and non-matching rows.
        // Note that scratch1 is undefined at this point -- it's preallocated storage.
        numSel = finishOuter(batch,
                    matchs, matchHashMapResultIndices, matchCount,
                    nonSpills, nonSpillCount,
                    spills, spillHashMapResultIndices, spillCount,
                    hashMapResults, hashMapResultCount,
                    scratch1);
      }

      batch.selectedInUse = true;
      batch.size =  numSel;

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
