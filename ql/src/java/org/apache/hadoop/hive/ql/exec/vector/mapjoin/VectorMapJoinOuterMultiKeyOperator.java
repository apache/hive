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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

// Multi-Key hash table import.
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;

// Multi-Key specific imports.
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;

/*
 * Specialized class for doing a vectorized map join that is an outer join on Multi-Key
 * using a hash map.
 */
public class VectorMapJoinOuterMultiKeyOperator extends VectorMapJoinOuterGenerateResultOperator {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(VectorMapJoinOuterMultiKeyOperator.class.getName());
  private static final String CLASS_NAME = VectorMapJoinOuterMultiKeyOperator.class.getName();

  // (none)

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  // The hash map for this specialized class.
  private transient VectorMapJoinBytesHashMap hashMap;

  //---------------------------------------------------------------------------
  // Multi-Key specific members.
  //

  // Object that can take a set of columns in row in a vectorized row batch and serialized it.
  private transient VectorSerializeRow keyVectorSerializeWrite;

  // The BinarySortable serialization of the current key.
  private transient Output currentKeyOutput;

  // The BinarySortable serialization of the saved key for a possible series of equal keys.
  private transient Output saveKeyOutput;

  //---------------------------------------------------------------------------
  // Pass-thru constructors.
  //

  public VectorMapJoinOuterMultiKeyOperator() {
    super();
  }

  public VectorMapJoinOuterMultiKeyOperator(VectorizationContext vContext, OperatorDesc conf) throws HiveException {
    super(vContext, conf);
  }

  //---------------------------------------------------------------------------
  // Process Multi-Key Outer Join on a vectorized row batch.
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
         * Initialize Multi-Key members for this specialized class.
         */

        keyVectorSerializeWrite = new VectorSerializeRow(
                        new BinarySortableSerializeWrite(bigTableKeyColumnMap.length));
        keyVectorSerializeWrite.init(bigTableKeyTypeNames, bigTableKeyColumnMap);

        currentKeyOutput = new Output();
        saveKeyOutput = new Output();

        needCommonSetup = false;
      }

      if (needHashTableSetup) {
        // Setup our hash table specialization.  It will be the first time the process
        // method is called, or after a Hybrid Grace reload.

        /*
         * Get our Multi-Key hash map information for this specialized class.
         */

        hashMap = (VectorMapJoinBytesHashMap) vectorMapJoinHashTable;

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
       * Multi-Key specific declarations.
       */

      // None.

      /*
       * Multi-Key Long check for repeating.
       */

      // If all BigTable input columns to key expressions are isRepeating, then
      // calculate key once; lookup once.
      // Also determine if any nulls are present since for a join that means no match.
      boolean allKeyInputColumnsRepeating;
      boolean someKeyInputColumnIsNull = false;  // Only valid if allKeyInputColumnsRepeating is true.
      if (bigTableKeyColumnMap.length == 0) {
       allKeyInputColumnsRepeating = false;
      } else {
        allKeyInputColumnsRepeating = true;
        for (int i = 0; i < bigTableKeyColumnMap.length; i++) {
          ColumnVector colVector = batch.cols[bigTableKeyColumnMap[i]];
          if (!colVector.isRepeating) {
            allKeyInputColumnsRepeating =  false;
            break;
          }
          if (!colVector.noNulls && colVector.isNull[0]) {
            someKeyInputColumnIsNull = true;
          }
        }
      }

      if (allKeyInputColumnsRepeating) {

        /*
         * Repeating.
         */

        // All key input columns are repeating.  Generate key once.  Lookup once.
        // Since the key is repeated, we must use entry 0 regardless of selectedInUse.

        /*
         * Multi-Key specific repeated lookup.
         */

        JoinUtil.JoinResult joinResult;
        if (someKeyInputColumnIsNull) {
          // Any null key column is no match for whole batch.
          joinResult = JoinUtil.JoinResult.NOMATCH;
        } else {

          // All key input columns are repeating.  Generate key once.  Lookup once.
          keyVectorSerializeWrite.setOutput(currentKeyOutput);
          keyVectorSerializeWrite.serializeWrite(batch, 0);
          byte[] keyBytes = currentKeyOutput.getData();
          int keyLength = currentKeyOutput.getLength();
          joinResult = hashMap.lookup(keyBytes, 0, keyLength, hashMapResults[0]);
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
         * Multi-Key specific variables.
         */

        Output temp;

        // We optimize performance by only looking up the first key in a series of equal keys.
        boolean haveSaveKey = false;
        JoinUtil.JoinResult saveJoinResult = JoinUtil.JoinResult.NOMATCH;

        // Logical loop over the rows in the batch since the batch may have selected in use.
        for (int logical = 0; logical < inputLogicalSize; logical++) {
          int batchIndex = (selectedInUse ? selected[logical] : logical);

          /*
           * Multi-Key outer null detection.
           */

          // Generate binary sortable key for current row in vectorized row batch.
          keyVectorSerializeWrite.setOutput(currentKeyOutput);
          boolean isNull = keyVectorSerializeWrite.serializeWrite(batch, batchIndex);

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
             * Multi-Key outer get key.
             */

            // Generated earlier to get possible null(s).

            /*
             * Equal key series checking.
             */

            if (!haveSaveKey || !saveKeyOutput.arraysEquals(currentKeyOutput)) {

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
               * Multi-Key specific save key.
               */

              temp = saveKeyOutput;
              saveKeyOutput = currentKeyOutput;
              currentKeyOutput = temp;

              /*
               * Multi-Key specific lookup key.
               */

              byte[] keyBytes = saveKeyOutput.getData();
              int keyLength = saveKeyOutput.getLength();
              saveJoinResult = hashMap.lookup(keyBytes, 0, keyLength, hashMapResults[hashMapResultCount]);
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
