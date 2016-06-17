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
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import com.google.common.base.Preconditions;

/**
 * Evaluate AND of 2 or more boolean columns and store the boolean result in the
 * output boolean column.  This is a projection or result producing expression (as opposed to
 * a filter expression).
 *
 * Some child boolean columns may be vector expressions evaluated into boolean scratch columns.
 */
public class ColAndCol extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int[] colNums;
  private int outputColumn;
  private int[] mapToChildExpression;
  private int[] andSelected;
  private boolean[] intermediateNulls;

  public ColAndCol(int[] colNums, int outputColumn) {
    this();
    this.colNums = colNums;
    this.outputColumn = outputColumn;
    mapToChildExpression = null;
    andSelected = new int[VectorizedRowBatch.DEFAULT_SIZE];
    intermediateNulls = new boolean[VectorizedRowBatch.DEFAULT_SIZE];
  }

  public ColAndCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    Preconditions.checkState(colNums.length >= 2);

    /*
     * Vector child expressions will be omitted if they are existing boolean vector columns,
     * so the child index does not necessarily index into the childExpressions.
     * We construct a simple index map to the child expression in mapToChildExpression.
     */
    if (childExpressions != null && mapToChildExpression == null) {
      // Some vector child expressions can be omitted (e.g. if they are existing boolean columns).
      mapToChildExpression = new int [colNums.length];
      Arrays.fill(mapToChildExpression, -1);
      for (int c = 0; c < childExpressions.length; c++) {
        VectorExpression ve = childExpressions[c];
        int outputColumn = ve.getOutputColumn();
        int i = 0;
        while (true) {
          if (i >= colNums.length) {
            throw new RuntimeException("Vectorized child expression output not found");
          }
          if (colNums[i] == outputColumn) {
            mapToChildExpression[i] = c;
            break;
          }
          i++;
        }
      }
    }

    final int n = batch.size;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    if (childExpressions != null) {
      /*
       * Evaluate first child expression.  Other child are conditionally evaluated later
       * based on whether there is still a need for AND processing.
       */
      int childExpressionIndex = mapToChildExpression[0];
      if (childExpressionIndex != -1) {
        VectorExpression ve = childExpressions[childExpressionIndex];
        Preconditions.checkState(ve.getOutputColumn() == colNums[0]);
        ve.evaluate(batch);
      }
    }

    int[] sel = batch.selected;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    long[] outputVector = outV.vector;

    /**
     * Null processing complicates the algorithm here for Multi-AND.
     *
     * All true --> true
     * 0 or more true with 1 or more null --> result = null
     * Any false --> false
     *
     * For AND-processing, we remember nulls in the intermediateNulls array as we go along so
     * later we can mark the row as null instead of true when there is a null.
     */

    /*
     * andRepeating will be true when all the children column vectors processed so far are
     * some combination of repeating true and repeating null.
     * andRepeatingIsNull will be true when there has been at least one repeating null column.
     */
    boolean andRepeating = false;
    boolean andRepeatingIsNull = false;

    /*
     * The andSel variable and andSelected member array represent rows that have at have
     * some combination of true and nulls.
     */
    int andSel = 0;

    Arrays.fill(intermediateNulls, 0, VectorizedRowBatch.DEFAULT_SIZE, false);

    // Reset noNulls to true, isNull to false, isRepeating to false.
    outV.reset();

    LongColumnVector firstColVector = (LongColumnVector) batch.cols[colNums[0]];
    long[] firstVector = firstColVector.vector;

    /*
     * We prime the pump by evaluating the first child to see if we are starting with
     * andRepeating/andRepeatingHasNulls or we are starting with andSel/andSelected processing.
     */
    if (firstColVector.isRepeating) {
      if (firstColVector.noNulls || !firstColVector.isNull[0]) {
        if (firstVector[0] == 0) {
          // When the entire child column is repeating false, we are done for AND.
          outV.isRepeating = true;
          outputVector[0] = 0;
          return;
        } else {
          // First column is repeating true.
        }
      } else {
        Preconditions.checkState(firstColVector.isNull[0]);

        // At least one repeating null column.
        andRepeatingIsNull = true;
      }
      andRepeating = true;
    } else if (firstColVector.noNulls) {

      /*
       * No nulls -- so all true rows go in andSel/andSelected.
       */
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (firstVector[i] == 1) {
            andSelected[andSel++] = i;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (firstVector[i] == 1) {
            andSelected[andSel++] = i;
          }
        }
      }
    } else  {

      /*
       * Can be nulls -- so all true rows and null rows go in andSel/andSelected.
       * Remember nulls in our separate intermediateNulls array.
       */
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (firstColVector.isNull[i]) {
            intermediateNulls[i] = true;
            andSelected[andSel++] = i;
          } else if (firstVector[i] == 1) {
            andSelected[andSel++] = i;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (firstColVector.isNull[i]) {
            intermediateNulls[i] = true;
            andSelected[andSel++] = i;
          } else if (firstVector[i] == 1) {
            andSelected[andSel++] = i;
          }
        }
      }
    }

    /*
     * Process child #2 and above.
     */
    int colNum = 1;
    do {
      if (!andRepeating && andSel == 0) {

        /*
         * Since andSel/andSelected represent currently true entries and there are none,
         * then nothing is true (how philosophical!).
         */
        break;
      }

      if (childExpressions != null) {
        int childExpressionIndex = mapToChildExpression[colNum];
        if (childExpressionIndex != -1) {
          if (andRepeating) {

            /*
             * We need to start with a full evaluate on all [selected] rows.
             */
            VectorExpression ve = childExpressions[childExpressionIndex];
            Preconditions.checkState(ve.getOutputColumn() == colNums[colNum]);
            ve.evaluate(batch);
          } else {

            /*
             * Evaluate next child expression.
             * But only evaluate the andSelected rows (i.e. current true or true with nulls rows).
             */
            boolean saveSelectedInUse = batch.selectedInUse;
            int[] saveSelected = sel;
            int saveSize = batch.size;
            batch.selectedInUse = true;
            batch.selected = andSelected;
            batch.size = andSel;

            VectorExpression ve = childExpressions[childExpressionIndex];
            Preconditions.checkState(ve.getOutputColumn() == colNums[colNum]);
            ve.evaluate(batch);

            batch.selectedInUse = saveSelectedInUse;
            batch.selected = saveSelected;
            batch.size = saveSize;
          }
        }
      }

      LongColumnVector nextColVector = (LongColumnVector) batch.cols[colNums[colNum]];
      long[] nextVector = nextColVector.vector;

      if (andRepeating) {

        /*
         * The andRepeating flag means the whole batch is repeating true possibly with
         * some repeating nulls.
         */
        if (nextColVector.isRepeating) {

          /*
           * Current child column is repeating so stay in repeating mode.
           */
          if (nextColVector.noNulls || !nextColVector.isNull[0]) {
            if (nextVector[0] == 0) {
              // When the entire child column is repeating false, we are done for AND.
              outV.isRepeating = true;
              outputVector[0] = 0;
              return;
            } else {
              // Current column is repeating true.
            }
          } else {
            Preconditions.checkState(nextColVector.isNull[0]);

            // At least one repeating null column.
            andRepeatingIsNull = true;
          }
          // Continue with andRepeating as true.
        } else {

          /*
           * Switch away from andRepeating/andRepeatingIsNull and now represent individual rows in
           * andSel/andSelected.
           */
          if (nextColVector.noNulls) {

            /*
             * Current child column has no nulls.
             */

            Preconditions.checkState(andSel == 0);
            andRepeating = false;

            if (andRepeatingIsNull) {

              /*
               * Since andRepeatingIsNull is true, we always set intermediateNulls when building
               * andSel/andSelected when the next row is true.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextVector[i] == 1) {
                    intermediateNulls[i] = true;
                    andSelected[andSel++] = i;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextVector[i] == 1) {
                    intermediateNulls[i] = true;
                    andSelected[andSel++] = i;
                  }
                }
              }
              andRepeatingIsNull = false;
            } else {

              /*
               * Previous rounds were all true with no null child columns.  Just build
               * andSel/andSelected when the next row is true.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextVector[i] == 1) {
                    andSelected[andSel++] = i;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextVector[i] == 1) {
                    andSelected[andSel++] = i;
                  }
                }
              }
            }
          } else {

            /*
             * Current child column can have nulls.
             */

            Preconditions.checkState(andSel == 0);
            andRepeating = false;

            if (andRepeatingIsNull) {

              /*
               * Since andRepeatingIsNull is true, we always set intermediateNulls when building
               * andSel/andSelected when the next row is null or true...
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextColVector.isNull[i] || nextVector[i] == 1) {
                    intermediateNulls[i] = true;
                    andSelected[andSel++] = i;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextColVector.isNull[i] || nextVector[i] == 1) {
                    intermediateNulls[i] = true;
                    andSelected[andSel++] = i;
                  }
                }
              }
              andRepeatingIsNull = false;
            } else {

              /*
               * Previous rounds were all true with no null child columns.  Build
               * andSel/andSelected when the next row is true; also build when next is null
               * and set intermediateNulls to true, too.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextColVector.isNull[i]) {
                    intermediateNulls[i] = true;
                    andSelected[andSel++] = i;
                  } else if (nextVector[i] == 1) {
                    andSelected[andSel++] = i;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextColVector.isNull[i]) {
                    intermediateNulls[i] = true;
                    andSelected[andSel++] = i;
                  } else if (nextVector[i] == 1) {
                    andSelected[andSel++] = i;
                  }
                }
              }
            }
          }
        }
      } else {

        /*
         * Continue in row mode: the andSel variable and andSelected member array contains the
         * rows that are some combination of true and null.
         */
        if (nextColVector.isRepeating) {

          /*
           * Current child column is repeating which affects all rows.
           */
          if (nextColVector.noNulls || !nextColVector.isNull[0]) {

            if (nextVector[0] == 0) {
              // When the entire child column is repeating false, we are done for AND.
              outV.isRepeating = true;
              outputVector[0] = 0;
              return;
            } else {
              // Child column is all true. Keep current andSel/andSelected rows.
            }
          } else {
            Preconditions.checkState(nextColVector.isNull[0]);

            // Column is repeating null -- need to mark all current rows in andSel/andSelected
            // as null.
            for (int j = 0; j < andSel; j++) {
              int i = andSelected[j];
              intermediateNulls[i] = true;
            }
          }
        } else if (nextColVector.noNulls) {

          /*
           * Current child column has no nulls.
           */

          /*
           * Rebuild andSel/andSelected to keep true rows.
           */
          int newSel = 0;
          for (int j = 0; j < andSel; j++) {
            int i = andSelected[j];
            if (nextVector[i] == 1) {
              andSelected[newSel++] = i;
            }
          }
          andSel = newSel;
        } else {

          /*
           * Current child column can have nulls.
           */

          /*
           * Rebuild andSel/andSelected to keep true rows or null rows.
           */
          int newSel = 0;
          for (int j = 0; j < andSel; j++) {
            int i = andSelected[j];
            if (nextColVector.isNull[i]) {
              // At least one null.
              intermediateNulls[i] = true;
              andSelected[newSel++] = i;
            } else if (nextVector[i] == 1) {
              andSelected[newSel++] = i;
            }
          }
          andSel = newSel;
        }
      }
    } while (++colNum < colNums.length);

    /*
     * Produce final result.
     */
    if (andRepeating) {
      outV.isRepeating = true;
      if (andRepeatingIsNull) {
        // The appearance of a null makes the repeated result null.
        outV.noNulls = false;
        outV.isNull[0] = true;
      } else {
        // All columns are repeating true.
        outputVector[0] = 1;
      }
    } else if (andSel == 0) {
      // No rows had true.
      outV.isRepeating = true;
      outputVector[0] = 0;
    } else {
      // Ok, rows were some combination of true and null throughout.
      int andIndex = 0;
      if (batch.selectedInUse) {
        /*
         * The batch selected array has all the rows we are projecting a boolean from.
         * The andSelected array has a subset of the selected rows that have at least
         * one true and may have some nulls. Now we need to decide if we are going to mark
         * those rows as true, or null because there was at least one null.
         *
         * We use the andIndex to progress through the andSelected array and make a decision
         * on how to fill out the boolean result.
         *
         * Since we reset the output column, we shouldn't have to set isNull false for true
         * entries.
         */
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (andIndex < andSel && andSelected[andIndex] == i) {
            // We haven't processed all the andSelected entries and the row index is in
            // andSelected, so make a result decision for true or null.
            if (intermediateNulls[i]) {
              outV.noNulls = false;
              outV.isNull[i] = true;
            } else {
              outputVector[i] = 1;
            }
            andIndex++;
          } else {
            // The row is not in the andSelected array.  Result is false.
            outputVector[i] = 0;
          }
        }
        Preconditions.checkState(andIndex == andSel);
      } else {
        /*
         * The andSelected array has a subset of the selected rows that have at least
         * one true and may have some nulls. Now we need to decide if we are going to mark
         * those rows as true, or null because there was at least one null.
         *
         * Prefill the result as all false.  Then decide about the andSelected entries.
         */
        Arrays.fill(outputVector, 0, n, 0);
        for (int j = 0; j < andSel; j++) {
          int i = andSelected[j];
          if (intermediateNulls[i]) {
            outV.noNulls = false;
            outV.isNull[i] = true;
          } else {
            outputVector[i] = 1;
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
