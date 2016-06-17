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
 * Evaluate OR of 2 or more boolean columns and store the boolean result in the
 * output boolean column.  This is a projection or result producing expression (as opposed to
 * a filter expression).
 *
 * Some child boolean columns may be vector expressions evaluated into boolean scratch columns.
 */
public class ColOrCol extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int[] colNums;
  private int outputColumn;
  private int[] mapToChildExpression;
  private int[] orSelected;
  private boolean[] intermediateNulls;

  public ColOrCol(int[] colNums, int outputColumn) {
    this();
    this.colNums = colNums;
    this.outputColumn = outputColumn;
    mapToChildExpression = null;
    orSelected = new int[VectorizedRowBatch.DEFAULT_SIZE];
    intermediateNulls = new boolean[VectorizedRowBatch.DEFAULT_SIZE];
  }

  public ColOrCol() {
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
       * based on whether there is still a need for OR processing.
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
     * Null processing complicates the algorithm here for Multi-OR.
     *
     * Any true --> true
     * 0 or more false with 1 or more null --> result = null
     * All false --> false
     *
     * For OR processing, we set the outputVector row to true rows as we find them.
     * Once set for OR, the row result doesn't change.
     *
     * We remember nulls in the intermediateNulls so we can later we can mark the row as null
     * instead of false if there was a null.
     */

    /*
     * orRepeating will be true when all the children column vectors processed so far are
     * some combination of repeating false and repeating null.
     * orRepeatingIsNull will be true when there is at least one repeating null column.
     */
    boolean orRepeating = false;
    boolean orRepeatingHasNulls = false;

    /*
     * The orSel variable and orSelected member array represent rows that have at have
     * some combination of false and nulls.
     */
    int orSel = 0;

    Arrays.fill(intermediateNulls, 0, VectorizedRowBatch.DEFAULT_SIZE, false);

    // Reset noNulls to true, isNull to false, isRepeating to false.
    outV.reset();

    // Initially, set all rows to false.
    if (batch.selectedInUse) {
      for (int j = 0; j != n; j++) {
        int i = sel[j];
        outputVector[i] = 0;
      }
    } else {
      Arrays.fill(outputVector, 0, n, 0);
    }

    LongColumnVector firstColVector = (LongColumnVector) batch.cols[colNums[0]];
    long[] firstVector = firstColVector.vector;

    /*
     * We prime the pump by evaluating the first child to see if we are starting with
     * orRepeating/orRepeatingHasNulls or we are starting with orSel/orSelected processing.
     */
    if (firstColVector.isRepeating) {
      if (firstColVector.noNulls || !firstColVector.isNull[0]) {
        if (firstVector[0] == 1) {
          // When the entire child column is repeating true, we are done for OR.
          outV.isRepeating = true;
          outputVector[0] = 1;
          return;
        } else {
          // First column is repeating false.
        }
      } else {
        Preconditions.checkState(firstColVector.isNull[0]);

        // At least one repeating null column.
        orRepeatingHasNulls = true;
      }
      orRepeating = true;
    } else if (firstColVector.noNulls) {

      /*
       * No nulls -- so all false rows go in orSel/orSelected.  Otherwise, when the row is true,
       * mark the output row as true.
       */
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (firstVector[i] == 0) {
            orSelected[orSel++] = i;
          } else {
            outputVector[i] = 1;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (firstVector[i] == 0) {
            orSelected[orSel++] = i;
          } else {
            outputVector[i] = 1;
          }
        }
      }
    } else {

      /*
       * Can be nulls -- so all false rows and null rows go in orSel/orSelected.
       * Remember nulls in our separate intermediateNulls array.  Otherwise, when the row is true,
       * mark the output row as true.
       */
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (firstColVector.isNull[i]) {
            intermediateNulls[i] = true;
            orSelected[orSel++] = i;
          } else if (firstVector[i] == 0) {
            orSelected[orSel++] = i;
          } else {
            outputVector[i] = 1;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (firstColVector.isNull[i]) {
            intermediateNulls[i] = true;
            orSelected[orSel++] = i;
          } else if (firstVector[i] == 0) {
            orSelected[orSel++] = i;
          } else {
            outputVector[i] = 1;
          }
        }
      }
    }

    /*
     * Process child #2 and above.
     */
    int colNum = 1;
    do {
      if (!orRepeating && orSel == 0) {

        /*
         * Since orSelected and orSel represent currently false entries and there are none,
         * then everything is true (how philosophical!).
         */
        break;
      }

      if (childExpressions != null) {
        int childExpressionIndex = mapToChildExpression[colNum];
        if (childExpressionIndex != -1) {
          if (orRepeating) {
            /*
             * We need to start with a full evaluate on all [selected] rows.
             */
            VectorExpression ve = childExpressions[childExpressionIndex];
            Preconditions.checkState(ve.getOutputColumn() == colNums[colNum]);
            ve.evaluate(batch);
          } else {
            /*
             * Evaluate next child expression.
             * But only on the orSelected rows (i.e. current false or false with nulls rows).
             */
            boolean saveSelectedInUse = batch.selectedInUse;
            int[] saveSelected = sel;
            int saveSize = batch.size;
            batch.selectedInUse = true;
            batch.selected = orSelected;
            batch.size = orSel;

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

      if (orRepeating) {

        /*
         * The orRepeating flag means the whole batch has some combination of repeating false
         * columns and repeating null columns.
         */
        if (nextColVector.isRepeating) {

          /*
           * Current child column is repeating so stay in repeating mode.
           */
          if (nextColVector.noNulls || !nextColVector.isNull[0]) {
            if (nextVector[0] == 1) {
              outV.isRepeating = true;
              outputVector[0] = 1;
              return;
            } else {
              // The orRepeatingHasNulls flag goes on to the next stage, too.
            }
          } else {
            Preconditions.checkState(nextColVector.isNull[0]);

            // At least one repeating null column.
            orRepeatingHasNulls = true;
          }
          // Continue with orRepeating as true.
        } else {

          /*
           * Switch away from orRepeating/orRepeatingHasNulls and now represent individual rows in
           * orSel/orSelected.
           */
          if (nextColVector.noNulls) {

            /*
             * Current child column has no nulls.
             */

            Preconditions.checkState(orSel == 0);
            orRepeating = false;

            if (orRepeatingHasNulls) {

              /*
               * Since orRepeatingIsNull is true, we always set intermediateNulls when building
               * orSel/orSelected when the next row is false.  Otherwise, when the row is true, mark
               * the output row as true.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextVector[i] == 0) {
                    intermediateNulls[i] = true;
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextVector[i] == 0) {
                    intermediateNulls[i] = true;
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              }
              orRepeatingHasNulls = false;
            } else {

              /*
               * Previous rounds were all false with no null child columns.  Build
               * orSel/orSelected when the next row is false.  Otherwise, when the row is true, mark
               * the output row as true.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextVector[i] == 0) {
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextVector[i] == 0) {
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              }
            }
          } else {

            /*
             * Current child column can have nulls.
             */

            Preconditions.checkState(orSel == 0);
            orRepeating = false;

            if (orRepeatingHasNulls) {

              /*
               * Since orRepeatingIsNull is true, we always set intermediateNulls when building
               * orSel/orSelected when the next row is null or false.  Otherwise, when the row
               * is true mark the output row as true.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextColVector.isNull[i] || nextVector[i] == 0) {
                    intermediateNulls[i] = true;
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextColVector.isNull[i] || nextVector[i] == 0) {
                    intermediateNulls[i] = true;
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              }
              orRepeatingHasNulls = false;
            } else {

              /*
               * Previous rounds were all true with no null child columns.  Build
               * andSel/andSelected when the next row is true; also build when next is null
               * and set intermediateNulls to true, too.  Otherwise, when the row
               * is true mark the output row as true.
               */
              if (batch.selectedInUse) {
                for (int j = 0; j != n; j++) {
                  int i = sel[j];
                  if (nextColVector.isNull[i]) {
                    intermediateNulls[i] = true;
                    orSelected[orSel++] = i;
                  } else if (nextVector[i] == 0) {
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              } else {
                for (int i = 0; i != n; i++) {
                  if (nextColVector.isNull[i]) {
                    intermediateNulls[i] = true;
                    orSelected[orSel++] = i;
                  } else if (nextVector[i] == 0) {
                    orSelected[orSel++] = i;
                  } else {
                    outputVector[i] = 1;
                  }
                }
              }
            }
          }
        }
      } else {

        /*
         * Continue in row mode: the orSel variable and orSelected member array contains the
         * rows that are a combination of false and null.
         */
        if (nextColVector.isRepeating) {

          if (nextColVector.noNulls || !nextColVector.isNull[0]) {

            if (nextVector[0] == 1) {
              // When the entire child column is repeating true, we are done for OR.
              outV.isRepeating = true;
              outputVector[0] = 1;
              return;
            } else {
              // Child column is all false. Keep all orSel/orSelected rows.
            }
          } else {
            Preconditions.checkState(nextColVector.isNull[0]);

            // Column is repeating null -- need to mark all current rows in orSel/orSelected
            // as null.
            for (int j = 0; j < orSel; j++) {
              int i = orSelected[j];
              intermediateNulls[i] = true;
            }
          }
        } else if (nextColVector.noNulls) {

          /*
           * Current child column has no nulls.
           */

          /*
           * Rebuild orSel/orSelected to keep false rows.  True rows get set in output vector.
           */
          int newSel = 0;
          for (int j = 0; j < orSel; j++) {
            int i = orSelected[j];
            if (nextVector[i] == 0) {
              orSelected[newSel++] = i;
            } else {
              outputVector[i] = 1;
            }
          }
          orSel = newSel;
        } else {

          /*
           * Current child column can have nulls.
           */

          /*
           * Rebuild orSel/orSelected to keep false rows or null rows.  True rows get set in
           * output vector.
           */
          int newSel = 0;
          for (int j = 0; j < orSel; j++) {
            int i = orSelected[j];
            if (nextColVector.isNull[i]) {
              // Mark row has at least one null.
              intermediateNulls[i] = true;
              orSelected[newSel++] = i;
            } else if (nextVector[i] == 0) {
              orSelected[newSel++] = i;
            } else {
              outputVector[i] = 1;
            }
          }
          orSel = newSel;
        }
      }
    } while (++colNum < colNums.length);

    /*
     *  Produce final result.
     */
    if (orRepeating) {

      /*
       * The orRepeating flags means the whole batch is false and may have nulls.
       */
      outV.isRepeating = true;
      if (orRepeatingHasNulls) {
        outV.noNulls = false;
        outV.isNull[0] = true;
      } else {
        outputVector[0] = 0;
      }
    } else {

      /*
       * When were any intermediate nulls for a row, the result row will be null.
       * Note the true entries were previously set in outputVector as we went along.
       */
      for (int j = 0; j < orSel; j++) {
        int i = orSelected[j];
        Preconditions.checkState(outputVector[i] == 0);
        if (intermediateNulls[i]) {
          outV.noNulls = false;
          outV.isNull[i] = true;
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
