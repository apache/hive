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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized instruction to get an element from a list with the index from another column and put
 * the result in an output column.
 */
public class ListIndexColColumn extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public ListIndexColColumn() {
    super();
  }

  public ListIndexColColumn(int listColumnNum, int indexColumnNum, int outputColumnNum) {
    super(listColumnNum, indexColumnNum, outputColumnNum);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    // return immediately if batch is empty
    final int n = batch.size;
    if (n == 0) {
      return;
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector outV = batch.cols[outputColumnNum];
    ListColumnVector listV = (ListColumnVector) batch.cols[inputColumnNum[0]];
    ColumnVector childV = listV.child;
    LongColumnVector indexColumnVector = (LongColumnVector) batch.cols[inputColumnNum[1]];
    long[] indexV = indexColumnVector.vector;
    int[] sel = batch.selected;
    boolean[] indexIsNull = indexColumnVector.isNull;
    boolean[] listIsNull = listV.isNull;
    boolean[] outputIsNull = outV.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    /*
     * List indices are 0-based.
     *
     * Do careful maintenance of the outputColVector.noNulls flag since the index may be
     * out-of-bounds.
     */

    if (indexColumnVector.isRepeating) {

      /*
       * Repeated index or repeated NULL index.
       */
      if (indexColumnVector.noNulls || !indexIsNull[0]) {
        final long repeatedLongIndex = indexV[0];
        if (repeatedLongIndex < 0) {

          // Invalid index for entire batch.
          outputIsNull[0] = true;
          outV.noNulls = false;
          outV.isRepeating = true;
          return;
        }

        /*
         * Same INDEX for entire batch. Still need to validate the LIST upper limit.
         */
        if (listV.isRepeating) {
          if (listV.noNulls || !listIsNull[0]) {
            final long repeatedLongListLength = listV.lengths[0];
            if (repeatedLongIndex >= repeatedLongListLength) {
              outV.isNull[0] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[0] = false;
              outV.setElement(0, (int) (listV.offsets[0] + repeatedLongIndex), childV);
            }
          } else {
            outputIsNull[0] = true;
            outV.noNulls = false;
          }
          outV.isRepeating = true;
          return;
        }

        /*
         * Individual row processing for LIST vector with *repeated* INDEX instance.
         */
        if (listV.noNulls) {
          if (batch.selectedInUse) {

            // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

            if (!outV.noNulls) {
              for (int j = 0; j < n; j++) {
                final int i = sel[j];
                final long longListLength = listV.lengths[i];
                if (repeatedLongIndex >= longListLength) {
                  outV.isNull[i] = true;
                  outV.noNulls = false;
                } else {
                  outV.isNull[i] = false;
                  outV.setElement(i, (int) (listV.offsets[i] + repeatedLongIndex), childV);
                }
              }
            } else {
              for (int j = 0; j < n; j++) {
                final int i = sel[j];
                final long longListLength = listV.lengths[i];
                if (repeatedLongIndex >= longListLength) {
                  outV.isNull[i] = true;
                  outV.noNulls = false;
                } else {
                  outV.setElement(i, (int) (listV.offsets[i] + repeatedLongIndex), childV);
                }
              }
            }
          } else {
            if (!outV.noNulls) {

              // Assume it is almost always a performance win to fill all of isNull so we can
              // safely reset noNulls.
              Arrays.fill(outputIsNull, false);
              outV.noNulls = true;
            }
            for (int i = 0; i < n; i++) {
              final long longListLength = listV.lengths[i];
              if (repeatedLongIndex >= longListLength) {
                outV.isNull[i] = true;
                outV.noNulls = false;
              } else {
                outV.setElement(i, (int) (listV.offsets[i] + repeatedLongIndex), childV);
              }
            }
          }
        } else /* there are NULLs in the LIST */ {

          if (batch.selectedInUse) {
            for (int j=0; j != n; j++) {
              int i = sel[j];
              if (!listIsNull[i]) {
                final long longListLength = listV.lengths[i];
                if (repeatedLongIndex >= longListLength) {
                  outV.isNull[i] = true;
                  outV.noNulls = false;
                } else {
                  outV.isNull[i] = false;
                  outV.setElement(i, (int) (listV.offsets[i] + repeatedLongIndex), childV);
                }
              } else {
                outputIsNull[i] = true;
                outV.noNulls = false;
              }
            }
          } else {
            for (int i = 0; i != n; i++) {
              if (!listIsNull[i]) {
                final long longListLength = listV.lengths[i];
                if (repeatedLongIndex >= longListLength) {
                  outV.isNull[i] = true;
                  outV.noNulls = false;
                } else {
                  outV.isNull[i] = false;
                  outV.setElement(i, (int) (listV.offsets[i] + repeatedLongIndex), childV);
                }
              } else {
                outputIsNull[i] = true;
                outV.noNulls = false;
              }
            }
          }
        }
      } else {
        outputIsNull[0] = true;
        outV.noNulls = false;
        outV.isRepeating = true;
      }
      return;
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    /*
     * Same LIST for entire batch. Still need to validate the LIST upper limit against varying
     * INDEX.
     *
     * (Repeated INDEX case handled above).
     */

    if (listV.isRepeating) {
      if (listV.noNulls || !listIsNull[0]) {

        /*
         * Individual row processing for INDEX vector with *repeated* LIST value.
         */
        final long repeatedLongListOffset = listV.offsets[0];
        final long repeatedLongListLength = listV.lengths[0];

        if (indexColumnVector.noNulls) {
          if (batch.selectedInUse) {

             // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

             if (!outV.noNulls) {
               for (int j = 0; j != n; j++) {
                 final int i = sel[j];
                 final long longIndex = indexV[i];
                 if (longIndex < 0) {

                   // Invalid index for entire batch.
                   outputIsNull[i] = true;
                   outV.noNulls = false;
                 } else {
                   if (longIndex >= repeatedLongListLength) {
                      outV.isNull[i] = true;
                      outV.noNulls = false;
                   } else {
                     outV.isNull[i] = false;
                     outV.setElement(i, (int) (repeatedLongListOffset + longIndex), childV);
                   }
                 }
               }
             } else {
               for (int j = 0; j != n; j++) {
                 final int i = sel[j];
                 final long longIndex = indexV[i];
                 if (longIndex < 0) {

                   // Invalid index for entire batch.
                   outputIsNull[i] = true;
                   outV.noNulls = false;
                 } else {
                   if (longIndex >= repeatedLongListLength) {
                      outV.isNull[i] = true;
                      outV.noNulls = false;
                   } else {
                     outV.setElement(i, (int) (repeatedLongListOffset + longIndex), childV);
                   }
                 }
               }
             }
          } else {
            if (!outV.noNulls) {

              // Assume it is almost always a performance win to fill all of isNull so we can
              // safely reset noNulls.
              Arrays.fill(outputIsNull, false);
              outV.noNulls = true;
            }
            for (int i = 0; i != n; i++) {
              final long longIndex = indexV[i];
              if (longIndex < 0) {

                // Invalid index for entire batch.
                outputIsNull[i] = true;
                outV.noNulls = false;
              } else {
                if (longIndex >= repeatedLongListLength) {
                   outV.isNull[i] = true;
                   outV.noNulls = false;
                } else {
                  outV.setElement(i, (int) (repeatedLongListOffset + longIndex), childV);
                }
              }
            }
          }
        } else /* there are NULLs in the inputColVector */ {

          /*
           * Do careful maintenance of the outV.noNulls flag.
           */

          if (batch.selectedInUse) {
            for(int j=0; j != n; j++) {
              int i = sel[j];
              if (!indexIsNull[i]) {
                final long longIndex = indexV[i];
                if (longIndex < 0) {

                  // Invalid index for entire batch.
                  outputIsNull[i] = true;
                  outV.noNulls = false;
                } else {
                  if (longIndex >= repeatedLongListLength) {
                     outV.isNull[i] = true;
                     outV.noNulls = false;
                  } else {
                    outV.isNull[i] = false;
                    outV.setElement(i, (int) (repeatedLongListOffset + longIndex), childV);
                  }
                }
              } else {
                outputIsNull[i] = true;
                outV.noNulls = false;
              }
            }
          } else {
            for(int i = 0; i != n; i++) {
              if (!indexIsNull[i]) {
                final long longIndex = indexV[i];
                if (longIndex < 0) {

                  // Invalid index for entire batch.
                  outputIsNull[i] = true;
                  outV.noNulls = false;
                } else {
                  if (longIndex >= repeatedLongListLength) {
                     outV.isNull[i] = true;
                     outV.noNulls = false;
                  } else {
                    outV.isNull[i] = false;
                    outV.setElement(i, (int) (repeatedLongListOffset + longIndex), childV);
                  }
                }
              } else {
                outputIsNull[i] = true;
                outV.noNulls = false;
              }
            }
          }
        }
      } else {
        outputIsNull[0] = true;
        outV.noNulls = false;
        outV.isRepeating = true;
      }
      return;
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    /*
     * Individual row processing for INDEX vectors and LIST vectors.
     */
    final boolean listNoNulls = listV.noNulls;

    if (indexColumnVector.noNulls) {
      if (batch.selectedInUse) {

         // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

         if (!outV.noNulls) {
           for (int j = 0; j != n; j++) {
             final int i = sel[j];
             final long longIndex = indexV[i];
             if (longIndex < 0) {

               // Invalid index for entire batch.
               outputIsNull[i] = true;
               outV.noNulls = false;
             } else {
               if (listNoNulls || !listIsNull[i]) {
                 final long longListLength = listV.lengths[i];
                 if (longIndex >= longListLength) {
                    outV.isNull[i] = true;
                    outV.noNulls = false;
                 } else {
                   outV.isNull[i] = false;
                   outV.setElement(i, (int) (listV.offsets[i] + longIndex), childV);
                 }
               } else {
                 outputIsNull[i] = true;
                 outV.noNulls = false;
               }
             }
           }
         } else {
           for (int j = 0; j != n; j++) {
             final int i = sel[j];
             final long longIndex = indexV[i];
             if (longIndex < 0) {

               // Invalid index for entire batch.
               outputIsNull[i] = true;
               outV.noNulls = false;
             } else {
               if (listNoNulls || !listIsNull[i]) {
                 final long longListLength = listV.lengths[i];
                 if (longIndex >= longListLength) {
                    outV.isNull[i] = true;
                    outV.noNulls = false;
                 } else {
                   outV.setElement(i, (int) (listV.offsets[i] + longIndex), childV);
                 }
               } else {
                 outputIsNull[i] = true;
                 outV.noNulls = false;
               }
             }
           }
         }
      } else {
        if (!outV.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outV.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          final long longIndex = indexV[i];
          if (longIndex < 0) {

            // Invalid index for entire batch.
            outputIsNull[i] = true;
            outV.noNulls = false;
          } else {
            if (listNoNulls || !listIsNull[i]) {
              final long longListLength = listV.lengths[i];
              if (longIndex >= longListLength) {
                 outV.isNull[i] = true;
                 outV.noNulls = false;
              } else {
                outV.setElement(i, (int) (listV.offsets[i] + longIndex), childV);
              }
            } else {
              outputIsNull[i] = true;
              outV.noNulls = false;
            }
          }
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      /*
       * Do careful maintenance of the outV.noNulls flag.
       */

      if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!indexIsNull[i]) {
            final long longIndex = indexV[i];
            if (longIndex < 0) {

              // Invalid index for entire batch.
              outputIsNull[i] = true;
              outV.noNulls = false;
            } else {
              if (listNoNulls || !listIsNull[i]) {
                final long longListLength = listV.lengths[i];
                if (longIndex >= longListLength) {
                   outV.isNull[i] = true;
                   outV.noNulls = false;
                } else {
                  outV.isNull[i] = false;
                  outV.setElement(i, (int) (listV.offsets[i] + longIndex), childV);
                }
              } else {
                outputIsNull[i] = true;
                outV.noNulls = false;
              }
            }
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!indexIsNull[i]) {
            final long longIndex = indexV[i];
            if (longIndex < 0) {

              // Invalid index for entire batch.
              outputIsNull[i] = true;
              outV.noNulls = false;
            } else {
              if (listNoNulls || !listIsNull[i]) {
                final long longListLength = listV.lengths[i];
                if (longIndex >= longListLength) {
                   outV.isNull[i] = true;
                   outV.noNulls = false;
                } else {
                  outV.isNull[i] = false;
                  outV.setElement(i, (int) (listV.offsets[i] + longIndex), childV);
                }
              } else {
                outputIsNull[i] = true;
                outV.noNulls = false;
              }
            }
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LIST,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
