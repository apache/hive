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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class ColOrCol extends VectorExpression {
	int colNum1;
	int colNum2;
	int outputColumn;

	public ColOrCol(int colNum1, int colNum2, int outputColumn)
	{
		this.colNum1 = colNum1;
		this.colNum2 = colNum2;
		this.outputColumn = outputColumn;
	}

	@Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[colNum1];
    LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[colNum2];
    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector1 = inputColVector1.vector;
    long[] vector2 = inputColVector2.vector;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    long[] outputVector = outV.vector;
    if (n <= 0) {
      //Nothing to do
      return;
    }

    if (inputColVector1.noNulls && inputColVector2.noNulls) {
      if ( (inputColVector1.isRepeating) && (inputColVector2.isRepeating) )  {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        outV.isRepeating = true;
        outputVector[0] = vector1[0] | vector2[0] ;
      } else if (inputColVector1.isRepeating && !inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[0] | vector2[i] ;
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[0] | vector2[i];
          }
        }
        outV.isRepeating = false;
      } else if (!inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[0] ;
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[0];
          }
        }
        outV.isRepeating = false;
      } else /* neither side is repeating */ {
        if (batch.selectedInUse) {
    			for(int j=0; j != n; j++) {
    				int i = sel[j];
    				outputVector[i] = vector1[i] | vector2[i] ;
    			}
    		} else {
    			for(int i = 0; i != n; i++) {
    			  outputVector[i] = vector1[i] | vector2[i];
    			}
  		  }
        outV.isRepeating = false;
      }
      outV.noNulls = true;
    } else if (inputColVector1.noNulls && !inputColVector2.noNulls) {
      // only input 2 side has nulls
      if ( (inputColVector1.isRepeating) && (inputColVector2.isRepeating) )  {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        outV.isRepeating = true;
        outputVector[0] = vector1[0] | vector2[0] ;
        outV.isNull[0] =  (vector1[0] == 0) && inputColVector2.isNull[0];
      } else if (inputColVector1.isRepeating && !inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[0] | vector2[i] ;
            outV.isNull[i] = (vector1[0] == 0) && inputColVector2.isNull[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[0] | vector2[i];
            outV.isNull[i] = (vector1[0] == 0) && inputColVector2.isNull[i];
          }
        }
        outV.isRepeating = false;
      } else if (!inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[0] ;
            outV.isNull[i] = (vector1[i] == 0) && inputColVector2.isNull[0];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[0];
            outV.isNull[i] = (vector1[i] == 0) && inputColVector2.isNull[0];
          }
        }
        outV.isRepeating = false;
      } else /* neither side is repeating */ {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[i] ;
            outV.isNull[i] = (vector1[i] == 0) && inputColVector2.isNull[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[i];
            outV.isNull[i] = (vector1[i] == 0) && inputColVector2.isNull[i];
          }
        }
        outV.isRepeating = false;
      }
      outV.noNulls = false;
    } else if (!inputColVector1.noNulls && inputColVector2.noNulls) {
      // only input 1 side has nulls
      if ( (inputColVector1.isRepeating) && (inputColVector2.isRepeating) )  {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        outV.isRepeating = true;
        outputVector[0] = vector1[0] | vector2[0] ;
        outV.isNull[0] =  inputColVector1.isNull[0] && (vector2[0] == 0);
      } else if (inputColVector1.isRepeating && !inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[0] | vector2[i] ;
            outV.isNull[i] =  inputColVector1.isNull[0] && (vector2[i] == 0);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[0] | vector2[i];
            outV.isNull[i] =  inputColVector1.isNull[0] && (vector2[i] == 0);
          }
        }
        outV.isRepeating = false;
      } else if (!inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[0] ;
            outV.isNull[i] =  inputColVector1.isNull[i] && (vector2[0] == 0);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[0];
            outV.isNull[i] =  inputColVector1.isNull[i] && (vector2[0] == 0);
          }
        }
        outV.isRepeating = false;
      } else /* neither side is repeating */ {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[i] ;
            outV.isNull[i] =  inputColVector1.isNull[i] && (vector2[i] == 0);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[i];
            outV.isNull[i] =  inputColVector1.isNull[i] && (vector2[i] == 0);
          }
        }
        outV.isRepeating = false;
      }
      outV.noNulls = false;
    } else  /* !inputColVector1.noNulls && !inputColVector2.noNulls */ {
      // either input 1 or input 2 may have nulls
      if ( (inputColVector1.isRepeating) && (inputColVector2.isRepeating) )  {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        outV.isRepeating = true;
        outputVector[0] = vector1[0] | vector2[0] ;
        outV.isNull[0] = ((vector1[0] == 0) && inputColVector2.isNull[0])
            || (inputColVector1.isNull[0] && (vector2[0] == 0))
            || (inputColVector1.isNull[0] && inputColVector2.isNull[0]);
      } else if (inputColVector1.isRepeating && !inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[0] | vector2[i];
            outV.isNull[i] = ((vector1[0] == 0) && inputColVector2.isNull[i])
                || (inputColVector1.isNull[0] && (vector2[i] == 0))
                || (inputColVector1.isNull[0] && inputColVector2.isNull[i]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[0] | vector2[i];
            outV.isNull[i] = ((vector1[0] == 0) && inputColVector2.isNull[i])
                || (inputColVector1.isNull[0] && (vector2[i] == 0))
                || (inputColVector1.isNull[0] && inputColVector2.isNull[i]);
          }
        }
        outV.isRepeating = false;
      } else if (!inputColVector1.isRepeating && inputColVector2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[0] ;
            outV.isNull[i] = ((vector1[i] == 0) && inputColVector2.isNull[0])
                || (inputColVector1.isNull[i] && (vector2[0] == 0))
                || (inputColVector1.isNull[i] && inputColVector2.isNull[0]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[0];
            outV.isNull[i] = ((vector1[i] == 0) && inputColVector2.isNull[0])
                || (inputColVector1.isNull[i] && (vector2[0] == 0))
                || (inputColVector1.isNull[i] && inputColVector2.isNull[0]);
          }
        }
        outV.isRepeating = false;
      } else /* neither side is repeating */ {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] | vector2[i] ;
            outV.isNull[i] = ((vector1[i] == 0) && inputColVector2.isNull[i])
                || (inputColVector1.isNull[i] && (vector2[i] == 0))
                || (inputColVector1.isNull[i] && inputColVector2.isNull[i]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] | vector2[i];
            outV.isNull[i] = ((vector1[i] == 0) && inputColVector2.isNull[i])
                || (inputColVector1.isNull[i] && (vector2[i] == 0))
                || (inputColVector1.isNull[i] && inputColVector2.isNull[i]);
          }
        }
        outV.isRepeating = false;
      }
      outV.noNulls = false;
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
}
