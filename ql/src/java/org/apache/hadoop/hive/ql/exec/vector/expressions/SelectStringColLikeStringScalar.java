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

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.expressions.AbstractFilterStringColLikeStringScalar.Checker;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class SelectStringColLikeStringScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;
  
  private int colNum;
  private int outputColumn;
  private byte[] pattern;
  transient Checker checker = null;

  public SelectStringColLikeStringScalar() {
    super();
  }

  public SelectStringColLikeStringScalar(int colNum, byte[] pattern, int outputColumn) {
    super();
    this.colNum = colNum;
    this.pattern = pattern;
    this.outputColumn = outputColumn;
  }

  @Override
	public void evaluate(VectorizedRowBatch batch) {
    if (checker == null) {
      checker = borrowChecker();
    }
    
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] length = inputColVector.length;
    int[] start = inputColVector.start;
    
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumn];
    long[] outputVector = outV.vector;
    
    // return immediately if batch is empty
    if (n == 0) {
      return;
    }
    
    outV.noNulls = inputColVector.noNulls;
    outV.isRepeating = inputColVector.isRepeating;
    
    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {
        outputVector[0] = (checker.check(vector[0], start[0], length[0]) ? 1 : 0);
        outV.isNull[0] = false;
      } else if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
          outV.isNull[i] = false;
        }
      } else {
        for (int i = 0; i != n; i++) {
          outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
          outV.isNull[i] = false;
        }
      }
    } else {
      if (inputColVector.isRepeating) {
        //All must be selected otherwise size would be zero. Repeating property will not change.
        if (!nullPos[0]) {
          outputVector[0] = (checker.check(vector[0], start[0], length[0]) ? 1 : 0);
          outV.isNull[0] = false;
        } else {
          outputVector[0] = LongColumnVector.NULL_VALUE;
          outV.isNull[0] = true;
        }
      } else if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
            outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
            outV.isNull[i] = false;
          } else {
            outputVector[0] = LongColumnVector.NULL_VALUE;
            outV.isNull[i] = true;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
            outV.isNull[i] = false;
          } else {
            outputVector[0] = LongColumnVector.NULL_VALUE;
            outV.isNull[i] = true;
          }
        }
      }
    }
	}
  
  private Checker borrowChecker() {
    FilterStringColLikeStringScalar fil = new FilterStringColLikeStringScalar();
    return fil.createChecker(new String(pattern, StandardCharsets.UTF_8));
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public byte[] getPattern() {
    return pattern;
  }

  public void setPattern(byte[] pattern) {
    this.pattern = pattern;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }
  
  @Override
  public int getOutputColumn() {
    return outputColumn;
  }
  
  @Override
  public String getOutputType() {
    return "String_Family";
  }

	@Override
	public Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
	}

}
