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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class IsNotNull extends VectorExpression {
	int colNum;
	int outputColumn;

	public IsNotNull(int colNum, int outputColumn) {
		this.colNum = colNum;
		this.outputColumn = outputColumn;
	}

	@Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    long[] outputVector = ((LongColumnVector) batch.cols[outputColumn]).vector;

    if (n <= 0) {
      //Nothing to do
      return;
    }

    if (inputColVector.isRepeating) {
      //All must be selected otherwise size would be zero
      //Selection property will not change.
      if (nullPos[0]) {
        outputVector[0] = 0;
      } else {
        outputVector[0] = 1;
      }
    } else if (batch.selectedInUse) {
			for(int j=0; j != n; j++) {
				int i = sel[j];
				if (nullPos[i]) {
				  outputVector[i] = 0;
				} else {
				  outputVector[i] = 1;
				}
			}
		}
		else {
			for(int i = 0; i != n; i++) {
				if (nullPos[i]) {
				  outputVector[i] = 0;
        } else {
          outputVector[i] = 1;
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
}
