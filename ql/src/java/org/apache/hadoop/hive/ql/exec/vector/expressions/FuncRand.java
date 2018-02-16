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
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Implements vectorized rand(seed) function evaluation.
 */
public class FuncRand extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final Random random;

  public FuncRand(long seed, int outputColumnNum) {
    super(outputColumnNum);
    this.random = new Random(seed);
  }

  public FuncRand() {
    super();

    // Dummy final assignments.
    random = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    int n = batch.size;
    double[] outputVector = outputColVector.vector;
    outputColVector.isRepeating = false;
    boolean[] outputIsNull = outputColVector.isNull;

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (batch.selectedInUse) {

      // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

      if (!outputColVector.noNulls) {
        for(int j = 0; j != n; j++) {
         final int i = sel[j];
         // Set isNull before call in case it changes it mind.
         outputIsNull[i] = false;
         outputVector[i] = random.nextDouble();
       }
      } else {
        for(int j = 0; j != n; j++) {
          final int i = sel[j];
          outputVector[i] = random.nextDouble();
        }
      }
    } else {
      if (!outputColVector.noNulls) {

        // Assume it is almost always a performance win to fill all of isNull so we can
        // safely reset noNulls.
        Arrays.fill(outputIsNull, false);
        outputColVector.noNulls = true;
      }
      for(int i = 0; i != n; i++) {
        outputVector[i] = random.nextDouble();
      }
    }
 }

  @Override
  public String vectorExpressionParameters() {
    // No input parameters.
    return null;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
