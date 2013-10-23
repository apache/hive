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

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Implements vectorized rand(seed) function evaluation.
 */
public class FuncRand extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int outputCol;
  private Random random;

  public FuncRand(long seed, int outputCol) {
    this.outputCol = outputCol;
    this.random = new Random(seed);
  }

  public FuncRand() {
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    DoubleColumnVector outputColVector = (DoubleColumnVector) batch.cols[outputCol];
    int[] sel = batch.selected;
    int n = batch.size;
    double[] outputVector = outputColVector.vector;
    outputColVector.noNulls = true;
    outputColVector.isRepeating = false;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // For no-seed case, create new random number generator locally.
    if (random == null) {
      random = new Random();
    }

    if (batch.selectedInUse) {
      for(int j = 0; j != n; j++) {
        int i = sel[j];
        outputVector[i] = random.nextDouble();
      }
    } else {
      for(int i = 0; i != n; i++) {
        outputVector[i] = random.nextDouble();
      }
    }
 }

  @Override
  public int getOutputColumn() {
    return outputCol;
  }

  public int getOutputCol() {
    return outputCol;
  }

  public void setOutputCol(int outputCol) {
    this.outputCol = outputCol;
  }

  public Random getRandom() {
    return random;
  }

  public void setRandom(Random random) {
    this.random = random;
  }

  @Override
  public String getOutputType() {
    return "double";
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
