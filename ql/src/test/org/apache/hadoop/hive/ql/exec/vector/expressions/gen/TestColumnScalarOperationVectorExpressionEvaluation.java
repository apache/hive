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

package org.apache.hadoop.hive.ql.exec.vector.expressions.gen;

import static org.junit.Assert.assertEquals;
import java.util.Random;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.junit.Test;


/**
 *
 * TestColumnScalarOperationVectorExpressionEvaluation.
 *
 */
public class TestColumnScalarOperationVectorExpressionEvaluation{

  private static final int BATCH_SIZE = 100;
  private static final long SEED = 0xfa57;

  
  @Test
  public void testLongColAddLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColAddLongScalar vectorExpression =
      new LongColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColAddLongScalar vectorExpression =
      new LongColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColAddLongScalar vectorExpression =
      new LongColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColAddLongScalar vectorExpression =
      new LongColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColAddLongScalar vectorExpression =
      new LongColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColSubtractLongScalar vectorExpression =
      new LongColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColSubtractLongScalar vectorExpression =
      new LongColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColSubtractLongScalar vectorExpression =
      new LongColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColSubtractLongScalar vectorExpression =
      new LongColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColSubtractLongScalar vectorExpression =
      new LongColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColMultiplyLongScalar vectorExpression =
      new LongColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColMultiplyLongScalar vectorExpression =
      new LongColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColMultiplyLongScalar vectorExpression =
      new LongColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColMultiplyLongScalar vectorExpression =
      new LongColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColMultiplyLongScalar vectorExpression =
      new LongColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColModuloLongScalar vectorExpression =
      new LongColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColModuloLongScalar vectorExpression =
      new LongColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColModuloLongScalar vectorExpression =
      new LongColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColModuloLongScalar vectorExpression =
      new LongColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongColModuloLongScalar vectorExpression =
      new LongColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColAddDoubleScalar vectorExpression =
      new LongColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColAddDoubleScalar vectorExpression =
      new LongColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColAddDoubleScalar vectorExpression =
      new LongColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColAddDoubleScalar vectorExpression =
      new LongColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColAddDoubleScalar vectorExpression =
      new LongColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColSubtractDoubleScalar vectorExpression =
      new LongColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColSubtractDoubleScalar vectorExpression =
      new LongColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColSubtractDoubleScalar vectorExpression =
      new LongColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColSubtractDoubleScalar vectorExpression =
      new LongColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColSubtractDoubleScalar vectorExpression =
      new LongColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColMultiplyDoubleScalar vectorExpression =
      new LongColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColMultiplyDoubleScalar vectorExpression =
      new LongColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColMultiplyDoubleScalar vectorExpression =
      new LongColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColMultiplyDoubleScalar vectorExpression =
      new LongColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColMultiplyDoubleScalar vectorExpression =
      new LongColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColDivideDoubleScalar vectorExpression =
      new LongColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColDivideDoubleScalar vectorExpression =
      new LongColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColDivideDoubleScalar vectorExpression =
      new LongColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColDivideDoubleScalar vectorExpression =
      new LongColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColDivideDoubleScalar vectorExpression =
      new LongColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColModuloDoubleScalar vectorExpression =
      new LongColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColModuloDoubleScalar vectorExpression =
      new LongColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColModuloDoubleScalar vectorExpression =
      new LongColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColModuloDoubleScalar vectorExpression =
      new LongColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColModuloDoubleScalar vectorExpression =
      new LongColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColAddLongScalar vectorExpression =
      new DoubleColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColAddLongScalar vectorExpression =
      new DoubleColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColAddLongScalar vectorExpression =
      new DoubleColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColAddLongScalar vectorExpression =
      new DoubleColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColAddLongScalar vectorExpression =
      new DoubleColAddLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColSubtractLongScalar vectorExpression =
      new DoubleColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColSubtractLongScalar vectorExpression =
      new DoubleColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColSubtractLongScalar vectorExpression =
      new DoubleColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColSubtractLongScalar vectorExpression =
      new DoubleColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColSubtractLongScalar vectorExpression =
      new DoubleColSubtractLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColMultiplyLongScalar vectorExpression =
      new DoubleColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColMultiplyLongScalar vectorExpression =
      new DoubleColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColMultiplyLongScalar vectorExpression =
      new DoubleColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColMultiplyLongScalar vectorExpression =
      new DoubleColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColMultiplyLongScalar vectorExpression =
      new DoubleColMultiplyLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColDivideLongScalar vectorExpression =
      new DoubleColDivideLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColDivideLongScalar vectorExpression =
      new DoubleColDivideLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColDivideLongScalar vectorExpression =
      new DoubleColDivideLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColDivideLongScalar vectorExpression =
      new DoubleColDivideLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColDivideLongScalar vectorExpression =
      new DoubleColDivideLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColModuloLongScalar vectorExpression =
      new DoubleColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColModuloLongScalar vectorExpression =
      new DoubleColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColModuloLongScalar vectorExpression =
      new DoubleColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColModuloLongScalar vectorExpression =
      new DoubleColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    DoubleColModuloLongScalar vectorExpression =
      new DoubleColModuloLongScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColAddDoubleScalar vectorExpression =
      new DoubleColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColAddDoubleScalar vectorExpression =
      new DoubleColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColAddDoubleScalar vectorExpression =
      new DoubleColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColAddDoubleScalar vectorExpression =
      new DoubleColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColAddDoubleScalar vectorExpression =
      new DoubleColAddDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColSubtractDoubleScalar vectorExpression =
      new DoubleColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColSubtractDoubleScalar vectorExpression =
      new DoubleColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColSubtractDoubleScalar vectorExpression =
      new DoubleColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColSubtractDoubleScalar vectorExpression =
      new DoubleColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColSubtractDoubleScalar vectorExpression =
      new DoubleColSubtractDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColMultiplyDoubleScalar vectorExpression =
      new DoubleColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColMultiplyDoubleScalar vectorExpression =
      new DoubleColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColMultiplyDoubleScalar vectorExpression =
      new DoubleColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColMultiplyDoubleScalar vectorExpression =
      new DoubleColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColMultiplyDoubleScalar vectorExpression =
      new DoubleColMultiplyDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColDivideDoubleScalar vectorExpression =
      new DoubleColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColDivideDoubleScalar vectorExpression =
      new DoubleColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColDivideDoubleScalar vectorExpression =
      new DoubleColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColDivideDoubleScalar vectorExpression =
      new DoubleColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColDivideDoubleScalar vectorExpression =
      new DoubleColDivideDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColModuloDoubleScalar vectorExpression =
      new DoubleColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColModuloDoubleScalar vectorExpression =
      new DoubleColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColModuloDoubleScalar vectorExpression =
      new DoubleColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColModuloDoubleScalar vectorExpression =
      new DoubleColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColModuloDoubleScalar vectorExpression =
      new DoubleColModuloDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddLongColumn vectorExpression =
      new LongScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddLongColumn vectorExpression =
      new LongScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddLongColumn vectorExpression =
      new LongScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddLongColumn vectorExpression =
      new LongScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddLongColumn vectorExpression =
      new LongScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractLongColumn vectorExpression =
      new LongScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractLongColumn vectorExpression =
      new LongScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractLongColumn vectorExpression =
      new LongScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractLongColumn vectorExpression =
      new LongScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractLongColumn vectorExpression =
      new LongScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyLongColumn vectorExpression =
      new LongScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyLongColumn vectorExpression =
      new LongScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyLongColumn vectorExpression =
      new LongScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyLongColumn vectorExpression =
      new LongScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyLongColumn vectorExpression =
      new LongScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloLongColumn vectorExpression =
      new LongScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloLongColumn vectorExpression =
      new LongScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloLongColumn vectorExpression =
      new LongScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloLongColumn vectorExpression =
      new LongScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloLongColumn vectorExpression =
      new LongScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddDoubleColumn vectorExpression =
      new LongScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddDoubleColumn vectorExpression =
      new LongScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddDoubleColumn vectorExpression =
      new LongScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddDoubleColumn vectorExpression =
      new LongScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarAddDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarAddDoubleColumn vectorExpression =
      new LongScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractDoubleColumn vectorExpression =
      new LongScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractDoubleColumn vectorExpression =
      new LongScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractDoubleColumn vectorExpression =
      new LongScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractDoubleColumn vectorExpression =
      new LongScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarSubtractDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarSubtractDoubleColumn vectorExpression =
      new LongScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyDoubleColumn vectorExpression =
      new LongScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyDoubleColumn vectorExpression =
      new LongScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyDoubleColumn vectorExpression =
      new LongScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyDoubleColumn vectorExpression =
      new LongScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarMultiplyDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarMultiplyDoubleColumn vectorExpression =
      new LongScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarDivideDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarDivideDoubleColumn vectorExpression =
      new LongScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarDivideDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarDivideDoubleColumn vectorExpression =
      new LongScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarDivideDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarDivideDoubleColumn vectorExpression =
      new LongScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarDivideDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarDivideDoubleColumn vectorExpression =
      new LongScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarDivideDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarDivideDoubleColumn vectorExpression =
      new LongScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloDoubleColumn vectorExpression =
      new LongScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloDoubleColumn vectorExpression =
      new LongScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloDoubleColumn vectorExpression =
      new LongScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloDoubleColumn vectorExpression =
      new LongScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongScalarModuloDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    LongScalarModuloDoubleColumn vectorExpression =
      new LongScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddLongColumn vectorExpression =
      new DoubleScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddLongColumn vectorExpression =
      new DoubleScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddLongColumn vectorExpression =
      new DoubleScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddLongColumn vectorExpression =
      new DoubleScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddLongColumn vectorExpression =
      new DoubleScalarAddLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractLongColumn vectorExpression =
      new DoubleScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractLongColumn vectorExpression =
      new DoubleScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractLongColumn vectorExpression =
      new DoubleScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractLongColumn vectorExpression =
      new DoubleScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractLongColumn vectorExpression =
      new DoubleScalarSubtractLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyLongColumn vectorExpression =
      new DoubleScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyLongColumn vectorExpression =
      new DoubleScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyLongColumn vectorExpression =
      new DoubleScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyLongColumn vectorExpression =
      new DoubleScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyLongColumn vectorExpression =
      new DoubleScalarMultiplyLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideLongColumn vectorExpression =
      new DoubleScalarDivideLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideLongColumn vectorExpression =
      new DoubleScalarDivideLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideLongColumn vectorExpression =
      new DoubleScalarDivideLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideLongColumn vectorExpression =
      new DoubleScalarDivideLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideLongColumn vectorExpression =
      new DoubleScalarDivideLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloLongColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloLongColumn vectorExpression =
      new DoubleScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloLongColumn vectorExpression =
      new DoubleScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloLongColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloLongColumn vectorExpression =
      new DoubleScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloLongColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloLongColumn vectorExpression =
      new DoubleScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloLongColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloLongColumn vectorExpression =
      new DoubleScalarModuloLongColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddDoubleColumn vectorExpression =
      new DoubleScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddDoubleColumn vectorExpression =
      new DoubleScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddDoubleColumn vectorExpression =
      new DoubleScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddDoubleColumn vectorExpression =
      new DoubleScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarAddDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarAddDoubleColumn vectorExpression =
      new DoubleScalarAddDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractDoubleColumn vectorExpression =
      new DoubleScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractDoubleColumn vectorExpression =
      new DoubleScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractDoubleColumn vectorExpression =
      new DoubleScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractDoubleColumn vectorExpression =
      new DoubleScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarSubtractDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarSubtractDoubleColumn vectorExpression =
      new DoubleScalarSubtractDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyDoubleColumn vectorExpression =
      new DoubleScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyDoubleColumn vectorExpression =
      new DoubleScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyDoubleColumn vectorExpression =
      new DoubleScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyDoubleColumn vectorExpression =
      new DoubleScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarMultiplyDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarMultiplyDoubleColumn vectorExpression =
      new DoubleScalarMultiplyDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideDoubleColumn vectorExpression =
      new DoubleScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideDoubleColumn vectorExpression =
      new DoubleScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideDoubleColumn vectorExpression =
      new DoubleScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideDoubleColumn vectorExpression =
      new DoubleScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarDivideDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarDivideDoubleColumn vectorExpression =
      new DoubleScalarDivideDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloDoubleColumnOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloDoubleColumn vectorExpression =
      new DoubleScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloDoubleColumn vectorExpression =
      new DoubleScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloDoubleColumnOutNullsColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloDoubleColumn vectorExpression =
      new DoubleScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloDoubleColumnOutNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloDoubleColumn vectorExpression =
      new DoubleScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleScalarModuloDoubleColumnOutNullsColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleScalarModuloDoubleColumn vectorExpression =
      new DoubleScalarModuloDoubleColumn(scalarValue, 0, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColEqualDoubleScalar vectorExpression =
      new LongColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColEqualDoubleScalar vectorExpression =
      new LongColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColEqualDoubleScalar vectorExpression =
      new LongColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColEqualDoubleScalar vectorExpression =
      new LongColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColEqualDoubleScalar vectorExpression =
      new LongColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColEqualDoubleScalar vectorExpression =
      new DoubleColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColEqualDoubleScalar vectorExpression =
      new DoubleColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColEqualDoubleScalar vectorExpression =
      new DoubleColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColEqualDoubleScalar vectorExpression =
      new DoubleColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColEqualDoubleScalar vectorExpression =
      new DoubleColEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColNotEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColNotEqualDoubleScalar vectorExpression =
      new LongColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColNotEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColNotEqualDoubleScalar vectorExpression =
      new LongColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColNotEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColNotEqualDoubleScalar vectorExpression =
      new LongColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColNotEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColNotEqualDoubleScalar vectorExpression =
      new LongColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColNotEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColNotEqualDoubleScalar vectorExpression =
      new LongColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColNotEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColNotEqualDoubleScalar vectorExpression =
      new DoubleColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColNotEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColNotEqualDoubleScalar vectorExpression =
      new DoubleColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColNotEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColNotEqualDoubleScalar vectorExpression =
      new DoubleColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColNotEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColNotEqualDoubleScalar vectorExpression =
      new DoubleColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColNotEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColNotEqualDoubleScalar vectorExpression =
      new DoubleColNotEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessDoubleScalar vectorExpression =
      new LongColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessDoubleScalar vectorExpression =
      new LongColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessDoubleScalar vectorExpression =
      new LongColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessDoubleScalar vectorExpression =
      new LongColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessDoubleScalar vectorExpression =
      new LongColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessDoubleScalar vectorExpression =
      new DoubleColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessDoubleScalar vectorExpression =
      new DoubleColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessDoubleScalar vectorExpression =
      new DoubleColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessDoubleScalar vectorExpression =
      new DoubleColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessDoubleScalar vectorExpression =
      new DoubleColLessDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessEqualDoubleScalar vectorExpression =
      new LongColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessEqualDoubleScalar vectorExpression =
      new LongColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessEqualDoubleScalar vectorExpression =
      new LongColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessEqualDoubleScalar vectorExpression =
      new LongColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColLessEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColLessEqualDoubleScalar vectorExpression =
      new LongColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessEqualDoubleScalar vectorExpression =
      new DoubleColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessEqualDoubleScalar vectorExpression =
      new DoubleColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessEqualDoubleScalar vectorExpression =
      new DoubleColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessEqualDoubleScalar vectorExpression =
      new DoubleColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColLessEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColLessEqualDoubleScalar vectorExpression =
      new DoubleColLessEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterDoubleScalar vectorExpression =
      new LongColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterDoubleScalar vectorExpression =
      new LongColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterDoubleScalar vectorExpression =
      new LongColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterDoubleScalar vectorExpression =
      new LongColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterDoubleScalar vectorExpression =
      new LongColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterDoubleScalar vectorExpression =
      new DoubleColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterDoubleScalar vectorExpression =
      new DoubleColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterDoubleScalar vectorExpression =
      new DoubleColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterDoubleScalar vectorExpression =
      new DoubleColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterDoubleScalar vectorExpression =
      new DoubleColGreaterDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterEqualDoubleScalar vectorExpression =
      new LongColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterEqualDoubleScalar vectorExpression =
      new LongColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterEqualDoubleScalar vectorExpression =
      new LongColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterEqualDoubleScalar vectorExpression =
      new LongColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColGreaterEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    LongColGreaterEqualDoubleScalar vectorExpression =
      new LongColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterEqualDoubleScalarOutRepeatsColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterEqualDoubleScalar vectorExpression =
      new DoubleColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterEqualDoubleScalar vectorExpression =
      new DoubleColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterEqualDoubleScalarOutNullsColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterEqualDoubleScalar vectorExpression =
      new DoubleColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterEqualDoubleScalarOutNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterEqualDoubleScalar vectorExpression =
      new DoubleColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColGreaterEqualDoubleScalarOutNullsColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;
    rowBatch.cols[1] = outputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    DoubleColGreaterEqualDoubleScalar vectorExpression =
      new DoubleColGreaterEqualDoubleScalar(0, scalarValue, 1);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
      "Output column vector is repeating state does not match operand column",
      inputColumnVector.isRepeating, outputColumnVector.isRepeating);

    assertEquals(
      "Output column vector no nulls state does not match operand column",
      inputColumnVector.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vector's is null state for index",
          inputColumnVector.isNull[i], outputColumnVector.isNull[i]);
      }
    }
  }


}


