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
 * TestColumnColumnOperationVectorExpressionEvaluation.
 *
 */
public class TestColumnColumnOperationVectorExpressionEvaluation{

  private static final int BATCH_SIZE = 100;
  private static final long SEED = 0xfa57;

  
  @Test
  public void testLongColAddLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddLongColumn vectorExpression =
      new LongColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddLongColumn vectorExpression =
      new LongColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddLongColumn vectorExpression =
      new LongColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddLongColumn vectorExpression =
      new LongColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddLongColumn vectorExpression =
      new LongColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddLongColumn vectorExpression =
      new LongColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractLongColumn vectorExpression =
      new LongColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractLongColumn vectorExpression =
      new LongColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractLongColumn vectorExpression =
      new LongColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractLongColumn vectorExpression =
      new LongColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractLongColumn vectorExpression =
      new LongColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractLongColumn vectorExpression =
      new LongColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyLongColumn vectorExpression =
      new LongColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyLongColumn vectorExpression =
      new LongColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyLongColumn vectorExpression =
      new LongColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyLongColumn vectorExpression =
      new LongColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyLongColumn vectorExpression =
      new LongColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyLongColumn vectorExpression =
      new LongColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloLongColumn vectorExpression =
      new LongColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloLongColumn vectorExpression =
      new LongColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloLongColumn vectorExpression =
      new LongColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloLongColumn vectorExpression =
      new LongColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloLongColumn vectorExpression =
      new LongColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloLongColumn vectorExpression =
      new LongColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddDoubleColumn vectorExpression =
      new LongColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddDoubleColumn vectorExpression =
      new LongColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddDoubleColumn vectorExpression =
      new LongColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddDoubleColumn vectorExpression =
      new LongColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddDoubleColumn vectorExpression =
      new LongColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColAddDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColAddDoubleColumn vectorExpression =
      new LongColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractDoubleColumn vectorExpression =
      new LongColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractDoubleColumn vectorExpression =
      new LongColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractDoubleColumn vectorExpression =
      new LongColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractDoubleColumn vectorExpression =
      new LongColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractDoubleColumn vectorExpression =
      new LongColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColSubtractDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColSubtractDoubleColumn vectorExpression =
      new LongColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyDoubleColumn vectorExpression =
      new LongColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyDoubleColumn vectorExpression =
      new LongColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyDoubleColumn vectorExpression =
      new LongColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyDoubleColumn vectorExpression =
      new LongColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyDoubleColumn vectorExpression =
      new LongColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColMultiplyDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColMultiplyDoubleColumn vectorExpression =
      new LongColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColDivideDoubleColumn vectorExpression =
      new LongColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColDivideDoubleColumn vectorExpression =
      new LongColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColDivideDoubleColumn vectorExpression =
      new LongColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColDivideDoubleColumn vectorExpression =
      new LongColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColDivideDoubleColumn vectorExpression =
      new LongColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColDivideDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColDivideDoubleColumn vectorExpression =
      new LongColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloDoubleColumn vectorExpression =
      new LongColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloDoubleColumn vectorExpression =
      new LongColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloDoubleColumn vectorExpression =
      new LongColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloDoubleColumn vectorExpression =
      new LongColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloDoubleColumn vectorExpression =
      new LongColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testLongColModuloDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    LongColModuloDoubleColumn vectorExpression =
      new LongColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddLongColumn vectorExpression =
      new DoubleColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddLongColumn vectorExpression =
      new DoubleColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddLongColumn vectorExpression =
      new DoubleColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddLongColumn vectorExpression =
      new DoubleColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddLongColumn vectorExpression =
      new DoubleColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddLongColumn vectorExpression =
      new DoubleColAddLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractLongColumn vectorExpression =
      new DoubleColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractLongColumn vectorExpression =
      new DoubleColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractLongColumn vectorExpression =
      new DoubleColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractLongColumn vectorExpression =
      new DoubleColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractLongColumn vectorExpression =
      new DoubleColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractLongColumn vectorExpression =
      new DoubleColSubtractLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyLongColumn vectorExpression =
      new DoubleColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyLongColumn vectorExpression =
      new DoubleColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyLongColumn vectorExpression =
      new DoubleColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyLongColumn vectorExpression =
      new DoubleColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyLongColumn vectorExpression =
      new DoubleColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyLongColumn vectorExpression =
      new DoubleColMultiplyLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideLongColumn vectorExpression =
      new DoubleColDivideLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideLongColumn vectorExpression =
      new DoubleColDivideLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideLongColumn vectorExpression =
      new DoubleColDivideLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideLongColumn vectorExpression =
      new DoubleColDivideLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideLongColumn vectorExpression =
      new DoubleColDivideLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideLongColumn vectorExpression =
      new DoubleColDivideLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloLongColumn vectorExpression =
      new DoubleColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloLongColumn vectorExpression =
      new DoubleColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloLongColumn vectorExpression =
      new DoubleColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloLongColumn vectorExpression =
      new DoubleColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloLongColumn vectorExpression =
      new DoubleColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloLongColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloLongColumn vectorExpression =
      new DoubleColModuloLongColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddDoubleColumn vectorExpression =
      new DoubleColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddDoubleColumn vectorExpression =
      new DoubleColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddDoubleColumn vectorExpression =
      new DoubleColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddDoubleColumn vectorExpression =
      new DoubleColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddDoubleColumn vectorExpression =
      new DoubleColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColAddDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColAddDoubleColumn vectorExpression =
      new DoubleColAddDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractDoubleColumn vectorExpression =
      new DoubleColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractDoubleColumn vectorExpression =
      new DoubleColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractDoubleColumn vectorExpression =
      new DoubleColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractDoubleColumn vectorExpression =
      new DoubleColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractDoubleColumn vectorExpression =
      new DoubleColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColSubtractDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColSubtractDoubleColumn vectorExpression =
      new DoubleColSubtractDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyDoubleColumn vectorExpression =
      new DoubleColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyDoubleColumn vectorExpression =
      new DoubleColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyDoubleColumn vectorExpression =
      new DoubleColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyDoubleColumn vectorExpression =
      new DoubleColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyDoubleColumn vectorExpression =
      new DoubleColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColMultiplyDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColMultiplyDoubleColumn vectorExpression =
      new DoubleColMultiplyDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideDoubleColumn vectorExpression =
      new DoubleColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideDoubleColumn vectorExpression =
      new DoubleColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideDoubleColumn vectorExpression =
      new DoubleColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideDoubleColumn vectorExpression =
      new DoubleColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideDoubleColumn vectorExpression =
      new DoubleColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColDivideDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColDivideDoubleColumn vectorExpression =
      new DoubleColDivideDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleColumnOutNullsRepeatsC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloDoubleColumn vectorExpression =
      new DoubleColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleColumnC1Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloDoubleColumn vectorExpression =
      new DoubleColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleColumnOutNullsC1NullsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloDoubleColumn vectorExpression =
      new DoubleColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleColumnOutNullsRepeatsC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloDoubleColumn vectorExpression =
      new DoubleColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleColumnC1RepeatsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloDoubleColumn vectorExpression =
      new DoubleColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }

  @Test
  public void testDoubleColModuloDoubleColumnOutRepeatsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector outputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(3, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;
    rowBatch.cols[2] = outputColumnVector;

    DoubleColModuloDoubleColumn vectorExpression =
      new DoubleColModuloDoubleColumn(0, 1, 2);

    vectorExpression.evaluate(rowBatch);

    assertEquals(
        "Output column vector repeating state does not match operand columns",
        (!inputColumnVector1.noNulls && inputColumnVector1.isRepeating)
        || (!inputColumnVector2.noNulls && inputColumnVector2.isRepeating)
        || inputColumnVector1.isRepeating && inputColumnVector2.isRepeating,
        outputColumnVector.isRepeating);

    assertEquals(
        "Output column vector no nulls state does not match operand columns",
        inputColumnVector1.noNulls && inputColumnVector2.noNulls, outputColumnVector.noNulls);

    if(!outputColumnVector.noNulls && !outputColumnVector.isRepeating) {
      for(int i = 0; i < BATCH_SIZE; i++) {
        //null vectors are safe to check, as they are always initialized to match the data vector
        assertEquals("Output vector doesn't match input vectors' is null state for index",
          inputColumnVector1.isNull[i] || inputColumnVector2.isNull[i],
          outputColumnVector.isNull[i]);
      }
    }
  }


}


