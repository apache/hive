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
 * TestColumnScalarFilterVectorExpressionEvaluation.
 *
 */
public class TestColumnScalarFilterVectorExpressionEvaluation{

  private static final int BATCH_SIZE = 100;
  private static final long SEED = 0xfa57;

  
  @Test
  public void testFilterLongColEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColEqualDoubleScalar vectorExpression =
      new FilterLongColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColEqualDoubleScalar vectorExpression =
      new FilterLongColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColEqualDoubleScalar vectorExpression =
      new FilterLongColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColEqualDoubleScalar vectorExpression =
      new FilterLongColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColEqualDoubleScalar vectorExpression =
      new FilterDoubleColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColEqualDoubleScalar vectorExpression =
      new FilterDoubleColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColEqualDoubleScalar vectorExpression =
      new FilterDoubleColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColEqualDoubleScalar vectorExpression =
      new FilterDoubleColEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColNotEqualDoubleScalar vectorExpression =
      new FilterLongColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColNotEqualDoubleScalar vectorExpression =
      new FilterLongColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColNotEqualDoubleScalar vectorExpression =
      new FilterLongColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColNotEqualDoubleScalar vectorExpression =
      new FilterLongColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualDoubleScalar vectorExpression =
      new FilterDoubleColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualDoubleScalar vectorExpression =
      new FilterDoubleColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualDoubleScalar vectorExpression =
      new FilterDoubleColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualDoubleScalar vectorExpression =
      new FilterDoubleColNotEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessDoubleScalar vectorExpression =
      new FilterLongColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessDoubleScalar vectorExpression =
      new FilterLongColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessDoubleScalar vectorExpression =
      new FilterLongColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessDoubleScalar vectorExpression =
      new FilterLongColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessDoubleScalar vectorExpression =
      new FilterDoubleColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessDoubleScalar vectorExpression =
      new FilterDoubleColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessDoubleScalar vectorExpression =
      new FilterDoubleColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessDoubleScalar vectorExpression =
      new FilterDoubleColLessDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessEqualDoubleScalar vectorExpression =
      new FilterLongColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessEqualDoubleScalar vectorExpression =
      new FilterLongColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessEqualDoubleScalar vectorExpression =
      new FilterLongColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColLessEqualDoubleScalar vectorExpression =
      new FilterLongColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualDoubleScalar vectorExpression =
      new FilterDoubleColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualDoubleScalar vectorExpression =
      new FilterDoubleColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualDoubleScalar vectorExpression =
      new FilterDoubleColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualDoubleScalar vectorExpression =
      new FilterDoubleColLessEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterDoubleScalar vectorExpression =
      new FilterLongColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterDoubleScalar vectorExpression =
      new FilterLongColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterDoubleScalar vectorExpression =
      new FilterLongColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterDoubleScalar vectorExpression =
      new FilterLongColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterDoubleScalar vectorExpression =
      new FilterDoubleColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterDoubleScalar vectorExpression =
      new FilterDoubleColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterDoubleScalar vectorExpression =
      new FilterDoubleColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterDoubleScalar vectorExpression =
      new FilterDoubleColGreaterDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualDoubleScalar vectorExpression =
      new FilterLongColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualDoubleScalar vectorExpression =
      new FilterLongColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualDoubleScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualDoubleScalar vectorExpression =
      new FilterLongColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualDoubleScalar vectorExpression =
      new FilterLongColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualDoubleScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualDoubleScalar vectorExpression =
      new FilterDoubleColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualDoubleScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualDoubleScalar vectorExpression =
      new FilterDoubleColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualDoubleScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualDoubleScalar vectorExpression =
      new FilterDoubleColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualDoubleScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    double scalarValue = 0;
    do {
      scalarValue = rand.nextDouble();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualDoubleScalar vectorExpression =
      new FilterDoubleColGreaterEqualDoubleScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColEqualLongScalar vectorExpression =
      new FilterLongColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColEqualLongScalar vectorExpression =
      new FilterLongColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColEqualLongScalar vectorExpression =
      new FilterLongColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColEqualLongScalar vectorExpression =
      new FilterLongColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColEqualLongScalar vectorExpression =
      new FilterDoubleColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColEqualLongScalar vectorExpression =
      new FilterDoubleColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColEqualLongScalar vectorExpression =
      new FilterDoubleColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColEqualLongScalar vectorExpression =
      new FilterDoubleColEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] == scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] == scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "=="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColNotEqualLongScalar vectorExpression =
      new FilterLongColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColNotEqualLongScalar vectorExpression =
      new FilterLongColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColNotEqualLongScalar vectorExpression =
      new FilterLongColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColNotEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColNotEqualLongScalar vectorExpression =
      new FilterLongColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualLongScalar vectorExpression =
      new FilterDoubleColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualLongScalar vectorExpression =
      new FilterDoubleColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualLongScalar vectorExpression =
      new FilterDoubleColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColNotEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColNotEqualLongScalar vectorExpression =
      new FilterDoubleColNotEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] != scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] != scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "!="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessLongScalar vectorExpression =
      new FilterLongColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessLongScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessLongScalar vectorExpression =
      new FilterLongColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessLongScalar vectorExpression =
      new FilterLongColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessLongScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessLongScalar vectorExpression =
      new FilterLongColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessLongScalar vectorExpression =
      new FilterDoubleColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessLongScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessLongScalar vectorExpression =
      new FilterDoubleColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessLongScalar vectorExpression =
      new FilterDoubleColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessLongScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessLongScalar vectorExpression =
      new FilterDoubleColLessLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] < scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] < scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessEqualLongScalar vectorExpression =
      new FilterLongColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessEqualLongScalar vectorExpression =
      new FilterLongColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessEqualLongScalar vectorExpression =
      new FilterLongColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColLessEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColLessEqualLongScalar vectorExpression =
      new FilterLongColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualLongScalar vectorExpression =
      new FilterDoubleColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualLongScalar vectorExpression =
      new FilterDoubleColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualLongScalar vectorExpression =
      new FilterDoubleColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColLessEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColLessEqualLongScalar vectorExpression =
      new FilterDoubleColLessEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] <= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] <= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + "<="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterLongScalar vectorExpression =
      new FilterLongColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterLongScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterLongScalar vectorExpression =
      new FilterLongColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterLongScalar vectorExpression =
      new FilterLongColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterLongScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterLongScalar vectorExpression =
      new FilterLongColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterLongScalar vectorExpression =
      new FilterDoubleColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterLongScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterLongScalar vectorExpression =
      new FilterDoubleColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterLongScalar vectorExpression =
      new FilterDoubleColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterLongScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterLongScalar vectorExpression =
      new FilterDoubleColGreaterLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] > scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] > scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">"
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualLongScalar vectorExpression =
      new FilterLongColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualLongScalar vectorExpression =
      new FilterLongColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualLongScalar() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualLongScalar vectorExpression =
      new FilterLongColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterLongColGreaterEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterLongColGreaterEqualLongScalar vectorExpression =
      new FilterLongColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualLongScalarColNullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualLongScalar vectorExpression =
      new FilterDoubleColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualLongScalarColNulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualLongScalar vectorExpression =
      new FilterDoubleColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualLongScalar() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualLongScalar vectorExpression =
      new FilterDoubleColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }

  @Test
  public void testFilterDoubleColGreaterEqualLongScalarColRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(1, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector;

    long scalarValue = 0;
    do {
      scalarValue = rand.nextLong();
    } while(scalarValue == 0);

    FilterDoubleColGreaterEqualLongScalar vectorExpression =
      new FilterDoubleColGreaterEqualLongScalar(0, scalarValue);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    //check for isRepeating optimization
    if(inputColumnVector.isRepeating) {
      //null vector is safe to check, as it is always initialized to match the data vector
      selectedIndex =
        !inputColumnVector.isNull[0] && inputColumnVector.vector[0] >= scalarValue
          ? BATCH_SIZE : 0;
    } else {
      for(int i = 0; i < BATCH_SIZE; i++) {
        if(!inputColumnVector.isNull[i]) {
          if(inputColumnVector.vector[i] >= scalarValue) {
            assertEquals(
              "Vector index that passes filter "
              + inputColumnVector.vector[i] + ">="
              + scalarValue + " is not in rowBatch selected index",
              i,
              rowBatch.selected[selectedIndex]);
            selectedIndex++;
          }
        }
      }
    }

    assertEquals("Row batch size not set to number of selected rows: " + selectedIndex,
      selectedIndex, rowBatch.size);

    if(selectedIndex > 0 && selectedIndex < BATCH_SIZE) {
      assertEquals(
        "selectedInUse should be set when > 0 and < entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        true, rowBatch.selectedInUse);
    } else if(selectedIndex == BATCH_SIZE) {
      assertEquals(
        "selectedInUse should not be set when entire batch(" + BATCH_SIZE + ") is selected: "
        + selectedIndex,
        false, rowBatch.selectedInUse);
    }
  }


}


