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
 * TestColumnColumnFilterVectorExpressionEvaluation.
 *
 */
public class TestColumnColumnFilterVectorExpressionEvaluation{

  private static final int BATCH_SIZE = 100;
  private static final long SEED = 0xfa57;

  
  @Test
  public void testFilterLongColEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualDoubleColumn vectorExpression =
      new FilterLongColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualDoubleColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualDoubleColumn vectorExpression =
      new FilterLongColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualDoubleColumn vectorExpression =
      new FilterLongColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualDoubleColumn vectorExpression =
      new FilterLongColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualDoubleColumn vectorExpression =
      new FilterLongColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualDoubleColumn vectorExpression =
      new FilterDoubleColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualDoubleColumn vectorExpression =
      new FilterDoubleColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualDoubleColumn vectorExpression =
      new FilterDoubleColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualDoubleColumn vectorExpression =
      new FilterDoubleColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualDoubleColumn vectorExpression =
      new FilterDoubleColEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualDoubleColumn vectorExpression =
      new FilterLongColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualDoubleColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualDoubleColumn vectorExpression =
      new FilterLongColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualDoubleColumn vectorExpression =
      new FilterLongColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualDoubleColumn vectorExpression =
      new FilterLongColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualDoubleColumn vectorExpression =
      new FilterLongColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualDoubleColumn vectorExpression =
      new FilterDoubleColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualDoubleColumn vectorExpression =
      new FilterDoubleColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualDoubleColumn vectorExpression =
      new FilterDoubleColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualDoubleColumn vectorExpression =
      new FilterDoubleColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualDoubleColumn vectorExpression =
      new FilterDoubleColNotEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessDoubleColumn vectorExpression =
      new FilterLongColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessDoubleColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessDoubleColumn vectorExpression =
      new FilterLongColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessDoubleColumn vectorExpression =
      new FilterLongColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessDoubleColumn vectorExpression =
      new FilterLongColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessDoubleColumn vectorExpression =
      new FilterLongColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessDoubleColumn vectorExpression =
      new FilterDoubleColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessDoubleColumn vectorExpression =
      new FilterDoubleColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessDoubleColumn vectorExpression =
      new FilterDoubleColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessDoubleColumn vectorExpression =
      new FilterDoubleColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessDoubleColumn vectorExpression =
      new FilterDoubleColLessDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualDoubleColumn vectorExpression =
      new FilterLongColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualDoubleColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualDoubleColumn vectorExpression =
      new FilterLongColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualDoubleColumn vectorExpression =
      new FilterLongColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualDoubleColumn vectorExpression =
      new FilterLongColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualDoubleColumn vectorExpression =
      new FilterLongColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualDoubleColumn vectorExpression =
      new FilterDoubleColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualDoubleColumn vectorExpression =
      new FilterDoubleColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualDoubleColumn vectorExpression =
      new FilterDoubleColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualDoubleColumn vectorExpression =
      new FilterDoubleColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualDoubleColumn vectorExpression =
      new FilterDoubleColLessEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterDoubleColumn vectorExpression =
      new FilterLongColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterDoubleColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterDoubleColumn vectorExpression =
      new FilterLongColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterDoubleColumn vectorExpression =
      new FilterLongColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterDoubleColumn vectorExpression =
      new FilterLongColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterDoubleColumn vectorExpression =
      new FilterLongColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterDoubleColumn vectorExpression =
      new FilterDoubleColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterDoubleColumn vectorExpression =
      new FilterDoubleColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterDoubleColumn vectorExpression =
      new FilterDoubleColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterDoubleColumn vectorExpression =
      new FilterDoubleColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterDoubleColumn vectorExpression =
      new FilterDoubleColGreaterDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualDoubleColumn vectorExpression =
      new FilterLongColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualDoubleColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualDoubleColumn vectorExpression =
      new FilterLongColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualDoubleColumn vectorExpression =
      new FilterLongColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualDoubleColumn vectorExpression =
      new FilterLongColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualDoubleColumn vectorExpression =
      new FilterLongColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualDoubleColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualDoubleColumn vectorExpression =
      new FilterDoubleColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualDoubleColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualDoubleColumn vectorExpression =
      new FilterDoubleColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualDoubleColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualDoubleColumn vectorExpression =
      new FilterDoubleColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualDoubleColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualDoubleColumn vectorExpression =
      new FilterDoubleColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualDoubleColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    DoubleColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualDoubleColumn vectorExpression =
      new FilterDoubleColGreaterEqualDoubleColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualLongColumn vectorExpression =
      new FilterLongColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualLongColumn vectorExpression =
      new FilterLongColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualLongColumn vectorExpression =
      new FilterLongColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualLongColumn vectorExpression =
      new FilterLongColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColEqualLongColumn vectorExpression =
      new FilterLongColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualLongColumn vectorExpression =
      new FilterDoubleColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualLongColumn vectorExpression =
      new FilterDoubleColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualLongColumn vectorExpression =
      new FilterDoubleColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualLongColumn vectorExpression =
      new FilterDoubleColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColEqualLongColumn vectorExpression =
      new FilterDoubleColEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] == inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "=="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualLongColumn vectorExpression =
      new FilterLongColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualLongColumn vectorExpression =
      new FilterLongColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualLongColumn vectorExpression =
      new FilterLongColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualLongColumn vectorExpression =
      new FilterLongColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColNotEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColNotEqualLongColumn vectorExpression =
      new FilterLongColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualLongColumn vectorExpression =
      new FilterDoubleColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualLongColumn vectorExpression =
      new FilterDoubleColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualLongColumn vectorExpression =
      new FilterDoubleColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualLongColumn vectorExpression =
      new FilterDoubleColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColNotEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColNotEqualLongColumn vectorExpression =
      new FilterDoubleColNotEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] != inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "!="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessLongColumn vectorExpression =
      new FilterLongColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessLongColumn vectorExpression =
      new FilterLongColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessLongColumn vectorExpression =
      new FilterLongColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessLongColumn vectorExpression =
      new FilterLongColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessLongColumn vectorExpression =
      new FilterLongColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessLongColumn vectorExpression =
      new FilterDoubleColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessLongColumn vectorExpression =
      new FilterDoubleColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessLongColumn vectorExpression =
      new FilterDoubleColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessLongColumn vectorExpression =
      new FilterDoubleColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessLongColumn vectorExpression =
      new FilterDoubleColLessLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] < inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualLongColumn vectorExpression =
      new FilterLongColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualLongColumn vectorExpression =
      new FilterLongColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualLongColumn vectorExpression =
      new FilterLongColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualLongColumn vectorExpression =
      new FilterLongColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColLessEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColLessEqualLongColumn vectorExpression =
      new FilterLongColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualLongColumn vectorExpression =
      new FilterDoubleColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualLongColumn vectorExpression =
      new FilterDoubleColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualLongColumn vectorExpression =
      new FilterDoubleColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualLongColumn vectorExpression =
      new FilterDoubleColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColLessEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColLessEqualLongColumn vectorExpression =
      new FilterDoubleColLessEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] <= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + "<="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterLongColumn vectorExpression =
      new FilterLongColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterLongColumn vectorExpression =
      new FilterLongColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterLongColumn vectorExpression =
      new FilterLongColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterLongColumn vectorExpression =
      new FilterLongColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterLongColumn vectorExpression =
      new FilterLongColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterLongColumn vectorExpression =
      new FilterDoubleColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterLongColumn vectorExpression =
      new FilterDoubleColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterLongColumn vectorExpression =
      new FilterDoubleColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterLongColumn vectorExpression =
      new FilterDoubleColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterLongColumn vectorExpression =
      new FilterDoubleColGreaterLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] > inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">"
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualLongColumn vectorExpression =
      new FilterLongColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualLongColumn() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualLongColumn vectorExpression =
      new FilterLongColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualLongColumn vectorExpression =
      new FilterLongColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualLongColumn vectorExpression =
      new FilterLongColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterLongColGreaterEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    LongColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterLongColGreaterEqualLongColumn vectorExpression =
      new FilterLongColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualLongColumnC1RepeatsC2NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualLongColumn vectorExpression =
      new FilterDoubleColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualLongColumn() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(false,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualLongColumn vectorExpression =
      new FilterDoubleColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualLongColumnC1NullsC2Nulls() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(true,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualLongColumn vectorExpression =
      new FilterDoubleColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualLongColumnC1NullsRepeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      true, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      false, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualLongColumn vectorExpression =
      new FilterDoubleColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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
  public void testFilterDoubleColGreaterEqualLongColumnC1NullsC2Repeats() {

    Random rand = new Random(SEED);

    DoubleColumnVector inputColumnVector1 =
      VectorizedRowGroupGenUtil.generateDoubleColumnVector(true,
      false, BATCH_SIZE, rand);

    LongColumnVector inputColumnVector2 =
      VectorizedRowGroupGenUtil.generateLongColumnVector(false,
      true, BATCH_SIZE, rand);

    VectorizedRowBatch rowBatch = new VectorizedRowBatch(2, BATCH_SIZE);
    rowBatch.cols[0] = inputColumnVector1;
    rowBatch.cols[1] = inputColumnVector2;

    FilterDoubleColGreaterEqualLongColumn vectorExpression =
      new FilterDoubleColGreaterEqualLongColumn(0, 1);

    vectorExpression.evaluate(rowBatch);

    int selectedIndex = 0;
    for(int i = 0; i < BATCH_SIZE; i++) {
      //null vector is safe to check, as it is always initialized to match the data vector
      if(!inputColumnVector1.isNull[i] && !inputColumnVector2.isNull[i]) {
        if(inputColumnVector1.vector[i] >= inputColumnVector2.vector[i]) {
          assertEquals(
            "Vector index that passes filter "
            + inputColumnVector1.vector[i] + ">="
            + inputColumnVector2.vector[i] + " is not in rowBatch selected index",
            i,
            rowBatch.selected[selectedIndex]);
          selectedIndex++;
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


