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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Random;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Test creation and basic manipulation of VectorizedRowBatch.
 */
public class TestVectorizedRowBatch {

  // test fields
  static final String[] COLORS = {"red", "yellow", "green", "blue", "violet", "orange"};
  private static byte[][] colorsBytes;

  private VectorizedRowBatch makeBatch() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    LongColumnVector lv = new LongColumnVector();
    DoubleColumnVector dv = new DoubleColumnVector();
    BytesColumnVector bv = new BytesColumnVector();
    setSampleStringCol(bv);
    batch.cols[0] = lv;
    batch.cols[1] = dv;
    batch.cols[2] = bv;
    addRandomNulls(batch);
    return batch;
  }

  @Test
  /**
   * Make sure you can create a batch and that all columns are the
   * default size.
   */
  public void testVectorizedRowBatchCreate() {
    VectorizedRowBatch batch = makeBatch();
    Assert.assertEquals(3, batch.numCols);
    Assert.assertEquals(VectorizedRowBatch.DEFAULT_SIZE, batch.size);
    Assert.assertEquals(((LongColumnVector) batch.cols[0]).vector.length,
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertEquals(((DoubleColumnVector) batch.cols[1]).vector.length,
        VectorizedRowBatch.DEFAULT_SIZE);
    Assert.assertEquals(((BytesColumnVector) batch.cols[2]).vector.length,
        VectorizedRowBatch.DEFAULT_SIZE);
  }

  /*
   * Test routines to exercise VectorizedRowBatch
   * by filling column vectors with data and null values.
   */

  public static void setRandom(VectorizedRowBatch batch) {
    batch.size = VectorizedRowBatch.DEFAULT_SIZE;
    for (int i = 0; i != batch.numCols; i++) {
      batch.cols[i] = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      setRandomLongCol((LongColumnVector) batch.cols[i]);
    }
  }

  public static void setSample(VectorizedRowBatch batch) {
    batch.size = VectorizedRowBatch.DEFAULT_SIZE;
    for (int i = 0; i != batch.numCols; i++) {
      setSampleLongCol((LongColumnVector) batch.cols[i]);
    }
  }

  /**
   * Set to sample data, re-using existing columns in batch.
   *
   * @param batch
   */
  public static void setSampleOverwrite(VectorizedRowBatch batch) {

    // Put sample data in the columns.
    for (int i = 0; i != batch.numCols; i++) {
      setSampleLongCol((LongColumnVector) batch.cols[i]);
    }

    // Reset the selection vector.
    batch.selectedInUse = false;
    batch.size = VectorizedRowBatch.DEFAULT_SIZE;
  }

  /**
   * Sprinkle null values in this column vector.
   *
   * @param col
   */
  public static void addRandomNulls(ColumnVector col) {
    col.noNulls = false;
    Random rand = new Random();
    for(int i = 0; i != col.isNull.length; i++) {
      col.isNull[i] = Math.abs(rand.nextInt() % 11) == 0;
    }
  }

  /**
   * Add null values, but do it faster, by avoiding use of Random().
   *
   * @param col
   */
  public void addSampleNulls(ColumnVector col) {
    col.noNulls = false;
    assert col.isNull != null;
    for(int i = 0; i != col.isNull.length; i++) {
      col.isNull[i] = i % 11 == 0;
    }
  }

  public static void addRandomNulls(VectorizedRowBatch batch) {
    for (int i = 0; i != batch.numCols; i++) {
      addRandomNulls(batch.cols[i]);
    }
  }

  public void addSampleNulls(VectorizedRowBatch batch) {
    for (int i = 0; i != batch.numCols; i++) {
      addSampleNulls(batch.cols[i]);
    }
  }

  /**
   * Set vector elements to sample string data from colorsBytes string table.
   * @param col
   */
  public static void setSampleStringCol(BytesColumnVector col) {
    initColors();
    int size = col.vector.length;
    for(int i = 0; i != size; i++) {
      int pos = i % colorsBytes.length;
      col.setRef(i, colorsBytes[pos], 0, colorsBytes[pos].length);
    }
  }

  /*
   * Initialize string table in a lazy fashion.
   */
  private static void initColors() {
    if (colorsBytes == null) {
      colorsBytes = new byte[COLORS.length][];
      for (int i = 0; i != COLORS.length; i++) {
        colorsBytes[i] = COLORS[i].getBytes();
      }
    }
  }


  /**
   * Set the vector to sample data that repeats an iteration from 0 to 99.
   * @param col
   */
  public static void setSampleLongCol(LongColumnVector col) {
    int size = col.vector.length;
    for(int i = 0; i != size; i++) {
      col.vector[i] = i % 100;
    }
  }

  /**
   * Set the vector to random data in the range 0 to 99.
   * This has significant overhead for random number generation. Use setSample() to reduce overhead.
   */
  public static void setRandomLongCol(LongColumnVector col) {
    int size = col.vector.length;
    Random rand = new Random(System.currentTimeMillis());
    for(int i = 0; i != size; i++) {
      col.vector[i] = Math.abs(rand.nextInt() % 100);
    }
  }

  public static void setRepeatingLongCol(LongColumnVector col) {
    col.isRepeating = true;
    col.vector[0] = 50;
  }

  /**
   * Set the vector to sample data that repeats an iteration from 0 to 99.
   * @param col
   */
  public static void setSampleDoubleCol(DoubleColumnVector col) {
    int size = col.vector.length;
    for(int i = 0; i != size; i++) {
      col.vector[i] = i % 100;
    }
  }

  /**
   * Set the vector to random data in the range 0 to 99.
   * This has significant overhead for random number generation. Use setSample() to reduce overhead.
   */
  public static void setRandomDoubleCol(DoubleColumnVector col) {
    int size = col.vector.length;
    Random rand = new Random();
    for(int i = 0; i != size; i++) {
      col.vector[i] = Math.abs(rand.nextInt() % 100);
    }
  }

  public static void setRepeatingDoubleCol(DoubleColumnVector col) {
    col.isRepeating = true;
    col.vector[0] = 50.0;
  }

  @Test
  public void testFlatten() {
    verifyFlatten(new LongColumnVector());
    verifyFlatten(new DoubleColumnVector());
    verifyFlatten(new BytesColumnVector());
  }

  private void verifyFlatten(ColumnVector v) {

    // verify that flattening and unflattenting no-nulls works
    v.noNulls = true;
    v.isNull[1] = true;
    int[] sel = {0, 2};
    int size = 2;
    v.flatten(true, sel, size);
    Assert.assertFalse(v.noNulls);
    Assert.assertFalse(v.isNull[0] || v.isNull[2]);
    v.unFlatten();
    Assert.assertTrue(v.noNulls);

    // verify that flattening and unflattening "isRepeating" works
    v.isRepeating = true;
    v.noNulls = false;
    v.isNull[0] = true;
    v.flatten(true, sel, 2);
    Assert.assertFalse(v.noNulls);
    Assert.assertTrue(v.isNull[0] && v.isNull[2]);
    Assert.assertFalse(v.isRepeating);
    v.unFlatten();
    Assert.assertFalse(v.noNulls);
    Assert.assertTrue(v.isRepeating);

    // verify extension of values in the array
    v.noNulls = true;
    if (v instanceof LongColumnVector) {
      ((LongColumnVector) v).vector[0] = 100;
      v.flatten(true, sel, 2);
      Assert.assertTrue(((LongColumnVector) v).vector[2] == 100);
    } else if (v instanceof DoubleColumnVector) {
      ((DoubleColumnVector) v).vector[0] = 200d;
      v.flatten(true, sel, 2);
      Assert.assertTrue(((DoubleColumnVector) v).vector[2] == 200d);
    } else if (v instanceof BytesColumnVector) {
      BytesColumnVector bv = (BytesColumnVector) v;
      byte[] b = null;
      try {
        b = "foo".getBytes("UTF-8");
      } catch (Exception e) {
        ; // eat it
      }
      bv.setRef(0, b, 0, b.length);
      bv.flatten(true, sel, 2);
      Assert.assertEquals(bv.vector[0],  bv.vector[2]);
      Assert.assertEquals(bv.start[0], bv.start[2]);
      Assert.assertEquals(bv.length[0], bv.length[2]);
    }
  }


}
