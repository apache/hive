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

import java.util.Random;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.benchmark.vectorization.ColumnVectorGenUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestMurmurHashExpression {

  private Random rand = new Random(TestMurmurHashExpression.class.getName().getBytes().hashCode());
  private int SIZE = VectorizedRowBatch.DEFAULT_SIZE;

  @Test
  public void testMurmurHashStringColIntCol() throws HiveException {
    BytesColumnVector cvString = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);
    LongColumnVector cvInt = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString;
    vrb.cols[1] = cvInt;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColIntCol(0, 1, 2).evaluate(vrb);

    for (int i = 0; i < SIZE; i++) {
      Text t = new Text();
      t.set(cvString.vector[i], cvString.start[i], cvString.length[i]);
      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(
              new Object[] { t, new LongWritable(cvInt.vector[i]) },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                  PrimitiveObjectInspectorFactory.writableLongObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashStringColStringCol() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Text t1 = new Text();
      t1.set(cvString1.vector[i], cvString1.start[i], cvString1.length[i]);
      Text t2 = new Text();
      t2.set(cvString2.vector[i], cvString2.start[i], cvString2.length[i]);

      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(new Object[] { t1, t2 },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                  PrimitiveObjectInspectorFactory.writableStringObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashIntColIntCol() throws HiveException {
    LongColumnVector cvInt1 = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);
    LongColumnVector cvInt2 = (LongColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("int"), false, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvInt1;
    vrb.cols[1] = cvInt2;

    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(ObjectInspectorUtils.getBucketHashCode(
          new Object[] { new LongWritable(cvInt1.vector[i]), new LongWritable(cvInt2.vector[i]) },
          new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableLongObjectInspector,
              PrimitiveObjectInspectorFactory.writableLongObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashRepeating() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(true, vrb.cols[2].isRepeating); // both of the inputs were repeating

    Text t1 = new Text();
    t1.set(cvString1.vector[0], cvString1.start[0], cvString1.length[0]);
    Text t2 = new Text();
    t2.set(cvString2.vector[0], cvString2.start[0], cvString2.length[0]);

    // output's first element is the hash of first input elements
    Assert.assertEquals(
        ObjectInspectorUtils.getBucketHashCode(new Object[] { t1, t2 },
            new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                PrimitiveObjectInspectorFactory.writableStringObjectInspector }),
        ((LongColumnVector) vrb.cols[2]).vector[0]);
  }

  @Test
  public void testMurmurHashRepeatingBothNulls() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), false, true, SIZE, rand);

    cvString1.isNull[0] = true;
    cvString2.isNull[0] = true;

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    // fake output value to test short-circuiting
    ((LongColumnVector) vrb.cols[2]).vector[1] = 1234;

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(true, vrb.cols[2].isRepeating); // both of the inputs were repeating

    // output's first element is 0, which is hash of null elements
    Assert.assertEquals(0, ((LongColumnVector) vrb.cols[2]).vector[0]);

    // if isRepeating, vectorization logic is not supposed to touch other elements than 0th
    Assert.assertEquals(1234, ((LongColumnVector) vrb.cols[2]).vector[1]);
  }

  @Test
  public void testMurmurHashWithNullsString() throws HiveException {
    BytesColumnVector cvString1 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), true, false, SIZE, rand);
    BytesColumnVector cvString2 = (BytesColumnVector) ColumnVectorGenUtil.generateColumnVector(
        TypeInfoFactory.getPrimitiveTypeInfo("string"), true, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvString1;
    vrb.cols[1] = cvString2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashStringColStringCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Text t1 = null;
      if (!cvString1.isNull[i]) {
        t1 = new Text();
        t1.set(cvString1.vector[i], cvString1.start[i], cvString1.length[i]);
      }

      Text t2 = null;
      if (!cvString2.isNull[i]) {
        t2 = new Text();
        t2.set(cvString2.vector[i], cvString2.start[i], cvString2.length[i]);
      }

      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(new Object[] { t1, t2 },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                  PrimitiveObjectInspectorFactory.writableStringObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }

  @Test
  public void testMurmurHashWithNullsInt() throws HiveException {
    LongColumnVector cvInt1 = (LongColumnVector) ColumnVectorGenUtil
        .generateColumnVector(TypeInfoFactory.getPrimitiveTypeInfo("int"), true, false, SIZE, rand);
    LongColumnVector cvInt2 = (LongColumnVector) ColumnVectorGenUtil
        .generateColumnVector(TypeInfoFactory.getPrimitiveTypeInfo("int"), true, false, SIZE, rand);

    VectorizedRowBatch vrb = new VectorizedRowBatch(3, SIZE);
    vrb.cols[0] = cvInt1;
    vrb.cols[1] = cvInt2;
    vrb.cols[2] = new LongColumnVector(SIZE);

    new MurmurHashIntColIntCol(0, 1, 2).evaluate(vrb);

    Assert.assertEquals(false, vrb.cols[2].isRepeating); // non-repeating

    for (int i = 0; i < SIZE; i++) {
      Assert.assertEquals(
          ObjectInspectorUtils.getBucketHashCode(
              new Object[] { cvInt1.isNull[i] ? null : new LongWritable(cvInt1.vector[i]),
                  cvInt2.isNull[i] ? null : new LongWritable(cvInt2.vector[i]) },
              new ObjectInspector[] { PrimitiveObjectInspectorFactory.writableLongObjectInspector,
                  PrimitiveObjectInspectorFactory.writableLongObjectInspector }),
          ((LongColumnVector) vrb.cols[2]).vector[i]);
    }
  }
}
