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

package org.apache.hadoop.hive.ql.udaf;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.ISupportStreamingModeForWindowing;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class TestStreamingSum {

  public static WindowFrameDef wdwFrame(int p, int f) {
    BoundaryDef start, end;
    if (p == 0) {
      start = new BoundaryDef(Direction.CURRENT, 0);
    } else {
      start = new BoundaryDef(Direction.PRECEDING, p);
    }

    if (f == 0) {
      end = new BoundaryDef(Direction.CURRENT, 0);
    } else {
      end = new BoundaryDef(Direction.FOLLOWING, f);
    }

    return new WindowFrameDef(WindowType.ROWS, start, end);
  }

  public void sumDouble(Iterator<Double> inVals, int inSz, int numPreceding,
      int numFollowing, Iterator<Double> outVals) throws HiveException {

    GenericUDAFSum fnR = new GenericUDAFSum();
    TypeInfo[] inputTypes = { TypeInfoFactory.doubleTypeInfo };
    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableDoubleObjectInspector };

    DoubleWritable[] in = new DoubleWritable[1];
    in[0] = new DoubleWritable();

    _agg(fnR, inputTypes, inVals, TypeHandler.DoubleHandler, in, inputOIs,
        inSz, numPreceding, numFollowing, outVals);

  }

  public void sumLong(Iterator<Long> inVals, int inSz, int numPreceding,
      int numFollowing, Iterator<Long> outVals) throws HiveException {

    GenericUDAFSum fnR = new GenericUDAFSum();
    TypeInfo[] inputTypes = { TypeInfoFactory.longTypeInfo };
    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableLongObjectInspector };

    LongWritable[] in = new LongWritable[1];
    in[0] = new LongWritable();

    _agg(fnR, inputTypes, inVals, TypeHandler.LongHandler, in, inputOIs, inSz,
        numPreceding, numFollowing, outVals);

  }

  public void sumHiveDecimal(Iterator<HiveDecimal> inVals, int inSz,
      int numPreceding, int numFollowing, Iterator<HiveDecimal> outVals)
      throws HiveException {

    GenericUDAFSum fnR = new GenericUDAFSum();
    TypeInfo[] inputTypes = { TypeInfoFactory.decimalTypeInfo };
    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector };

    HiveDecimalWritable[] in = new HiveDecimalWritable[1];
    in[0] = new HiveDecimalWritable();

    _agg(fnR, inputTypes, inVals, TypeHandler.HiveDecimalHandler, in, inputOIs,
        inSz, numPreceding, numFollowing, outVals);

  }

  static interface TypeHandler<T, TW> {
    public void set(T i, TW iw);

    public T get(TW iw);

    TypeHandler<Double, DoubleWritable> DoubleHandler = new TypeHandler<Double, DoubleWritable>() {
      public void set(Double d, DoubleWritable iw) {
        iw.set(d);
      }

      public Double get(DoubleWritable iw) {
        return iw.get();
      }
    };

    TypeHandler<Long, LongWritable> LongHandler = new TypeHandler<Long, LongWritable>() {
      public void set(Long d, LongWritable iw) {
        iw.set(d);
      }

      public Long get(LongWritable iw) {
        return iw.get();
      }
    };

    TypeHandler<HiveDecimal, HiveDecimalWritable> HiveDecimalHandler = new TypeHandler<HiveDecimal, HiveDecimalWritable>() {
      public void set(HiveDecimal d, HiveDecimalWritable iw) {
        iw.set(d);
      }

      public HiveDecimal get(HiveDecimalWritable iw) {
        return iw.getHiveDecimal();
      }
    };
  }

  public static <T, TW> void _agg(GenericUDAFResolver fnR,
      TypeInfo[] inputTypes, Iterator<T> inVals,
      TypeHandler<T, TW> typeHandler, TW[] in, ObjectInspector[] inputOIs,
      int inSz, int numPreceding, int numFollowing, Iterator<T> outVals)
      throws HiveException {

    GenericUDAFEvaluator fn = fnR.getEvaluator(inputTypes);
    fn.init(Mode.COMPLETE, inputOIs);
    fn = fn.getWindowingEvaluator(wdwFrame(numPreceding, numFollowing));
    AggregationBuffer agg = fn.getNewAggregationBuffer();
    ISupportStreamingModeForWindowing oS = (ISupportStreamingModeForWindowing) fn;

    int outSz = 0;
    while (inVals.hasNext()) {
      typeHandler.set(inVals.next(), in[0]);
      fn.aggregate(agg, in);
      Object out = oS.getNextResult(agg);
      if (out != null) {
        if ( out == ISupportStreamingModeForWindowing.NULL_RESULT ) {
          out = null;
        } else {
          try {
            out = typeHandler.get((TW) out);
          } catch(ClassCastException ce) {
          }
        }
        Assert.assertEquals(out, outVals.next());
        outSz++;
      }
    }

    fn.terminate(agg);

    while (outSz < inSz) {
      Object out = oS.getNextResult(agg);
      if ( out == ISupportStreamingModeForWindowing.NULL_RESULT ) {
        out = null;
      } else {
        try {
          out = typeHandler.get((TW) out);
        } catch(ClassCastException ce) {
        }
      }
      Assert.assertEquals(out, outVals.next());
      outSz++;
    }

  }

  @Test
  public void testDouble_3_4() throws HiveException {

    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(15.0, 21.0, 28.0, 36.0, 44.0, 52.0,
        49.0, 45.0, 40.0, 34.0);
    sumDouble(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testDouble_3_0() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(1.0, 3.0, 6.0, 10.0, 14.0, 18.0, 22.0,
        26.0, 30.0, 34.0);
    sumDouble(inVals.iterator(), 10, 3, 0, outVals.iterator());
  }

  @Test
  public void testDouble_unb_0() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(1.0, 3.0, 6.0, 10.0, 15.0, 21.0, 28.0,
        36.0, 45.0, 55.0);
    sumDouble(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 0,
        outVals.iterator());
  }

  @Test
  public void testDouble_0_5() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(21.0, 27.0, 33.0, 39.0, 45.0, 40.0,
        34.0, 27.0, 19.0, 10.0);
    sumDouble(inVals.iterator(), 10, 0, 5, outVals.iterator());
  }

  @Test
  public void testDouble_unb_5() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(21.0, 28.0, 36.0, 45.0, 55.0, 55.0,
        55.0, 55.0, 55.0, 55.0);
    sumDouble(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 5,
        outVals.iterator());
  }

  @Test
  public void testDouble_7_2() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(6.0, 10.0, 15.0, 21.0, 28.0, 36.0,
        45.0, 55.0, 54.0, 52.0);
    sumDouble(inVals.iterator(), 10, 7, 2, outVals.iterator());
  }

  @Test
  public void testDouble_15_15() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(55.0, 55.0, 55.0, 55.0, 55.0, 55.0,
        55.0, 55.0, 55.0, 55.0);
    sumDouble(inVals.iterator(), 10, 15, 15, outVals.iterator());
  }

  @Test
  public void testLong_3_4() throws HiveException {

    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(15L, 21L, 28L, 36L, 44L, 52L, 49L, 45L,
        40L, 34L);
    sumLong(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testLong_3_0() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(1L, 3L, 6L, 10L, 14L, 18L, 22L, 26L,
        30L, 34L);
    sumLong(inVals.iterator(), 10, 3, 0, outVals.iterator());
  }

  @Test
  public void testLong_unb_0() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(1L, 3L, 6L, 10L, 15L, 21L, 28L, 36L,
        45L, 55L);
    sumLong(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 0,
        outVals.iterator());
  }

  @Test
  public void testLong_0_5() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(21L, 27L, 33L, 39L, 45L, 40L, 34L, 27L,
        19L, 10L);
    sumLong(inVals.iterator(), 10, 0, 5, outVals.iterator());
  }

  @Test
  public void testLong_unb_5() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(21L, 28L, 36L, 45L, 55L, 55L, 55L, 55L,
        55L, 55L);
    sumLong(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 5,
        outVals.iterator());
  }

  @Test
  public void testLong_7_2() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(6L, 10L, 15L, 21L, 28L, 36L, 45L, 55L,
        54L, 52L);
    sumLong(inVals.iterator(), 10, 7, 2, outVals.iterator());
  }

  @Test
  public void testLong_15_15() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(55L, 55L, 55L, 55L, 55L, 55L, 55L, 55L,
        55L, 55L);
    sumLong(inVals.iterator(), 10, 15, 15, outVals.iterator());
  }

  @Test
  public void testHiveDecimal_3_4() throws HiveException {

    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(15L),
        HiveDecimal.create(21L), HiveDecimal.create(28L),
        HiveDecimal.create(36L), HiveDecimal.create(44L),
        HiveDecimal.create(52L), HiveDecimal.create(49L),
        HiveDecimal.create(45L), HiveDecimal.create(40L),
        HiveDecimal.create(34L));
    sumHiveDecimal(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testHiveDecimal_3_0() throws HiveException {
    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(1L),
        HiveDecimal.create(3L), HiveDecimal.create(6L),
        HiveDecimal.create(10L), HiveDecimal.create(14L),
        HiveDecimal.create(18L), HiveDecimal.create(22L),
        HiveDecimal.create(26L), HiveDecimal.create(30L),
        HiveDecimal.create(34L));
    sumHiveDecimal(inVals.iterator(), 10, 3, 0, outVals.iterator());
  }

  @Test
  public void testHiveDecimal_unb_0() throws HiveException {
    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(1L),
        HiveDecimal.create(3L), HiveDecimal.create(6L),
        HiveDecimal.create(10L), HiveDecimal.create(15L),
        HiveDecimal.create(21L), HiveDecimal.create(28L),
        HiveDecimal.create(36L), HiveDecimal.create(45L),
        HiveDecimal.create(55L));
    sumHiveDecimal(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 0,
        outVals.iterator());
  }

  @Test
  public void testHiveDecimal_0_5() throws HiveException {
    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(21L),
        HiveDecimal.create(27L), HiveDecimal.create(33L),
        HiveDecimal.create(39L), HiveDecimal.create(45L),
        HiveDecimal.create(40L), HiveDecimal.create(34L),
        HiveDecimal.create(27L), HiveDecimal.create(19L),
        HiveDecimal.create(10L));
    sumHiveDecimal(inVals.iterator(), 10, 0, 5, outVals.iterator());
  }

  @Test
  public void testHiveDecimal_unb_5() throws HiveException {
    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(21L),
        HiveDecimal.create(28L), HiveDecimal.create(36L),
        HiveDecimal.create(45L), HiveDecimal.create(55L),
        HiveDecimal.create(55L), HiveDecimal.create(55L),
        HiveDecimal.create(55L), HiveDecimal.create(55L),
        HiveDecimal.create(55L));
    sumHiveDecimal(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 5,
        outVals.iterator());
  }

  @Test
  public void testHiveDecimal_7_2() throws HiveException {
    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(6L),
        HiveDecimal.create(10L), HiveDecimal.create(15L),
        HiveDecimal.create(21L), HiveDecimal.create(28L),
        HiveDecimal.create(36L), HiveDecimal.create(45L),
        HiveDecimal.create(55L), HiveDecimal.create(54L),
        HiveDecimal.create(52L));
    sumHiveDecimal(inVals.iterator(), 10, 7, 2, outVals.iterator());
  }

  @Test
  public void testHiveDecimal_15_15() throws HiveException {
    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(HiveDecimal.create(55L),
        HiveDecimal.create(55L), HiveDecimal.create(55L),
        HiveDecimal.create(55L), HiveDecimal.create(55L),
        HiveDecimal.create(55L), HiveDecimal.create(55L),
        HiveDecimal.create(55L), HiveDecimal.create(55L),
        HiveDecimal.create(55L));
    sumHiveDecimal(inVals.iterator(), 10, 15, 15, outVals.iterator());
  }

}
