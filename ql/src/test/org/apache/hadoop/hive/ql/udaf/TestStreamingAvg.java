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

package org.apache.hadoop.hive.ql.udaf;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.udaf.TestStreamingSum.TypeHandler;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

public class TestStreamingAvg {

  public void avgDouble(Iterator<Double> inVals, int inSz, int numPreceding,
      int numFollowing, Iterator<Double> outVals) throws HiveException {

    GenericUDAFAverage fnR = new GenericUDAFAverage();
    TypeInfo[] inputTypes = { TypeInfoFactory.doubleTypeInfo };
    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableDoubleObjectInspector };

    DoubleWritable[] in = new DoubleWritable[1];
    in[0] = new DoubleWritable();

    TestStreamingSum._agg(fnR, inputTypes, inVals, TypeHandler.DoubleHandler,
        in, inputOIs, inSz, numPreceding, numFollowing, outVals);

  }

  public void avgHiveDecimal(Iterator<HiveDecimal> inVals, int inSz,
      int numPreceding, int numFollowing, Iterator<HiveDecimal> outVals)
      throws HiveException {

    GenericUDAFAverage fnR = new GenericUDAFAverage();
    TypeInfo[] inputTypes = { TypeInfoFactory.decimalTypeInfo };
    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector };

    HiveDecimalWritable[] in = new HiveDecimalWritable[1];
    in[0] = new HiveDecimalWritable();

    TestStreamingSum._agg(fnR, inputTypes, inVals,
        TypeHandler.HiveDecimalHandler, in, inputOIs, inSz, numPreceding,
        numFollowing, outVals);

  }

  @Test
  public void testDouble_3_4() throws HiveException {

    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(15.0 / 5, 21.0 / 6, 28.0 / 7,
        36.0 / 8, 44.0 / 8, 52.0 / 8, 49.0 / 7, 45.0 / 6, 40.0 / 5, 34.0 / 4);
    avgDouble(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testHiveDecimal_3_4() throws HiveException {

    List<HiveDecimal> inVals = Arrays
        .asList(HiveDecimal.create(1L), HiveDecimal.create(2L),
            HiveDecimal.create(3L), HiveDecimal.create(4L),
            HiveDecimal.create(5L), HiveDecimal.create(6L),
            HiveDecimal.create(7L), HiveDecimal.create(8L),
            HiveDecimal.create(9L), HiveDecimal.create(10L));
    List<HiveDecimal> outVals = Arrays.asList(
        HiveDecimal.create(new BigDecimal(15.0 / 5)),
        HiveDecimal.create(new BigDecimal(21.0 / 6)),
        HiveDecimal.create(new BigDecimal(28.0 / 7)),
        HiveDecimal.create(new BigDecimal(36.0 / 8)),
        HiveDecimal.create(new BigDecimal(44.0 / 8)),
        HiveDecimal.create(new BigDecimal(52.0 / 8)),
        HiveDecimal.create(new BigDecimal(49.0 / 7)),
        HiveDecimal.create(new BigDecimal(45.0 / 6)),
        HiveDecimal.create(new BigDecimal(40.0 / 5)),
        HiveDecimal.create(new BigDecimal(34.0 / 4)));
    avgHiveDecimal(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testDouble_3_0() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(1.0 / 1, 3.0 / 2, 6.0 / 3, 10.0 / 4,
        14.0 / 4, 18.0 / 4, 22.0 / 4, 26.0 / 4, 30.0 / 4, 34.0 / 4);
    avgDouble(inVals.iterator(), 10, 3, 0, outVals.iterator());
  }

  @Test
  public void testDouble_unb_0() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(1.0 / 1, 3.0 / 2, 6.0 / 3, 10.0 / 4,
        15.0 / 5, 21.0 / 6, 28.0 / 7, 36.0 / 8, 45.0 / 9, 55.0 / 10);
    avgDouble(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 0,
        outVals.iterator());
  }

  @Test
  public void testDouble_0_5() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(21.0 / 6, 27.0 / 6, 33.0 / 6,
        39.0 / 6, 45.0 / 6, 40.0 / 5, 34.0 / 4, 27.0 / 3, 19.0 / 2, 10.0 / 1);
    avgDouble(inVals.iterator(), 10, 0, 5, outVals.iterator());
  }

  @Test
  public void testDouble_unb_5() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(21.0 / 6, 28.0 / 7, 36.0 / 8,
        45.0 / 9, 55.0 / 10, 55.0 / 10, 55.0 / 10, 55.0 / 10, 55.0 / 10,
        55.0 / 10);
    avgDouble(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 5,
        outVals.iterator());
  }

  @Test
  public void testDouble_7_2() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(6.0 / 3, 10.0 / 4, 15.0 / 5, 21.0 / 6,
        28.0 / 7, 36.0 / 8, 45.0 / 9, 55.0 / 10, 54.0 / 9, 52.0 / 8);
    avgDouble(inVals.iterator(), 10, 7, 2, outVals.iterator());
  }

  @Test
  public void testDouble_15_15() throws HiveException {
    List<Double> inVals = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
        9.0, 10.0);
    List<Double> outVals = Arrays.asList(55.0 / 10, 55.0 / 10, 55.0 / 10,
        55.0 / 10, 55.0 / 10, 55.0 / 10, 55.0 / 10, 55.0 / 10, 55.0 / 10,
        55.0 / 10);
    avgDouble(inVals.iterator(), 10, 15, 15, outVals.iterator());
  }
}
