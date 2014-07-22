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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.udaf.TestStreamingSum.TypeHandler;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class TestStreamingMax {

  public void maxLong(Iterator<Long> inVals, int inSz, int numPreceding,
      int numFollowing, Iterator<Long> outVals) throws HiveException {

    GenericUDAFMax fnR = new GenericUDAFMax();
    TypeInfo[] inputTypes = { TypeInfoFactory.longTypeInfo };
    ObjectInspector[] inputOIs = { PrimitiveObjectInspectorFactory.writableLongObjectInspector };

    LongWritable[] in = new LongWritable[1];
    in[0] = new LongWritable();

    TestStreamingSum._agg(fnR, inputTypes, inVals, TypeHandler.LongHandler, in,
        inputOIs, inSz, numPreceding, numFollowing, outVals);

  }

  @Test
  public void testLong_3_4() throws HiveException {

    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(5L, 6L, 7L, 8L, 9L, 10L, 10L, 10L, 10L,
        10L);
    maxLong(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testLongr_3_4() throws HiveException {

    List<Long> inVals = Arrays.asList(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    List<Long> outVals = Arrays.asList(10L, 10L, 10L, 10L, 9L, 8L, 7L, 6L, 5L,
        4L);
    maxLong(inVals.iterator(), 10, 3, 4, outVals.iterator());
  }

  @Test
  public void testLongr_1_4() throws HiveException {

    List<Long> inVals = Arrays.asList(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    List<Long> outVals = Arrays
        .asList(10L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L);
    maxLong(inVals.iterator(), 10, 1, 4, outVals.iterator());
  }

  @Test
  public void testLong_3_0() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    maxLong(inVals.iterator(), 10, 3, 0, outVals.iterator());
  }

  @Test
  public void testLong_0_5() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(6L, 7L, 8L, 9L, 10L, 10L, 10L, 10L, 10L,
        10L);
    maxLong(inVals.iterator(), 10, 0, 5, outVals.iterator());
  }

  @Test
  public void testLong_7_2() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 10L,
        10L);
    maxLong(inVals.iterator(), 10, 7, 2, outVals.iterator());
  }

  @Test
  public void testLongr_7_2() throws HiveException {
    List<Long> inVals = Arrays.asList(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L);
    List<Long> outVals = Arrays.asList(10L, 10L, 10L, 10L, 10L, 10L, 10L, 10L,
        9L, 8L);
    maxLong(inVals.iterator(), 10, 7, 2, outVals.iterator());
  }

  @Test
  public void testLong_15_15() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(10L, 10L, 10L, 10L, 10L, 10L, 10L, 10L,
        10L, 10L);
    maxLong(inVals.iterator(), 10, 15, 15, outVals.iterator());
  }

  @Test
  public void testLong_unb_0() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    maxLong(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 0,
        outVals.iterator());
  }

  @Test
  public void testLong_unb_5() throws HiveException {
    List<Long> inVals = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    List<Long> outVals = Arrays.asList(6L, 7L, 8L, 9L, 10L, 10L, 10L, 10L, 10L,
        10L);
    maxLong(inVals.iterator(), 10, BoundarySpec.UNBOUNDED_AMOUNT, 5,
        outVals.iterator());
  }
}
