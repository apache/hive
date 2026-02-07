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

package org.apache.hadoop.hive.ql.udf.generic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Optional;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

class TestGenericUDFCoalesceStatEstimator {

  @Test
  void testAllArgumentsConstant() throws UDFArgumentTypeException {
    // COALESCE('A', 'B', 'C') - first constant 'A' is always returned
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{constA, constB, constC});

    StatEstimator estimator = udf.getStatEstimator();

    ColStatistics arg1Stats = createColStats("arg1", 1, 0);
    arg1Stats.setAvgColLen(5.0);
    ColStatistics arg2Stats = createColStats("arg2", 1, 0);
    arg2Stats.setAvgColLen(25.0);
    ColStatistics arg3Stats = createColStats("arg3", 1, 0);
    arg3Stats.setAvgColLen(15.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(arg1Stats, arg2Stats, arg3Stats));

    assertTrue(result.isPresent());
    assertEquals(1, result.get().getCountDistint());
    assertEquals(5.0, result.get().getAvgColLen());
  }

  @Test
  void testSingleConstantArgument() throws UDFArgumentTypeException {
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));

    udf.initialize(new ObjectInspector[]{constA});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("arg1", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(1, result.get().getCountDistint());
  }

  @Test
  void testMixedConstantAndNonConstantArguments() throws UDFArgumentTypeException {
    // COALESCE('A', nonConst, 'C') - first arg is constant 'A', always returned, NDV = 1
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{constA, nonConst, constC});

    StatEstimator estimator = udf.getStatEstimator();

    // Constants have NDV=1, non-constants have their actual NDV
    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("constA", 1, 0),
        createColStats("col", 200, 20),
        createColStats("constC", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(1, result.get().getCountDistint());
  }

  @Test
  void testAllNonConstantArguments() throws UDFArgumentTypeException {
    // COALESCE(col1, col2, col3) - no constants, NDV = max of all columns
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{nonConst, nonConst, nonConst});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("arg1", 100, 10),
        createColStats("arg2", 200, 20),
        createColStats("arg3", 300, 30)));

    assertTrue(result.isPresent());
    assertEquals(300, result.get().getCountDistint());
  }

  @Test
  void testColumnThenConstant() throws UDFArgumentTypeException {
    // COALESCE(col, 'default') - returns col values OR 'default', NDV = NDV(col) + 1
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector constDefault = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("default"));

    udf.initialize(new ObjectInspector[]{nonConst, constDefault});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("col", 100, 10),
        createColStats("const", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(101, result.get().getCountDistint());
  }

  @Test
  void testMultipleColumnsThenConstant() throws UDFArgumentTypeException {
    // COALESCE(col1, col2, 'default') - returns col1, col2, or 'default', NDV = max(col1, col2) + 1
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector constDefault = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("default"));

    udf.initialize(new ObjectInspector[]{nonConst, nonConst, constDefault});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("col1", 100, 10),
        createColStats("col2", 200, 20),
        createColStats("const", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(201, result.get().getCountDistint());
  }

  private ColStatistics createColStats(String name, long ndv, long numNulls) {
    ColStatistics cs = new ColStatistics(name, "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(numNulls);
    cs.setAvgColLen(10.0);
    return cs;
  }
}
