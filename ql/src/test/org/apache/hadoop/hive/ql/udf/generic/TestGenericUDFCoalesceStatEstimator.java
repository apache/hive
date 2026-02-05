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
  void testAllArgumentsConstantDistinctValues() throws UDFArgumentTypeException {
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{constA, constB, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("arg1", 1, 0),
        createColStats("arg2", 1, 0),
        createColStats("arg3", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(3, result.get().getCountDistint());
  }

  @Test
  void testAllArgumentsConstantWithDuplicates() throws UDFArgumentTypeException {
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constA2 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));

    udf.initialize(new ObjectInspector[]{constA, constA2, constB});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("arg1", 1, 0),
        createColStats("arg2", 1, 0),
        createColStats("arg3", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
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
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{constA, nonConst, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("arg1", 100, 10),
        createColStats("arg2", 200, 20),
        createColStats("arg3", 300, 30)));

    assertTrue(result.isPresent());
    assertEquals(300, result.get().getCountDistint());
  }

  @Test
  void testAllNonConstantArguments() throws UDFArgumentTypeException {
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
  void testConstantArgumentsTakesMaxAvgColLen() throws UDFArgumentTypeException {
    GenericUDFCoalesce udf = new GenericUDFCoalesce();

    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{constA, constB, constC});

    StatEstimator estimator = udf.getStatEstimator();

    ColStatistics arg1Stats = createColStats("arg1", 100, 10);
    arg1Stats.setAvgColLen(5.0);
    ColStatistics arg2Stats = createColStats("arg2", 200, 20);
    arg2Stats.setAvgColLen(25.0);
    ColStatistics arg3Stats = createColStats("arg3", 300, 30);
    arg3Stats.setAvgColLen(15.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(arg1Stats, arg2Stats, arg3Stats));

    assertTrue(result.isPresent());
    assertEquals(25.0, result.get().getAvgColLen());
  }

  private ColStatistics createColStats(String name, long ndv, long numNulls) {
    ColStatistics cs = new ColStatistics(name, "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(numNulls);
    cs.setAvgColLen(10.0);
    return cs;
  }
}
