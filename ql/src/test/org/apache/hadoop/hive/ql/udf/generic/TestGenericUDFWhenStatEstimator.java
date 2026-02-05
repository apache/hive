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

class TestGenericUDFWhenStatEstimator {

  @Test
  void testAllBranchesConstantDistinctValues() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("else", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(3, result.get().getCountDistint());
  }

  @Test
  void testAllBranchesConstantWithDuplicates() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constA2 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constA2, constB});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("else", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  void testWithoutElseBranch() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  void testMixedConstantAndNonConstantBranches() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, nonConst, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20),
        createColStats("else", 300, 30)));

    assertTrue(result.isPresent());
    assertEquals(300, result.get().getCountDistint());
  }

  @Test
  void testNonConstantElseWithConstantBranches() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, nonConst});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20),
        createColStats("else", 300, 30)));

    assertTrue(result.isPresent());
    assertEquals(300, result.get().getCountDistint());
  }

  @Test
  void testConstantsWithComplexExpression() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector complexExprOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, complexExprOI, constB});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("complex_expr", 500, 0),
        createColStats("else", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(500, result.get().getCountDistint());
  }

  @Test
  void testConstantsWithSmallComplexExpression() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));
    ObjectInspector complexExprOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, boolOI, complexExprOI, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("cond3", 2, 0),
        createColStats("complex_expr", 2, 0),
        createColStats("else", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(3, result.get().getCountDistint());
  }

  @Test
  void testConstantsWithUnknownNdvColumn() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, nonConst});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("else", 0, 0)));

    assertTrue(result.isPresent());
    assertEquals(0, result.get().getCountDistint());
  }

  @Test
  void testManyConstantsWithSmallNdvColumn() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));
    ObjectInspector constD = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("D"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, boolOI, constC, boolOI, constD, nonConst});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("cond3", 2, 0),
        createColStats("then3", 1, 0),
        createColStats("cond4", 2, 0),
        createColStats("then4", 1, 0),
        createColStats("else", 2, 0)));

    assertTrue(result.isPresent());
    assertEquals(4, result.get().getCountDistint());
  }

  @Test
  void testAllBranchesNonConstant() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{boolOI, nonConst, boolOI, nonConst, nonConst});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("col1", 100, 0),
        createColStats("cond2", 2, 0),
        createColStats("col2", 200, 0),
        createColStats("col3", 300, 0)));

    assertTrue(result.isPresent());
    assertEquals(300, result.get().getCountDistint());
  }

  @Test
  void testNullConstantWithNonNullConstants() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constNull = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, null);
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constNull2 = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, null);

    udf.initialize(new ObjectInspector[]{boolOI, constNull, boolOI, constA, constNull2});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("else", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  void testAllNullConstants() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constNull = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, null);

    udf.initialize(new ObjectInspector[]{boolOI, constNull, boolOI, constNull, constNull});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 1, 0),
        createColStats("cond2", 2, 0),
        createColStats("then2", 1, 0),
        createColStats("else", 1, 0)));

    assertTrue(result.isPresent());
    assertEquals(1, result.get().getCountDistint());
  }

  @Test
  void testConstantBranchesTakesMaxAvgColLen() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, constC});

    StatEstimator estimator = udf.getStatEstimator();

    ColStatistics then1Stats = createColStats("then1", 100, 10);
    then1Stats.setAvgColLen(5.0);
    ColStatistics then2Stats = createColStats("then2", 200, 20);
    then2Stats.setAvgColLen(25.0);
    ColStatistics elseStats = createColStats("else", 300, 30);
    elseStats.setAvgColLen(15.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        then1Stats,
        createColStats("cond2", 2, 0),
        then2Stats,
        elseStats));

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
