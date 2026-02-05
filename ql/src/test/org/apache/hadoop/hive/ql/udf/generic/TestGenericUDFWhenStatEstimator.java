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

    // CASE WHEN cond1 THEN 'A' WHEN cond2 THEN 'B' ELSE 'C' END
    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20),
        createColStats("else", 300, 30)));

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

    // CASE WHEN cond1 THEN 'A' WHEN cond2 THEN 'A' ELSE 'B' END
    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constA2, constB});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20),
        createColStats("else", 300, 30)));

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

    // CASE WHEN cond1 THEN 'A' WHEN cond2 THEN 'B' END (no ELSE)
    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20)));

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  void testNonConstantBranchFallsBackToPessimisticCombiner() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector constC = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("C"));

    // CASE WHEN cond1 THEN 'A' WHEN cond2 THEN col ELSE 'C' END
    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, nonConst, constC});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20),
        createColStats("else", 300, 30)));

    assertTrue(result.isPresent());
    assertEquals(0, result.get().getCountDistint());
  }

  @Test
  void testNonConstantElseFallsBackToPessimisticCombiner() throws UDFArgumentTypeException {
    GenericUDFWhen udf = new GenericUDFWhen();

    ObjectInspector boolOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector constA = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector constB = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));
    ObjectInspector nonConst = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    // CASE WHEN cond1 THEN 'A' WHEN cond2 THEN 'B' ELSE col END
    udf.initialize(new ObjectInspector[]{boolOI, constA, boolOI, constB, nonConst});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats("cond1", 2, 0),
        createColStats("then1", 100, 10),
        createColStats("cond2", 2, 0),
        createColStats("then2", 200, 20),
        createColStats("else", 300, 30)));

    assertTrue(result.isPresent());
    assertEquals(0, result.get().getCountDistint());
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
