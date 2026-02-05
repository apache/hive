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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

class TestGenericUDFIfStatEstimator {

  @Test
  void testBothBranchesConstantDistinctValues() throws UDFArgumentException {
    GenericUDFIf udf = new GenericUDFIf();

    ObjectInspector conditionOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector thenOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector elseOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));

    udf.initialize(new ObjectInspector[]{conditionOI, thenOI, elseOI});

    StatEstimator estimator = udf.getStatEstimator();
    ColStatistics thenStats = createColStats("then_col", 100, 10);
    ColStatistics elseStats = createColStats("else_col", 200, 20);

    Optional<ColStatistics> result = estimator.estimate(
        Arrays.asList(createColStats("cond", 2, 0), thenStats, elseStats));

    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  void testBothBranchesConstantSameValue() throws UDFArgumentException {
    GenericUDFIf udf = new GenericUDFIf();

    ObjectInspector conditionOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector thenOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector elseOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));

    udf.initialize(new ObjectInspector[]{conditionOI, thenOI, elseOI});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(
        Arrays.asList(createColStats("cond", 2, 0),
            createColStats("then_col", 100, 10),
            createColStats("else_col", 200, 20)));

    assertTrue(result.isPresent());
    assertEquals(1, result.get().getCountDistint());
  }

  @Test
  void testNonConstantThenBranchFallsBackToPessimisticCombiner() throws UDFArgumentException {
    GenericUDFIf udf = new GenericUDFIf();

    ObjectInspector conditionOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector thenOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector elseOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));

    udf.initialize(new ObjectInspector[]{conditionOI, thenOI, elseOI});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(
        Arrays.asList(createColStats("cond", 2, 0),
            createColStats("then_col", 100, 10),
            createColStats("else_col", 200, 20)));

    assertTrue(result.isPresent());
    assertEquals(0, result.get().getCountDistint());
  }

  @Test
  void testNonConstantElseBranchFallsBackToPessimisticCombiner() throws UDFArgumentException {
    GenericUDFIf udf = new GenericUDFIf();

    ObjectInspector conditionOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector thenOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector elseOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    udf.initialize(new ObjectInspector[]{conditionOI, thenOI, elseOI});

    StatEstimator estimator = udf.getStatEstimator();

    Optional<ColStatistics> result = estimator.estimate(
        Arrays.asList(createColStats("cond", 2, 0),
            createColStats("then_col", 100, 10),
            createColStats("else_col", 200, 20)));

    assertTrue(result.isPresent());
    assertEquals(0, result.get().getCountDistint());
  }

  @Test
  void testConstantBranchesTakesMaxAvgColLen() throws UDFArgumentException {
    GenericUDFIf udf = new GenericUDFIf();

    ObjectInspector conditionOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector thenOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("A"));
    ObjectInspector elseOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, new Text("B"));

    udf.initialize(new ObjectInspector[]{conditionOI, thenOI, elseOI});

    StatEstimator estimator = udf.getStatEstimator();
    ColStatistics thenStats = createColStats("then_col", 100, 10);
    thenStats.setAvgColLen(5.0);
    ColStatistics elseStats = createColStats("else_col", 200, 20);
    elseStats.setAvgColLen(15.0);

    Optional<ColStatistics> result = estimator.estimate(
        Arrays.asList(createColStats("cond", 2, 0), thenStats, elseStats));

    assertTrue(result.isPresent());
    assertEquals(15.0, result.get().getAvgColLen());
  }

  private ColStatistics createColStats(String name, long ndv, long numNulls) {
    ColStatistics cs = new ColStatistics(name, "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(numNulls);
    cs.setAvgColLen(10.0);
    return cs;
  }
}
