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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestGenericUDFWhenStatEstimator {

  private static ObjectInspector booleanOI =
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

  private static ObjectInspector stringConstant(String value) {
    return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        TypeInfoFactory.stringTypeInfo, value == null ? null : new Text(value));
  }

  private static ObjectInspector stringColumn() {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  private static ColStatistics colStats(long ndv) {
    ColStatistics cs = new ColStatistics("col", "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(0);
    return cs;
  }

  @Test
  public void testAllDistinctConstants() throws Exception {
    // CASE WHEN cond THEN 'A' WHEN cond THEN 'B' ELSE 'C' END
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = {
        booleanOI, stringConstant("A"),
        booleanOI, stringConstant("B"),
        stringConstant("C")
    };
    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    List<ColStatistics> argStats = Arrays.asList(
        colStats(2), colStats(1),
        colStats(2), colStats(1),
        colStats(1)
    );

    Optional<ColStatistics> result = estimator.estimate(argStats);
    assertTrue(result.isPresent());
    assertEquals(3, result.get().getCountDistint());
  }

  @Test
  public void testDuplicateConstants() throws Exception {
    // CASE WHEN cond THEN 'A' WHEN cond THEN 'A' ELSE 'B' END
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = {
        booleanOI, stringConstant("A"),
        booleanOI, stringConstant("A"),
        stringConstant("B")
    };
    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    List<ColStatistics> argStats = Arrays.asList(
        colStats(2), colStats(1),
        colStats(2), colStats(1),
        colStats(1)
    );

    Optional<ColStatistics> result = estimator.estimate(argStats);
    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  public void testConstantsWithNullElse() throws Exception {
    // CASE WHEN cond THEN 'A' WHEN cond THEN 'B' ELSE NULL END
    // NULL constant has NDV=0 from StatsUtils, but we have 2 non-null constants
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = {
        booleanOI, stringConstant("A"),
        booleanOI, stringConstant("B"),
        stringConstant(null)
    };
    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    List<ColStatistics> argStats = Arrays.asList(
        colStats(2), colStats(1),
        colStats(2), colStats(1),
        colStats(0)  // NULL constant gets NDV=0
    );

    Optional<ColStatistics> result = estimator.estimate(argStats);
    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  public void testConstantsFloorOverColumnNdv() throws Exception {
    // CASE WHEN cond THEN 'A' WHEN cond THEN 'B' WHEN cond THEN 'C' ELSE column END
    // 3 constants should floor NDV even if column has lower NDV
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = {
        booleanOI, stringConstant("A"),
        booleanOI, stringConstant("B"),
        booleanOI, stringConstant("C"),
        stringColumn()
    };
    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    List<ColStatistics> argStats = Arrays.asList(
        colStats(2), colStats(1),
        colStats(2), colStats(1),
        colStats(2), colStats(1),
        colStats(2)  // column with NDV=2
    );

    Optional<ColStatistics> result = estimator.estimate(argStats);
    assertTrue(result.isPresent());
    assertEquals(3, result.get().getCountDistint());
  }

  @Test
  public void testManyConstantsWithNullElse() throws Exception {
    // CASE with 20 distinct constants + NULL ELSE
    // Should return NDV=20, not NDV=1 (the bug we're fixing)
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = new ObjectInspector[41]; // 20 pairs + 1 else
    ColStatistics[] stats = new ColStatistics[41];

    for (int i = 0; i < 20; i++) {
      args[i * 2] = booleanOI;
      args[i * 2 + 1] = stringConstant(String.valueOf((char) ('A' + i)));
      stats[i * 2] = colStats(2);
      stats[i * 2 + 1] = colStats(1);
    }
    args[40] = stringConstant(null);
    stats[40] = colStats(0);  // NULL gets NDV=0

    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stats));

    assertTrue(result.isPresent());
    assertEquals(20, result.get().getCountDistint());
  }

  @Test
  public void testNoElseClause() throws Exception {
    // CASE WHEN cond THEN 'A' WHEN cond THEN 'B' END (implicit NULL else)
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = {
        booleanOI, stringConstant("A"),
        booleanOI, stringConstant("B")
    };
    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    List<ColStatistics> argStats = Arrays.asList(
        colStats(2), colStats(1),
        colStats(2), colStats(1)
    );

    Optional<ColStatistics> result = estimator.estimate(argStats);
    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

  @Test
  public void testNullInThenBranch() throws Exception {
    // CASE WHEN cond THEN NULL WHEN cond THEN 'A' ELSE 'B' END
    GenericUDFWhen udf = new GenericUDFWhen();
    ObjectInspector[] args = {
        booleanOI, stringConstant(null),
        booleanOI, stringConstant("A"),
        stringConstant("B")
    };
    udf.initialize(args);

    StatEstimator estimator = udf.getStatEstimator();
    List<ColStatistics> argStats = Arrays.asList(
        colStats(2), colStats(0),
        colStats(2), colStats(1),
        colStats(1)
    );

    Optional<ColStatistics> result = estimator.estimate(argStats);
    assertTrue(result.isPresent());
    assertEquals(2, result.get().getCountDistint());
  }

}
