/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class ValueBoundaryScannerBench {
  private static final long INNER_ITERATIONS = 50000L;

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public static class BaseBench {
    private PTFExpressionDef expressionDef = new PTFExpressionDef();
    byte[] epochBytes = new TimestampWritableV2(new Timestamp()).getBytes();

    @Setup
    public void setup() throws Exception {
      expressionDef.setOI(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
    }

    @Benchmark
    @Warmup(iterations = 1, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testTimestampEqualsWithInspector() {
      TimestampWritableV2 v1 = new TimestampWritableV2(epochBytes, 0);
      TimestampWritableV2 v2 = new TimestampWritableV2(epochBytes, 0);

      // prevalidate before heavy loop
      Timestamp l1 = PrimitiveObjectInspectorUtils.getTimestamp(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      Timestamp l2 = PrimitiveObjectInspectorUtils.getTimestamp(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      if (!l1.equals(l2)) {
        throw new RuntimeException("timestamps are not equal in bench as expected");
      }

      for (int j = 0; j < INNER_ITERATIONS; j++) {
        l1 = PrimitiveObjectInspectorUtils.getTimestamp(v1,
            (PrimitiveObjectInspector) expressionDef.getOI());
        l2 = PrimitiveObjectInspectorUtils.getTimestamp(v2,
            (PrimitiveObjectInspector) expressionDef.getOI());
        l1.equals(l2);
      }
    }

    @Benchmark
    @Warmup(iterations = 1, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testTimestampEqualsWithoutInspector() {
      TimestampWritableV2 v1 = new TimestampWritableV2(epochBytes, 0);
      TimestampWritableV2 v2 = new TimestampWritableV2(epochBytes, 0);

      // prevalidate before heavy loop
      TimestampWritableV2 w1 = (TimestampWritableV2) v1;
      TimestampWritableV2 w2 = (TimestampWritableV2) v2;
      if (!w1.equals(w2)) {
        throw new RuntimeException("timestamps are not equal in bench as expected");
      }

      for (int j = 0; j < INNER_ITERATIONS; j++) {
        TimestampWritableV2 w1check = (TimestampWritableV2) v1;
        TimestampWritableV2 w2check = (TimestampWritableV2) v2;
        w1check.equals(w2check);
      }
    }

    @Benchmark
    @Warmup(iterations = 1, time = 2, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
    public void testTimestampEqualsWithoutInspectorWithTypeCheck() {
      TimestampWritableV2 v1 = new TimestampWritableV2(epochBytes, 0);
      TimestampWritableV2 v2 = new TimestampWritableV2(epochBytes, 0);

      // prevalidate before heavy loop
      TimestampWritableV2 w1 = (TimestampWritableV2) v1;
      TimestampWritableV2 w2 = (TimestampWritableV2) v2;
      if (!w1.equals(w2)) {
        throw new RuntimeException("timestamps are not equal in bench as expected");
      }

      for (int j = 0; j < INNER_ITERATIONS; j++) {
        if (v1 instanceof TimestampWritableV2 && v2 instanceof TimestampWritableV2) {
          TimestampWritableV2 w1check = (TimestampWritableV2) v1;
          TimestampWritableV2 w2check = (TimestampWritableV2) v2;
          w1check.equals(w2check);
        }
      }
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + ValueBoundaryScannerBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}