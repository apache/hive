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
package org.apache.hive.benchmark.vectorization;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This test measures the performance for vectorization.
 * <p/>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p/>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedComparisonBench
 * <p/>
 * To use the default settings used by JMH, use:
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedComparisonBench
 * <p/>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p/>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedComparisonBench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class VectorizedComparisonBench {
  public static class LongColEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(), getLongColumnVector());
      expression = new LongColEqualLongColumn(0, 1, 2);
    }
  }

  public static class LongColGreaterEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(), getLongColumnVector());
      expression = new LongColGreaterEqualLongColumn(0, 1, 2);
    }
  }

  public static class LongColGreaterLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(), getLongColumnVector());
      expression = new LongColGreaterLongColumn(0, 1, 2);
    }
  }

  public static class LongColLessEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(), getLongColumnVector());
      expression = new LongColLessEqualLongColumn(0, 1, 2);
    }
  }

  public static class LongColLessLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(), getLongColumnVector());
      expression = new LongColLessLongColumn(0, 1, 2);
    }
  }

  public static class LongColNotEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(), getLongColumnVector());
      expression = new LongColNotEqualLongColumn(0, 1, 2);
    }
  }

  public static class LongColEqualLongScalarBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongColEqualLongScalar(0, 0, 1);
    }
  }

  public static class LongColGreaterEqualLongScalarBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongColGreaterEqualLongScalar(0, 0, 1);
    }
  }

  public static class LongColGreaterLongScalarBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongColGreaterLongScalar(0, 0, 1);
    }
  }

  public static class LongColLessEqualLongScalarBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongColLessEqualLongScalar(0, 0, 1);
    }
  }

  public static class LongColLessLongScalarBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongColLessLongScalar(0, 0, 1);
    }
  }

  public static class LongColNotEqualLongScalarBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongColNotEqualLongScalar(0, 0, 1);
    }
  }

  public static class LongScalarEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongScalarEqualLongColumn(0, 0, 1);
    }
  }

  public static class LongScalarGreaterEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongScalarGreaterEqualLongColumn(0, 0, 1);
    }
  }

  public static class LongScalarGreaterLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongScalarGreaterLongColumn(0, 0, 1);
    }
  }

  public static class LongScalarLessEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongScalarLessEqualLongColumn(0, 0, 1);
    }
  }

  public static class LongScalarLessLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongScalarLessLongColumn(0, 0, 1);
    }
  }

  public static class LongScalarNotEqualLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getLongColumnVector());
      expression = new LongScalarNotEqualLongColumn(0, 0, 1);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + VectorizedComparisonBench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }
}
