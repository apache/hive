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
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColAndCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColOrCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprLongColumnLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NotCol;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * This test measures the performance for vectorization.
 * <p>
 * This test uses JMH framework for benchmarking.
 * You may execute this benchmark tool using JMH command line in different ways:
 * <p>
 * To use the settings shown in the main() function, use:
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedLogicBench
 * <p>
 * To use the default settings used by JMH, use:
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedLogicBench
 * <p>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedLogicBench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class VectorizedLogicBench {

  public static class ColAndColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getBooleanLongColumnVector(),
          getBooleanLongColumnVector());
      expression = new ColAndCol(0, 1, 2);
    }
  }

  public static class ColAndRepeatingColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getBooleanLongColumnVector(),
          getBooleanRepeatingLongColumnVector());
      expression = new ColAndCol(0, 1, 2);
    }
  }

  public static class RepeatingColAndColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getBooleanRepeatingLongColumnVector(),
          getBooleanLongColumnVector());
      expression = new ColAndCol(0, 1, 2);
    }
  }

  public static class ColOrColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getBooleanLongColumnVector(),
          getBooleanLongColumnVector());
      expression = new ColOrCol(0, 1, 2);
    }
  }

  public static class ColOrRepeatingColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getBooleanLongColumnVector(),
          getBooleanRepeatingLongColumnVector());
      expression = new ColOrCol(0, 1, 2);
    }
  }

  public static class RepeatingColOrColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getBooleanRepeatingLongColumnVector(),
          getBooleanLongColumnVector());
      expression = new ColOrCol(0, 1, 2);
    }
  }

  public static class NotColBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 1, getBooleanLongColumnVector());
      expression = new NotCol(0, 1);
    }
  }

  public static class IfExprLongColumnLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 3, getBooleanLongColumnVector(),
          getLongColumnVector(), getLongColumnVector());
      expression = new IfExprLongColumnLongColumn(0, 1, 2, 3);
    }
  }

  public static class IfExprRepeatingLongColumnLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 3, getBooleanLongColumnVector(),
          getRepeatingLongColumnVector(), getLongColumnVector());
      expression = new IfExprLongColumnLongColumn(0, 1, 2, 3);
    }
  }

  public static class IfExprLongColumnRepeatingLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 3, getBooleanLongColumnVector(),
          getLongColumnVector(), getRepeatingLongColumnVector());
      expression = new IfExprLongColumnLongColumn(0, 1, 2, 3);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + VectorizedLogicBench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }
}