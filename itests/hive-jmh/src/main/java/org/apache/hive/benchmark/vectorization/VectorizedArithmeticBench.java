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

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumnChecked;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
 * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedArithmeticBench
 * <p/>
 * To use the default settings used by JMH, use:
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedArithmeticBench
 * <p/>
 * To specify different parameters, use:
 * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
 * display the Average Time (avgt) in Microseconds (us)
 * - Benchmark mode. Available modes are:
 * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
 * - Output time unit. Available time units are: [m, s, ms, us, ns].
 * <p/>
 * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizedArithmeticBench
 * -wi 10 -i 5 -f 2 -bm avgt -tu us
 */
@State(Scope.Benchmark)
public class VectorizedArithmeticBench {
  public static class DoubleColAddRepeatingDoubleColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, getDoubleColumnVector(),
          getRepeatingDoubleColumnVector());
      expression = new DoubleColAddDoubleColumn(0, 1, 2);
    }
  }

  public static class LongColAddRepeatingLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getRepeatingLongColumnVector());
      expression = new LongColAddLongColumn(0, 1, 2);
    }
  }

  public static class DoubleColDivideDoubleColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, getDoubleColumnVector(),
          getDoubleColumnVector());
      expression = new DoubleColDivideDoubleColumn(0, 1, 2);
    }
  }

  public static class DoubleColDivideRepeatingDoubleColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, getDoubleColumnVector(),
          getRepeatingDoubleColumnVector());
      expression = new DoubleColDivideDoubleColumn(0, 1, 2);
    }
  }

  public static class LongColDivideLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new LongColDivideLongColumn(0, 1, 2);
    }
  }

  public static class LongColDivideRepeatingLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, getLongColumnVector(),
          getRepeatingLongColumnVector());
      expression = new LongColDivideLongColumn(0, 1, 2);
    }
  }

  public static class LongColAddLongColumnCheckedBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new LongColAddLongColumnChecked(0, 1, 2);
      expression.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    }
  }

  public static class LongColAddLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new LongColAddLongColumn(0, 1, 2);
      expression.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + VectorizedArithmeticBench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }
}
