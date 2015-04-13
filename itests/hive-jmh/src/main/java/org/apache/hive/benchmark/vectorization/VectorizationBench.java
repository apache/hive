/**
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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColDivideDoubleColumn;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class VectorizationBench {
  /**
   * This test measures the performance for vectorization.
   * <p/>
   * This test uses JMH framework for benchmarking.
   * You may execute this benchmark tool using JMH command line in different ways:
   * <p/>
   * To use the settings shown in the main() function, use:
   * $ java -cp target/benchmarks.jar org.apache.hive.benchmark.vectorization.VectorizationBench
   * <p/>
   * To use the default settings used by JMH, use:
   * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization VectorizationBench
   * <p/>
   * To specify different parameters, use:
   * - This command will use 10 warm-up iterations, 5 test iterations, and 2 forks. And it will
   * display the Average Time (avgt) in Microseconds (us)
   * - Benchmark mode. Available modes are:
   * [Throughput/thrpt, AverageTime/avgt, SampleTime/sample, SingleShotTime/ss, All/all]
   * - Output time unit. Available time units are: [m, s, ms, us, ns].
   * <p/>
   * $ java -jar target/benchmarks.jar org.apache.hive.benchmark.vectorization VectorizationBench
   * -wi 10 -i 5 -f 2 -bm avgt -tu us
   */
  private static LongColumnVector longColumnVector = new LongColumnVector();
  private static LongColumnVector dupLongColumnVector = new LongColumnVector();
  private static DoubleColumnVector doubleColumnVector = new DoubleColumnVector();
  private static DoubleColumnVector dupDoubleColumnVector = new DoubleColumnVector();

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static abstract class AbstractExpression {
    protected VectorExpression expression;
    protected VectorizedRowBatch rowBatch;

    protected VectorizedRowBatch buildRowBatch(ColumnVector output, int colNum, ColumnVector...
        cols) {
      VectorizedRowBatch rowBatch = new VectorizedRowBatch(colNum + 1);
      for (int i = 0; i < cols.length; i++) {
        rowBatch.cols[i] = cols[i];
      }
      rowBatch.cols[colNum] = output;
      return rowBatch;
    }

    @Setup
    public abstract void setup();

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {
      expression.evaluate(rowBatch);
    }
  }

  public static class DoubleAddDoubleExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, doubleColumnVector,
          dupDoubleColumnVector);
      expression = new DoubleColAddDoubleColumn(0, 1, 2);
    }
  }

  public static class LongAddLongExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, longColumnVector, dupLongColumnVector);
      expression = new LongColAddLongColumn(0, 1, 2);
    }
  }

  public static class LongAddDoubleExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, longColumnVector, doubleColumnVector);
      expression = new LongColAddDoubleColumn(0, 1, 2);
    }
  }

  public static class DoubleAddLongExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, doubleColumnVector, longColumnVector);
      expression = new DoubleColAddLongColumn(0, 1, 2);
    }
  }

  public static class DoubleDivideDoubleExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, doubleColumnVector,
          dupDoubleColumnVector);
      expression = new DoubleColDivideDoubleColumn(0, 1, 2);
    }
  }

  public static class LongDivideLongExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, longColumnVector,
          dupLongColumnVector);
      expression = new LongColDivideLongColumn(0, 1, 2);
    }
  }

  public static class DoubleDivideLongExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, doubleColumnVector,
          longColumnVector);
      expression = new DoubleColDivideLongColumn(0, 1, 2);
    }
  }

  public static class LongDivideDoubleExpr extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DoubleColumnVector(), 2, longColumnVector,
          doubleColumnVector);
      expression = new LongColDivideDoubleColumn(0, 1, 2);
    }
  }

  @Setup(Level.Trial)
  public void initialColumnVectors() {
    Random random = new Random();

    dupLongColumnVector.fill(random.nextLong());
    dupDoubleColumnVector.fill(random.nextDouble());
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      doubleColumnVector.vector[i] = random.nextDouble();
      longColumnVector.vector[i] = random.nextLong();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + VectorizationBench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }
}