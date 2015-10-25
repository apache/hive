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
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprLongColumnLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.IfExprDoubleColumnDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColAndCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ColOrCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NotCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColLessEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColLessLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColGreaterEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColLessEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColLessLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColNotEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarLessEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarLessLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalarNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.LongColDivideLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColDivideDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
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

import java.util.Arrays;
import java.lang.Override;
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

  @BenchmarkMode(Mode.AverageTime)
  @Fork(1)
  @State(Scope.Thread)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static abstract class AbstractExpression {
    private static final int DEFAULT_ITER_TIME = 1000000;
    protected VectorExpression expression;
    protected VectorizedRowBatch rowBatch;

    protected VectorizedRowBatch buildRowBatch(ColumnVector output, int colNum, ColumnVector...
      cols) {
      VectorizedRowBatch rowBatch = new VectorizedRowBatch(colNum + 1);
      for (int i = 0; i < cols.length; i++) {
        rowBatch.cols[i] = cols[i];
      }
      rowBatch.cols[colNum] = output;
//      rowBatch.selectedInUse = true;
      return rowBatch;
    }

    @Setup
    public abstract void setup();

    @Benchmark
    @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
    public void bench() {
      for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
        expression.evaluate(rowBatch);
      }
    }

    protected LongColumnVector getLongColumnVector() {
      LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      Random random = new Random();
      for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
        columnVector.vector[i] = random.nextLong();
      }
      return columnVector;
    }

    protected LongColumnVector getRepeatingLongColumnVector() {
      LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      columnVector.fill(2);
      return columnVector;
    }

    protected LongColumnVector getLongColumnVectorWithNull() {
      LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      columnVector.noNulls = false;
      Random random = new Random();
      for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
        if (i % 100 == 0) {
          columnVector.isNull[i] = true;
        }
        columnVector.vector[i] = random.nextLong();
      }
      return columnVector;
    }

    protected LongColumnVector getBooleanLongColumnVector() {
      LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      Random random = new Random();
      for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
        columnVector.vector[i] = random.nextInt(2);
      }
      return columnVector;
    }

    protected LongColumnVector getBooleanRepeatingLongColumnVector() {
      LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      columnVector.fill(1);
      return columnVector;
    }

    protected LongColumnVector getBooleanLongColumnVectorWithNull() {
      LongColumnVector columnVector = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      columnVector.noNulls = false;
      Random random = new Random();
      for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
        if (i % 100 == 0) {
          columnVector.isNull[i] = true;
        }
        columnVector.vector[i] = random.nextInt(2);
      }
      return columnVector;
    }

    protected DoubleColumnVector getDoubleColumnVector() {
      DoubleColumnVector columnVector = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      Random random = new Random();
      for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
        columnVector.vector[i] = random.nextDouble();
      }
      return columnVector;
    }

    protected DoubleColumnVector getRepeatingDoubleColumnVector() {
      DoubleColumnVector columnVector = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      columnVector.fill(2.0d);
      return columnVector;
    }

    protected DoubleColumnVector getDoubleColumnVectorWithNull() {
      DoubleColumnVector columnVector = new DoubleColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      columnVector.noNulls = false;
      Random random = new Random();
      for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
        if (i % 100 == 0) {
          columnVector.isNull[i] = true;
        }
        columnVector.vector[i] = random.nextDouble();
      }
      return columnVector;
    }

  }

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
    Options opt = new OptionsBuilder().include(".*" + VectorizationBench.class.getSimpleName() +
      ".*").build();
    new Runner(opt).run();
  }
}
