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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class AbstractExpression {
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
    return rowBatch;
  }

  @Setup
  public abstract void setup();

  @Benchmark
  @Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
  @Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.MILLISECONDS)
  public void bench() {
    for (int i = 0; i < DEFAULT_ITER_TIME; i++) {
      rowBatch.selectedInUse = false;
      rowBatch.size = VectorizedRowBatch.DEFAULT_SIZE;

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

  protected BytesColumnVector getBytesColumnVector() {
    BytesColumnVector columnVector = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    Random random = new Random();
    int length = 16;
    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      columnVector.vector[i] = new byte[length];
      columnVector.start[i] = 0;
      columnVector.length[i] = length;
      for (int j = 0; j < length; j++) {
        columnVector.vector[i][j] = (byte)(random.nextInt(+ 'c' - 'a' + 1) + 'a');
      }
    }
    return columnVector;
  }
}
