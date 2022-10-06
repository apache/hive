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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.NullUtil;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.function.LongBinaryOperator;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
public class VectorizationTemplateBench {
  public static class LongColAddLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new LongColAddLongColumn(0, 1, 2);
      expression.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    }
  }

  public static class LambdaLongColAddLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new LambdaLongColAddLongColumn(0, 1, 2);
      expression.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    }
  }

  public static class OverrideLongColAddLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new OverrideLongColAddLongColumn(0, 1, 2);
      expression.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    }
  }

  public static class StreamLongColAddLongColumnBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new LongColumnVector(), 2, getLongColumnVector(),
          getLongColumnVector());
      expression = new StreamLongColAddLongColumn(0, 1, 2);
      expression.setOutputTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("int"));
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + VectorizationTemplateBench.class.getSimpleName() +
        ".*").build();
    new Runner(opt).run();
  }

  static class LongColAddLongColumn extends BaseLongColAddLongColumn {
    LongColAddLongColumn(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    LongColAddLongColumn() {
      super();
    }

    @Override
    public void evaluate(VectorizedRowBatch batch) throws HiveException {
      // return immediately if batch is empty
      final int n = batch.size;
      if (n == 0) {
        return;
      }

      if (childExpressions != null) {
        super.evaluateChildren(batch);
      }

      LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[inputColumnNum[0]];
      LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[inputColumnNum[1]];
      LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
      int[] sel = batch.selected;

      long[] vector1 = inputColVector1.vector;
      long[] vector2 = inputColVector2.vector;
      long[] outputVector = outputColVector.vector;

      /*
       * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
       */
      NullUtil.propagateNullsColCol(
          inputColVector1, inputColVector2, outputColVector, sel, n, batch.selectedInUse);

      /* Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        outputVector[0] = vector1[0] + vector2[0];
      } else if (inputColVector1.isRepeating) {
        final long vector1Value = vector1[0];
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1Value + vector2[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1Value + vector2[i];
          }
        }
      } else if (inputColVector2.isRepeating) {
        final long vector2Value = vector2[0];
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] + vector2Value;
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] + vector2Value;
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector1[i] + vector2[i];
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = vector1[i] + vector2[i];
          }
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      NullUtil.setNullDataEntriesLong(outputColVector, batch.selectedInUse, sel, n);
    }
  }

  static class LambdaLongColAddLongColumn extends BaseLongColAddLongColumn {
    LambdaLongColAddLongColumn(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    LambdaLongColAddLongColumn() {
      super();
    }

    private final static LongBinaryOperator func = (long a, long b) -> a + b;

    @Override
    public void evaluate(VectorizedRowBatch batch) throws HiveException {
      // return immediately if batch is empty
      final int n = batch.size;
      if (n == 0) {
        return;
      }

      if (childExpressions != null) {
        super.evaluateChildren(batch);
      }

      LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[inputColumnNum[0]];
      LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[inputColumnNum[1]];
      LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
      int[] sel = batch.selected;

      long[] vector1 = inputColVector1.vector;
      long[] vector2 = inputColVector2.vector;
      long[] outputVector = outputColVector.vector;

      /*
       * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
       */
      NullUtil.propagateNullsColCol(
          inputColVector1, inputColVector2, outputColVector, sel, n, batch.selectedInUse);

      /* Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        outputVector[0] = vector1[0] + vector2[0];
      } else if (inputColVector1.isRepeating) {
        final long vector1Value = vector1[0];
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = func.applyAsLong(vector1Value, vector2[i]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = func.applyAsLong(vector1Value, vector2[i]);
          }
        }
      } else if (inputColVector2.isRepeating) {
        final long vector2Value = vector2[0];
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = func.applyAsLong(vector1[i], vector2Value);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = func.applyAsLong(vector1[i], vector2Value);
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = func.applyAsLong(vector1[i], vector2[i]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = func.applyAsLong(vector1[i], vector2[i]);
          }
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      NullUtil.setNullDataEntriesLong(outputColVector, batch.selectedInUse, sel, n);
    }
  }

  static class OverrideLongColAddLongColumn extends BaseLongColAddLongColumn {
    OverrideLongColAddLongColumn(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    OverrideLongColAddLongColumn() {
      super();
    }

    private static long func(long a, long b) {
      return a + b;
    }

    @Override
    public void evaluate(VectorizedRowBatch batch) throws HiveException {
      // return immediately if batch is empty
      final int n = batch.size;
      if (n == 0) {
        return;
      }

      if (childExpressions != null) {
        super.evaluateChildren(batch);
      }

      LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[inputColumnNum[0]];
      LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[inputColumnNum[1]];
      LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
      int[] sel = batch.selected;

      long[] vector1 = inputColVector1.vector;
      long[] vector2 = inputColVector2.vector;
      long[] outputVector = outputColVector.vector;

      /*
       * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
       */
      NullUtil.propagateNullsColCol(
          inputColVector1, inputColVector2, outputColVector, sel, n, batch.selectedInUse);

      /* Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        outputVector[0] = vector1[0] + vector2[0];
      } else if (inputColVector1.isRepeating) {
        final long vector1Value = vector1[0];
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = func(vector1Value, vector2[i]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = func(vector1Value, vector2[i]);
          }
        }
      } else if (inputColVector2.isRepeating) {
        final long vector2Value = vector2[0];
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = func(vector1[i], vector2Value);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = func(vector1[i], vector2Value);
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = func(vector1[i], vector2[i]);
          }
        } else {
          for(int i = 0; i != n; i++) {
            outputVector[i] = func(vector1[i], vector2[i]);
          }
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      NullUtil.setNullDataEntriesLong(outputColVector, batch.selectedInUse, sel, n);
    }
  }

  static class StreamLongColAddLongColumn extends BaseLongColAddLongColumn {
    StreamLongColAddLongColumn(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    StreamLongColAddLongColumn() {
      super();
    }

    @Override
    public void evaluate(VectorizedRowBatch batch) throws HiveException {

      // return immediately if batch is empty
      final int n = batch.size;
      if (n == 0) {
        return;
      }

      if (childExpressions != null) {
        super.evaluateChildren(batch);
      }

      LongColumnVector inputColVector1 = (LongColumnVector) batch.cols[inputColumnNum[0]];
      LongColumnVector inputColVector2 = (LongColumnVector) batch.cols[inputColumnNum[1]];
      LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
      int[] sel = batch.selected;

      long[] vector1 = inputColVector1.vector;
      long[] vector2 = inputColVector2.vector;
      long[] outputVector = outputColVector.vector;

      /*
       * Propagate null values for a two-input operator and set isRepeating and noNulls appropriately.
       */
      NullUtil.propagateNullsColCol(
          inputColVector1, inputColVector2, outputColVector, sel, n, batch.selectedInUse);

      /* Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
        outputVector[0] = vector1[0] + vector2[0];
      } else if (inputColVector1.isRepeating) {
        final long vector1Value = vector1[0];
        if (batch.selectedInUse) {
          Arrays.stream(sel).forEach(i -> outputVector[i] = vector1Value + vector2[i]);
        } else {
          IntStream.range(0, n).forEach(i -> outputVector[i] = vector1Value + vector2[i]);
        }

      } else if (inputColVector2.isRepeating) {
        final long vector2Value = vector2[0];
        if (batch.selectedInUse) {
          Arrays.stream(sel).forEach(i -> outputVector[i] = vector1[i] + vector2Value);
        } else {
          IntStream.range(0, n).forEach(i -> outputVector[i] = vector1[i] + vector2Value);
        }
      } else {
        if (batch.selectedInUse) {
          Arrays.stream(sel).forEach(i -> outputVector[i] = vector1[i] + vector2[i]);
        } else {
          IntStream.range(0, n).forEach(i -> outputVector[i] = vector1[i] + vector2[i]);
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      NullUtil.setNullDataEntriesLong(outputColVector, batch.selectedInUse, sel, n);
    }
  }

  abstract static class BaseLongColAddLongColumn extends VectorExpression {
    private static final long serialVersionUID = 1L;

    BaseLongColAddLongColumn(int colNum1, int colNum2, int outputColumnNum) {
      super(colNum1, colNum2, outputColumnNum);
    }

    BaseLongColAddLongColumn() {
      super();
    }

    @Override
    public String vectorExpressionParameters() {
      return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]);
    }

    @Override
    public VectorExpressionDescriptor.Descriptor getDescriptor() {
      return (new VectorExpressionDescriptor.Builder())
          .setMode(
              VectorExpressionDescriptor.Mode.PROJECTION)
          .setNumArguments(2)
          .setArgumentTypes(
              VectorExpressionDescriptor.ArgumentType.getType("long"),
              VectorExpressionDescriptor.ArgumentType.getType("long"))
          .setInputExpressionTypes(
              VectorExpressionDescriptor.InputExpressionType.COLUMN,
              VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
    }
  }
}
