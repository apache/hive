package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.DateParser;

/**
 * Vectorized implementation of trunc(date, fmt) function for timestamp input
 */
public class TruncDateFromTimestamp extends VectorExpression {
  private static final long serialVersionUID = 1L;
  protected int colNum;
  protected String fmt;
  protected transient final DateParser dateParser = new DateParser();

  public TruncDateFromTimestamp() {
    super();
    colNum = -1;
  }

  public TruncDateFromTimestamp(int colNum, byte[] fmt, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.fmt = new String(fmt, StandardCharsets.UTF_8);
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + colNum + ", format " + fmt;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[colNum];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        truncDate(inputColVector, outputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.
        if (!outputColVector.noNulls) {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            // Set isNull before call in case it changes it mind.
            outputIsNull[i] = false;
            truncDate(inputColVector, outputColVector, i);
          }
        } else {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            truncDate(inputColVector, outputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          truncDate(inputColVector, outputColVector, i);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          truncDate(inputColVector, outputColVector, i);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for (int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            truncDate(inputColVector, outputColVector, i);
          }
        }
      }
    }
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    Date date = Date.ofEpochMilli(((TimestampColumnVector) inV).getTime(i));
    processDate(outV, i, date);
  }

  protected void processDate(BytesColumnVector outV, int i, Date date) {
    if ("MONTH".equals(fmt) || "MON".equals(fmt) || "MM".equals(fmt)) {
      date.setDayOfMonth(1);
    } else if ("QUARTER".equals(fmt) || "Q".equals(fmt)) {
      int month = date.getMonth() - 1;
      int quarter = month / 3;
      int monthToSet = quarter * 3 + 1;
      date.setMonth(monthToSet);
      date.setDayOfMonth(1);
    } else if ("YEAR".equals(fmt) || "YYYY".equals(fmt) || "YY".equals(fmt)) {
      date.setMonth(1);
      date.setDayOfMonth(1);
    }
    byte[] bytes = date.toString().getBytes(StandardCharsets.UTF_8);
    outV.setVal(i, bytes, 0, bytes.length);
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(getInputColumnType(),
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }

  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.TIMESTAMP;
  }
}
