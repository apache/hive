package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;

/**
 * Vectorized implementation of trunc(date, fmt) function for string input
 */
public class TruncDateFromString extends TruncDateFromTimestamp {
  private transient Date date = new Date();

  public TruncDateFromString(int colNum, byte[] fmt, int outputColumnNum) {
    super(colNum, fmt, outputColumnNum);
  }

  private static final long serialVersionUID = 1L;

  public TruncDateFromString() {
    super();
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    truncDate((BytesColumnVector) inV, outV, i);
  }

  protected void truncDate(BytesColumnVector inV, BytesColumnVector outV, int i) {
    if (inV.vector[i] == null) {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }

    String dateString =
        new String(inV.vector[i], inV.start[i], inV.length[i], StandardCharsets.UTF_8);
    if (dateParser.parseDate(dateString, date)) {
      processDate(outV, i, date);
    } else {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }
  }

  @Override
  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.STRING_FAMILY;
  }
}
