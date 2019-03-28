package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;

/**
 * Vectorized implementation of trunc(date, fmt) function date timestamp input
 */
public class TruncDateFromDate extends TruncDateFromTimestamp {
  private transient Date date = new Date();

  public TruncDateFromDate(int colNum, byte[] fmt, int outputColumnNum) {
    super(colNum, fmt, outputColumnNum);
  }

  private static final long serialVersionUID = 1L;

  public TruncDateFromDate() {
    super();
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    truncDate((LongColumnVector) inV, outV, i);
  }

  protected void truncDate(LongColumnVector inV, BytesColumnVector outV, int i) {
    date = Date.ofEpochMilli(inV.vector[i]);
    processDate(outV, i, date);
  }

  @Override
  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.DATE;
  }
}
