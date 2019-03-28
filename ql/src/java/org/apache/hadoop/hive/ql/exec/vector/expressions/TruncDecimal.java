package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Vectorized implementation of trunc(number, scale) function for decimal input
 */
public class TruncDecimal extends TruncFloat {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public TruncDecimal() {
    super();
  }

  public TruncDecimal(int colNum, int scale, int outputColumnNum) {
    super(colNum, scale, outputColumnNum);
  }

  @Override
  protected void trunc(ColumnVector inputColVector, ColumnVector outputColVector, int i) {
    HiveDecimal input = ((DecimalColumnVector) inputColVector).vector[i].getHiveDecimal();

    HiveDecimal output = trunc(input);
    ((DecimalColumnVector) outputColVector).vector[i] = new HiveDecimalWritable(output);
  }

  protected HiveDecimal trunc(HiveDecimal input) {
    HiveDecimal pow = HiveDecimal.create(Math.pow(10, Math.abs(scale)));

    if (scale >= 0) {
      if (scale != 0) {
        long longValue = input.multiply(pow).longValue();
        return HiveDecimal.create(longValue).divide(pow);
      } else {
        return HiveDecimal.create(input.longValue());
      }
    } else {
      long longValue2 = input.divide(pow).longValue();
      return HiveDecimal.create(longValue2).multiply(pow);
    }
  }

  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.DECIMAL;
  }
}
