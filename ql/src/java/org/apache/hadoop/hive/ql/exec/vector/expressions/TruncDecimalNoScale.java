package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;

/**
 * Vectorized implementation of trunc(number) function for decimal input
 */
public class TruncDecimalNoScale extends TruncDecimal {
  private static final long serialVersionUID = 1L;

  public TruncDecimalNoScale() {
    super();
    colNum = -1;
  }

  public TruncDecimalNoScale(int colNum, int outputColumnNum) {
    super(colNum, 0, outputColumnNum);
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(1)
        .setArgumentTypes(getInputColumnType())
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
