package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

public class InputRefShifter extends RexShuttle {
  private final int startIndex;
  private final RelBuilder relBuilder;

  InputRefShifter(int startIndex, RelBuilder relBuilder) {
    this.startIndex = startIndex;
    this.relBuilder = relBuilder;
  }

  /**
   * Shift input reference index by one if the referenced column index is higher or equals with the startIndex.
   * @param inputRef - {@link RexInputRef} to transform
   * @return new {@link RexInputRef} if the referenced column index is higher or equals with the startIndex,
   * original otherwise
   */
  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    if (inputRef.getIndex() >= startIndex) {
      RexBuilder rexBuilder = relBuilder.getRexBuilder();
      return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + 1);
    }
    return inputRef;
  }
}
