package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to lessThanOrEqual over {@link PrimitiveComparison} instance.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class LessThanOrEqual implements CompareOp {

  private PrimitiveComparison comp;

  public LessThanOrEqual() {}

  public LessThanOrEqual(PrimitiveComparison comp) {
    this.comp = comp;
  }

  @Override
  public void setPrimitiveCompare(PrimitiveComparison comp) {
    this.comp = comp;
  }

  @Override
  public PrimitiveComparison getPrimitiveCompare() {
    return comp;
  }

  @Override
  public boolean accept(byte[] val) {
    return comp.lessThanOrEqual(val);
  }
}
