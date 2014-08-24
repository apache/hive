package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to isEqual over {@link PrimitiveComparison} instance and returns the negation.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class NotEqual implements CompareOp {

  private PrimitiveComparison comp;

  public NotEqual() {}

  public NotEqual(PrimitiveComparison comp) {
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
    return !comp.isEqual(val);
  }
}
