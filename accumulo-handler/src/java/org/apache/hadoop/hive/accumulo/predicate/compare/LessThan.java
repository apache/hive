package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to lessThan over {@link PrimitiveComparison} instance.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class LessThan implements CompareOp {

  private PrimitiveComparison comp;

  public LessThan() {}

  public LessThan(PrimitiveComparison comp) {
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
    return comp.lessThan(val);
  }
}
