package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to greaterThan over {@link PrimitiveComparison} instance.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class GreaterThan implements CompareOp {

  private PrimitiveComparison comp;

  public GreaterThan() {}

  public GreaterThan(PrimitiveComparison comp) {
    this.comp = comp;
  }

  @Override
  public void setPrimitiveCompare(PrimitiveComparison comp) {
    this.comp = comp;
  }

  @Override
  public PrimitiveComparison getPrimitiveCompare() {
    return this.comp;
  }

  @Override
  public boolean accept(byte[] val) {
    return comp.greaterThan(val);
  }
}
