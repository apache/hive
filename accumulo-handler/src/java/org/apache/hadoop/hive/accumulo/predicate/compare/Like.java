package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to like over {@link PrimitiveComparison} instance. Currently only supported by
 * StringCompare.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class Like implements CompareOp {

  PrimitiveComparison comp;

  public Like() {}

  public Like(PrimitiveComparison comp) {
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
    return comp.like(val);
  }
}
