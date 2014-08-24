package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps call to isEqual() over PrimitiveCompare instance.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class Equal implements CompareOp {

  private PrimitiveComparison comp;

  public Equal() {}

  public Equal(PrimitiveComparison comp) {
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
    return comp.isEqual(val);
  }
}
