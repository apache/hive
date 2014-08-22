package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Handles different types of comparisons in hive predicates. Filter iterator delegates value
 * acceptance to the CompareOpt.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}. Works with
 * {@link PrimitiveComparison}
 */
public interface CompareOp {
  /**
   * Sets the PrimitiveComparison for this CompareOp
   */
  public void setPrimitiveCompare(PrimitiveComparison comp);

  /**
   * @return The PrimitiveComparison this CompareOp is a part of
   */
  public PrimitiveComparison getPrimitiveCompare();

  /**
   * @param val The bytes from the Accumulo Value
   * @return true if the value is accepted by this CompareOp
   */
  public boolean accept(byte[] val);
}
