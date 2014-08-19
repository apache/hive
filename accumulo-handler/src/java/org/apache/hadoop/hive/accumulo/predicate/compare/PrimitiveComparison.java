package org.apache.hadoop.hive.accumulo.predicate.compare;

/**
 * Wraps type-specific comparison operations over a constant value. Methods take raw byte from
 * incoming Accumulo values.
 *
 * The CompareOpt instance in the iterator uses one or more methods from a PrimitiveCompare
 * implementation to perform type-specific comparisons and determine acceptances.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}. Works with
 * {@link CompareOp}
 */
public interface PrimitiveComparison {

  public boolean isEqual(byte[] value);

  public boolean isNotEqual(byte[] value);

  public boolean greaterThanOrEqual(byte[] value);

  public boolean greaterThan(byte[] value);

  public boolean lessThanOrEqual(byte[] value);

  public boolean lessThan(byte[] value);

  public boolean like(byte[] value);

  public Object serialize(byte[] value);

  public void init(byte[] constant);
}
