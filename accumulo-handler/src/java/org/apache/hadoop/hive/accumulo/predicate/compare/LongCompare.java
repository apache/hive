package org.apache.hadoop.hive.accumulo.predicate.compare;

import java.nio.ByteBuffer;

/**
 * Set of comparison operations over a long constant. Used for Hive predicates involving long
 * comparison.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class LongCompare implements PrimitiveComparison {

  private long constant;

  @Override
  public void init(byte[] constant) {
    this.constant = serialize(constant);
  }

  @Override
  public boolean isEqual(byte[] value) {
    long lonVal = serialize(value);
    return lonVal == constant;
  }

  @Override
  public boolean isNotEqual(byte[] value) {
    return serialize(value) != constant;
  }

  @Override
  public boolean greaterThanOrEqual(byte[] value) {
    return serialize(value) >= constant;
  }

  @Override
  public boolean greaterThan(byte[] value) {
    return serialize(value) > constant;
  }

  @Override
  public boolean lessThanOrEqual(byte[] value) {
    return serialize(value) <= constant;
  }

  @Override
  public boolean lessThan(byte[] value) {
    return serialize(value) < constant;
  }

  @Override
  public boolean like(byte[] value) {
    throw new UnsupportedOperationException("Like not supported for " + getClass().getName());
  }

  public Long serialize(byte[] value) {
    try {
      return ByteBuffer.wrap(value).asLongBuffer().get();
    } catch (Exception e) {
      throw new RuntimeException(e.toString() + " occurred trying to build long value. "
          + "Make sure the value type for the byte[] is long ");
    }
  }
}
