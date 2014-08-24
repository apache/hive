package org.apache.hadoop.hive.accumulo.predicate.compare;

import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * Set of comparison operations over a string constant. Used for Hive predicates involving string
 * comparison.
 *
 * Used by {@link org.apache.hadoop.hive.accumulo.predicate.PrimitiveComparisonFilter}
 */
public class StringCompare implements PrimitiveComparison {
  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(StringCompare.class);

  private String constant;

  @Override
  public void init(byte[] constant) {
    this.constant = serialize(constant);
  }

  @Override
  public boolean isEqual(byte[] value) {
    return serialize(value).equals(constant);
  }

  @Override
  public boolean isNotEqual(byte[] value) {
    return !isEqual(value);
  }

  @Override
  public boolean greaterThanOrEqual(byte[] value) {
    return serialize(value).compareTo(constant) >= 0;
  }

  @Override
  public boolean greaterThan(byte[] value) {
    return serialize(value).compareTo(constant) > 0;
  }

  @Override
  public boolean lessThanOrEqual(byte[] value) {
    return serialize(value).compareTo(constant) <= 0;
  }

  @Override
  public boolean lessThan(byte[] value) {
    return serialize(value).compareTo(constant) < 0;
  }

  @Override
  public boolean like(byte[] value) {
    String temp = new String(value).replaceAll("%", "[\\\\\\w]+?");
    Pattern pattern = Pattern.compile(temp);
    boolean match = pattern.matcher(constant).matches();
    return match;
  }

  public String serialize(byte[] value) {
    return new String(value);
  }
}
