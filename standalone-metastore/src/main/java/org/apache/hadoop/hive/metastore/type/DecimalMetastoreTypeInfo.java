package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

public class DecimalMetastoreTypeInfo extends PrimitiveMetastoreTypeInfo {
  private static final long serialVersionUID = 1L;

  private int precision;
  private int scale;

  // no-arg constructor for deserialization.
  public DecimalMetastoreTypeInfo() {
    super(ColumnType.DECIMAL_TYPE_NAME);
  }

  public DecimalMetastoreTypeInfo(int precision, int scale) {
    super(ColumnType.DECIMAL_TYPE_NAME);
    validateParameter(precision, scale);
    this.precision = precision;
    this.scale = scale;
  }

  public static void validateParameter(int precision, int scale) {
    if (precision < 1 || precision > ColumnType.MAX_PRECISION) {
      throw new IllegalArgumentException("Decimal precision out of allowed range [1," +
          ColumnType.MAX_PRECISION + "]");
    }

    if (scale < 0 || scale > ColumnType.MAX_SCALE) {
      throw new IllegalArgumentException("Decimal scale out of allowed range [0," +
          ColumnType.MAX_SCALE + "]");
    }

    if (precision < scale) {
      throw new IllegalArgumentException("Decimal scale must be less than or equal to precision");
    }
  }

  @Override
  public String getTypeName() {
    return getQualifiedName();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    DecimalMetastoreTypeInfo dti = (DecimalMetastoreTypeInfo)other;

    return this.precision() == dti.precision() && this.scale() == dti.scale();

  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return 31 * (17 + precision) + scale;
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }

  @Override
  public String getQualifiedName() {
    return getQualifiedName(precision, scale);
  }

  public static String getQualifiedName(int precision, int scale) {
    StringBuilder sb = new StringBuilder(ColumnType.DECIMAL_TYPE_NAME);
    sb.append("(");
    sb.append(precision);
    sb.append(",");
    sb.append(scale);
    sb.append(")");
    return sb.toString();
  }

  public int precision() {
    return precision;
  }

  public int scale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

}
