package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

public class VarcharMetastoreTypeInfo extends BaseMetastoreCharTypeInfo {
  private static final long serialVersionUID = 1L;

  // no-arg constructor to make kyro happy.
  public VarcharMetastoreTypeInfo() {
    super(ColumnType.VARCHAR_TYPE_NAME);
  }

  public VarcharMetastoreTypeInfo(int length) {
    super(ColumnType.VARCHAR_TYPE_NAME, length);
    validateVarcharParameter(length);
  }

  public static void validateVarcharParameter(int length) {
    if (length > ColumnType.MAX_VARCHAR_LENGTH || length < 1) {
      throw new RuntimeException("Varchar length " + length + " out of allowed range [1, " +
          ColumnType.MAX_VARCHAR_LENGTH + "]");
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

    VarcharMetastoreTypeInfo pti = (VarcharMetastoreTypeInfo) other;

    return this.getLength() == pti.getLength();
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return getLength();
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }
}
