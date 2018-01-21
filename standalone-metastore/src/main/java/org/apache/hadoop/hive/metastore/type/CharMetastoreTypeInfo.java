package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

public class CharMetastoreTypeInfo extends BaseMetastoreCharTypeInfo {
  private static final long serialVersionUID = 1L;

  // no-arg constructor to make kyro happy.
  public CharMetastoreTypeInfo() {
    super(ColumnType.CHAR_TYPE_NAME);
  }

  public CharMetastoreTypeInfo(int length) {
    super(ColumnType.CHAR_TYPE_NAME, length);
    validateCharParameter(length);
  }

  public static void validateCharParameter(int length) {
    if (length > ColumnType.MAX_CHAR_LENGTH || length < 1) {
      throw new RuntimeException("Char length " + length + " out of allowed range [1, " +
          ColumnType.MAX_CHAR_LENGTH + "]");
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

    CharMetastoreTypeInfo pti = (CharMetastoreTypeInfo) other;

    return this.typeName.equals(pti.typeName) && this.getLength() == pti.getLength();
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return getQualifiedName().hashCode();
  }

  @Override
  public String toString() {
    return getQualifiedName();
  }
}
