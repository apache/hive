package org.apache.hadoop.hive.serde2.typeinfo;

import org.apache.hadoop.hive.serde.serdeConstants;

public class CharTypeInfo  extends BaseCharTypeInfo {
  private static final long serialVersionUID = 1L;

  // no-arg constructor to make kyro happy.
  public CharTypeInfo() {
  }

  public CharTypeInfo(int length) {
    super(serdeConstants.CHAR_TYPE_NAME, length);
    BaseCharUtils.validateCharParameter(length);
  }

  @Override
  public String getTypeName() {
    return getQualifiedName();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof CharTypeInfo)) {
      return false;
    }

    CharTypeInfo pti = (CharTypeInfo) other;

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
