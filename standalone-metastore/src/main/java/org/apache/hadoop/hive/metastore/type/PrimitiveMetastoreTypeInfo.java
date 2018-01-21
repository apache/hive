package org.apache.hadoop.hive.metastore.type;

import java.util.Objects;

public class PrimitiveMetastoreTypeInfo extends MetastoreTypeInfo {
  private static final long serialVersionUID = 1L;

  // Base name (varchar vs fully qualified name such as varchar(200)).
  protected String typeName;
  private MetastorePrimitiveTypeCategory primitiveTypeCategory;

  /**
   * For java serialization use only.
   */
  public PrimitiveMetastoreTypeInfo() {
  }

  /**
   * For MetastoreTypeInfoFactory use only.
   */
  PrimitiveMetastoreTypeInfo(String typeName) {
    this.typeName = typeName;
    primitiveTypeCategory = MetastorePrimitiveTypeCategory.from(typeName);
  }

  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public String toString() {
    return typeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PrimitiveMetastoreTypeInfo that = (PrimitiveMetastoreTypeInfo) o;
    return Objects.equals(typeName, that.typeName);
  }

  @Override
  public int hashCode() {

    return Objects.hash(typeName);
  }

  public MetastorePrimitiveTypeCategory getPrimitiveCategory() {
    if (primitiveTypeCategory == null) {
      primitiveTypeCategory = MetastorePrimitiveTypeCategory.from(typeName);
    }
    return primitiveTypeCategory;
  }
}
