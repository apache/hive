package org.apache.hadoop.hive.metastore.type;

public abstract class MetastoreTypeInfo {
  /**
   * Category.
   *
   */
  public static enum Category {
    PRIMITIVE, LIST, MAP, STRUCT, UNION
  };

  public abstract Category getCategory();
  public abstract String getTypeName();
  public abstract boolean equals(Object o);
  public abstract int hashCode();
  /**
   * String representing the qualified type name.
   * Qualified types should override this method.
   * @return
   */
  public String getQualifiedName() {
    return getTypeName();
  }
}
