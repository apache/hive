package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

public class ListMetastoreTypeInfo extends MetastoreTypeInfo {
  private static final long serialVersionUID = 1L;
  private MetastoreTypeInfo listElementTypeInfo;

  /**
   * For java serialization use only.
   */
  public ListMetastoreTypeInfo() {
  }

  @Override
  public String getTypeName() {
    return ColumnType.LIST_TYPE_NAME + "<"
        + listElementTypeInfo.getTypeName() + ">";
  }

  /**
   * For java serialization use only.
   */
  public void setListElementTypeInfo(MetastoreTypeInfo listElementTypeInfo) {
    this.listElementTypeInfo = listElementTypeInfo;
  }

  /**
   * For TypeInfoFactory use only.
   */
  ListMetastoreTypeInfo(MetastoreTypeInfo elementTypeInfo) {
    listElementTypeInfo = elementTypeInfo;
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  public MetastoreTypeInfo getListElementTypeInfo() {
    return listElementTypeInfo;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ListMetastoreTypeInfo)) {
      return false;
    }
    return getListElementTypeInfo().equals(
        ((ListMetastoreTypeInfo) other).getListElementTypeInfo());
  }

  @Override
  public int hashCode() {
    return listElementTypeInfo.hashCode();
  }
}
