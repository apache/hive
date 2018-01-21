package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

import java.util.ArrayList;
import java.util.List;

public class UnionMetastoreTypeInfo extends MetastoreTypeInfo {
  private static final long serialVersionUID = 1L;

  private List<MetastoreTypeInfo> allUnionObjectTypeInfos;

  /**
   * For java serialization use only.
   */
  public UnionMetastoreTypeInfo() {
  }

  @Override
  public String getTypeName() {
    StringBuilder sb = new StringBuilder();
    sb.append(ColumnType.UNION_TYPE_NAME + "<");
    for (int i = 0; i < allUnionObjectTypeInfos.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(allUnionObjectTypeInfos.get(i).getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  /**
   * For java serialization use only.
   */
  public void setAllUnionObjectTypeInfos(
      List<MetastoreTypeInfo> allUnionObjectTypeInfos) {
    this.allUnionObjectTypeInfos = allUnionObjectTypeInfos;
  }

  /**
   * For TypeInfoFactory use only.
   */
  UnionMetastoreTypeInfo(List<MetastoreTypeInfo> typeInfos) {
    allUnionObjectTypeInfos = new ArrayList<MetastoreTypeInfo>();
    allUnionObjectTypeInfos.addAll(typeInfos);
  }

  @Override
  public Category getCategory() {
    return Category.UNION;
  }

  public List<MetastoreTypeInfo> getAllUnionObjectTypeInfos() {
    return allUnionObjectTypeInfos;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof UnionMetastoreTypeInfo)) {
      return false;
    }
    UnionMetastoreTypeInfo o = (UnionMetastoreTypeInfo) other;

    // Compare the field types
    return o.getAllUnionObjectTypeInfos().equals(getAllUnionObjectTypeInfos());
  }

  @Override
  public int hashCode() {
    return allUnionObjectTypeInfos.hashCode();
  }
}
