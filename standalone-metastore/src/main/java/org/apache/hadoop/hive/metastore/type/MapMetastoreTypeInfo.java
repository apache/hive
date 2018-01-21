package org.apache.hadoop.hive.metastore.type;

import org.apache.hadoop.hive.metastore.ColumnType;

public class MapMetastoreTypeInfo extends MetastoreTypeInfo {
  private static final long serialVersionUID = 1L;

  private MetastoreTypeInfo mapKeyTypeInfo;
  private MetastoreTypeInfo mapValueTypeInfo;

  /**
   * For java serialization use only.
   */
  public MapMetastoreTypeInfo() {
  }

  @Override
  public String getTypeName() {
    return ColumnType.MAP_TYPE_NAME + "<"
        + mapKeyTypeInfo.getTypeName() + "," + mapValueTypeInfo.getTypeName()
        + ">";
  }

  /**
   * For java serialization use only.
   */
  public void setMapKeyTypeInfo(MetastoreTypeInfo mapKeyTypeInfo) {
    this.mapKeyTypeInfo = mapKeyTypeInfo;
  }

  /**
   * For java serialization use only.
   */
  public void setMapValueTypeInfo(MetastoreTypeInfo mapValueTypeInfo) {
    this.mapValueTypeInfo = mapValueTypeInfo;
  }

  // For TypeInfoFactory use only
  MapMetastoreTypeInfo(MetastoreTypeInfo keyTypeInfo, MetastoreTypeInfo valueTypeInfo) {
    mapKeyTypeInfo = keyTypeInfo;
    mapValueTypeInfo = valueTypeInfo;
  }

  @Override
  public Category getCategory() {
    return Category.MAP;
  }

  public MetastoreTypeInfo getMapKeyTypeInfo() {
    return mapKeyTypeInfo;
  }

  public MetastoreTypeInfo getMapValueTypeInfo() {
    return mapValueTypeInfo;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MetastoreTypeInfo)) {
      return false;
    }
    MapMetastoreTypeInfo o = (MapMetastoreTypeInfo) other;
    return o.getMapKeyTypeInfo().equals(getMapKeyTypeInfo())
        && o.getMapValueTypeInfo().equals(getMapValueTypeInfo());
  }

  @Override
  public int hashCode() {
    return mapKeyTypeInfo.hashCode() ^ mapValueTypeInfo.hashCode();
  }
}
