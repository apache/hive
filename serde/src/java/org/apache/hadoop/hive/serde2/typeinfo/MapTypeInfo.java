/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2.typeinfo;

import java.io.Serializable;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

/**
 * A Map Type has homogeneous keys and homogeneous values. All keys of the Map
 * have the same TypeInfo, which is returned by getMapKeyTypeInfo(); and all
 * values of the Map has the same TypeInfo, which is returned by
 * getMapValueTypeInfo().
 * 
 * Always use the TypeInfoFactory to create new TypeInfo objects, instead of
 * directly creating an instance of this class.
 */
public final class MapTypeInfo extends TypeInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private TypeInfo mapKeyTypeInfo;
  private TypeInfo mapValueTypeInfo;

  /**
   * For java serialization use only.
   */
  public MapTypeInfo() {
  }

  @Override
  public String getTypeName() {
    return org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME + "<"
        + mapKeyTypeInfo.getTypeName() + "," + mapValueTypeInfo.getTypeName()
        + ">";
  }

  /**
   * For java serialization use only.
   */
  public void setMapKeyTypeInfo(TypeInfo mapKeyTypeInfo) {
    this.mapKeyTypeInfo = mapKeyTypeInfo;
  }

  /**
   * For java serialization use only.
   */
  public void setMapValueTypeInfo(TypeInfo mapValueTypeInfo) {
    this.mapValueTypeInfo = mapValueTypeInfo;
  }

  // For TypeInfoFactory use only
  MapTypeInfo(TypeInfo keyTypeInfo, TypeInfo valueTypeInfo) {
    mapKeyTypeInfo = keyTypeInfo;
    mapValueTypeInfo = valueTypeInfo;
  }

  @Override
  public Category getCategory() {
    return Category.MAP;
  }

  public TypeInfo getMapKeyTypeInfo() {
    return mapKeyTypeInfo;
  }

  public TypeInfo getMapValueTypeInfo() {
    return mapValueTypeInfo;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof MapTypeInfo)) {
      return false;
    }
    MapTypeInfo o = (MapTypeInfo) other;
    return o.getMapKeyTypeInfo().equals(getMapKeyTypeInfo())
        && o.getMapValueTypeInfo().equals(getMapValueTypeInfo());
  }

  @Override
  public int hashCode() {
    return mapKeyTypeInfo.hashCode() ^ mapValueTypeInfo.hashCode();
  }

}
