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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

/**
 * UnionTypeInfo represents the TypeInfo of an union. A union holds only one
 * field of the specified fields at any point of time. The fields, a Union can
 * hold, can have the same or different TypeInfo.
 *
 * Always use the TypeInfoFactory to create new TypeInfo objects, instead of
 * directly creating an instance of this class.
 */
public class UnionTypeInfo extends TypeInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<TypeInfo> allUnionObjectTypeInfos;

  /**
   * For java serialization use only.
   */
  public UnionTypeInfo() {
  }

  @Override
  public String getTypeName() {
    StringBuilder sb = new StringBuilder();
    sb.append(serdeConstants.UNION_TYPE_NAME + "<");
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
      List<TypeInfo> allUnionObjectTypeInfos) {
    this.allUnionObjectTypeInfos = allUnionObjectTypeInfos;
  }

  /**
   * For TypeInfoFactory use only.
   */
  UnionTypeInfo(List<TypeInfo> typeInfos) {
    allUnionObjectTypeInfos = new ArrayList<TypeInfo>();
    allUnionObjectTypeInfos.addAll(typeInfos);
  }

  @Override
  public Category getCategory() {
    return Category.UNION;
  }

  public List<TypeInfo> getAllUnionObjectTypeInfos() {
    return allUnionObjectTypeInfos;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof UnionTypeInfo)) {
      return false;
    }
    UnionTypeInfo o = (UnionTypeInfo) other;

    // Compare the field types
    return o.getAllUnionObjectTypeInfos().equals(getAllUnionObjectTypeInfos());
  }

  @Override
  public int hashCode() {
    return allUnionObjectTypeInfos.hashCode();
  }
}
