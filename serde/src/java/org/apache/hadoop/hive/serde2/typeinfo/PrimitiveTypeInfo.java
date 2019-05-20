/*
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
import java.util.Objects;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;

/**
 * There are limited number of Primitive Types. All Primitive Types are defined
 * by TypeInfoFactory.isPrimitiveClass().
 *
 * Always use the TypeInfoFactory to create new TypeInfo objects, instead of
 * directly creating an instance of this class.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PrimitiveTypeInfo extends TypeInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  // Base name (varchar vs fully qualified name such as varchar(200)).
  protected String typeName;

  /**
   * For java serialization use only.
   */
  public PrimitiveTypeInfo() {
  }

  /**
   * For TypeInfoFactory use only.
   */
  PrimitiveTypeInfo(String typeName) {
    Objects.requireNonNull(typeName);
    this.typeName = typeName;
  }

  /**
   * Returns the category of this TypeInfo.
   */
  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  public PrimitiveCategory getPrimitiveCategory() {
    return getPrimitiveTypeEntry().primitiveCategory;
  }

  public Class<?> getPrimitiveWritableClass() {
    return getPrimitiveTypeEntry().primitiveWritableClass;
  }

  public Class<?> getPrimitiveJavaClass() {
    return getPrimitiveTypeEntry().primitiveJavaClass;
  }

  // The following 2 methods are for java serialization use only.
  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  public PrimitiveTypeEntry getPrimitiveTypeEntry() {
    return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(typeName);
  }

  @Override
  public String toString() {
    return typeName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((typeName == null) ? 0 : typeName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PrimitiveTypeInfo other = (PrimitiveTypeInfo) obj;
    if (typeName == null) {
      if (other.typeName != null) {
        return false;
      }
    } else if (!typeName.equals(other.typeName)) {
      return false;
    }
    return true;
  }
}
