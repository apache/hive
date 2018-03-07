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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;

import java.io.Serializable;

/**
 * This class represents a PrimitiveTypeInfo. Hive extends this class to create PrimitiveTypeInfo
 */
@LimitedPrivate("Hive")
public class MetastorePrimitiveTypeInfo extends TypeInfo implements Serializable {
  // Base name (varchar vs fully qualified name such as varchar(200)).
  protected String typeName;

  public MetastorePrimitiveTypeInfo() {
  }

  public MetastorePrimitiveTypeInfo(String typeName) {
    this.typeName = typeName;
  }

  // The following 2 methods are for java serialization use only.
  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public MetastoreTypeCategory getCategory() {
    return MetastoreTypeCategory.PRIMITIVE;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    MetastorePrimitiveTypeInfo pti = (MetastorePrimitiveTypeInfo) other;

    return this.typeName.equals(pti.typeName);
  }

  /**
   * Generate the hashCode for this TypeInfo.
   */
  @Override
  public int hashCode() {
    return typeName.hashCode();
  }

  @Override
  public String toString() {
    return typeName;
  }

  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  /**
   * parameterized TypeInfos should override this to return array of parameters
   * @return
   */
  public Object[] getParameters() {
    //default is no parameters
    return EMPTY_OBJECT_ARRAY;
  }
}
