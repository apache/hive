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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * An AbstractPrimitiveObjectInspector is based on
 * ObjectInspectorUtils.PrimitiveTypeEntry.
 */
public abstract class AbstractPrimitiveObjectInspector implements
    PrimitiveObjectInspector {

  protected PrimitiveTypeInfo typeInfo;

  protected AbstractPrimitiveObjectInspector() {
    super();
  }
  /**
   * Construct a AbstractPrimitiveObjectInspector.
   */
  protected AbstractPrimitiveObjectInspector(PrimitiveTypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  /**
   * Return the associated Java primitive class for this primitive
   * ObjectInspector.
   */
  @Override
  public Class<?> getJavaPrimitiveClass() {
    return typeInfo.getPrimitiveJavaClass();
  }

  /**
   * Return the associated primitive category for this primitive
   * ObjectInspector.
   */
  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return typeInfo.getPrimitiveCategory();
  }

  /**
   * Return the associated primitive Writable class for this primitive
   * ObjectInspector.
   */
  @Override
  public Class<?> getPrimitiveWritableClass() {
    return typeInfo.getPrimitiveWritableClass();
  }

  /**
   * Return the associated category this primitive ObjectInspector.
   */
  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  /**
   * Return the type name for this primitive ObjectInspector.
   */
  @Override
  public String getTypeName() {
    return typeInfo.getTypeName();
  }

  @Override
  public PrimitiveTypeInfo getTypeInfo() {
    return this.typeInfo;
  }

  /**
   * Default implementation.  Maybe overridden by exact types.
   */
  @Override
  public int precision() {
    return HiveDecimalUtils.getPrecisionForType(typeInfo);
  }

  /**
   * Default implementation.  Maybe overridden by exact types.
   */
  @Override
  public int scale() {
    return HiveDecimalUtils.getScaleForType(typeInfo);
  }

}
