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

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * VariantTypeInfo represents the TypeInfo of a Variant type in Hive.
 * <p>
 * A Variant type is a flexible data type that can store values of different types
 * in a single column. It is particularly useful for semi-structured data like JSON
 * where the actual type of a value may not be known at schema definition time.
 * <p>
 * The Variant type is primarily used in conjunction with Apache Iceberg tables
 * and provides a way to store polymorphic data efficiently. When used with Iceberg,
 * variant values are internally represented as a struct containing:
 * <ul>
 *   <li><strong>metadata</strong>: Binary metadata describing the actual type of the value</li>
 *   <li><strong>value</strong>: Binary representation of the actual value</li>
 * </ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class VariantTypeInfo extends TypeInfo implements Serializable {

  @Serial
  private static final long serialVersionUID = 1L;


  @Override
  public Category getCategory() {
    return Category.VARIANT;
  }

  @Override
  public String getTypeName() {
    return "variant";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof VariantTypeInfo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(VariantTypeInfo.class, getTypeName());
  }
}
