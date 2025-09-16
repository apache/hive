/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.iceberg.variants.Variant;

/**
 * ObjectInspector for Iceberg's Variant type in Hive.
 * <p>
 * This ObjectInspector enables Hive to work with Iceberg's Variant type, which stores
 * polymorphic data in a single column. Variant types are particularly useful for
 * semi-structured data like JSON where the actual type may vary per row.
 * <p>
 * The ObjectInspector exposes each Variant as a Hive struct with two binary fields:
 * <ul>
 *   <li><strong>metadata</strong>: Binary metadata containing type information and schema</li>
 *   <li><strong>value</strong>: Binary representation of the actual data value</li>
 * </ul>
 * <p>
 */
public final class IcebergVariantObjectInspector extends VariantObjectInspector {

  private static final ObjectInspector INSTANCE = new IcebergVariantObjectInspector();

  private IcebergVariantObjectInspector() {
  }

  public static ObjectInspector get() {
    return INSTANCE;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    Variant variant = (Variant) data;
    MyField field = (MyField) fieldRef;

    switch (field.getFieldID()) {
      case 0: // "metadata" field (binary)
        ByteBuffer metadata = ByteBuffer.allocate(variant.metadata().sizeInBytes());
        variant.metadata().writeTo(metadata, 0);
        return metadata.array();
      case 1: // "value" field (binary)
        ByteBuffer value = ByteBuffer.allocate(variant.value().sizeInBytes());
        variant.value().writeTo(value, 0);
        return value.array();
      default:
        throw new IllegalArgumentException("Unknown field position: " + field.getFieldID());
    }
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    Variant variant = (Variant) data;
    ByteBuffer metadata = ByteBuffer.allocate(variant.metadata().sizeInBytes());
    variant.metadata().writeTo(metadata, 0);

    ByteBuffer value = ByteBuffer.allocate(variant.value().sizeInBytes());
    variant.value().writeTo(value, 0);

    // Return the data for our fields in the correct order: metadata, value
    return List.of(metadata.array(), value.array());
  }
}
