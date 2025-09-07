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
import java.util.Objects;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.variants.Variant;

/**
 * ObjectInspector for Iceberg's Variant type.
 * <p>
 * Exposes a Variant value as a Hive struct with two fields:
 * <ul>
 *   <li>{@code type}: (STRING) The name of the actual type of the value.</li>
 *   <li>{@code value}: (BINARY) The binary value content.</li>
 *   <li>{@code metadata}: (BINARY) The binary metadata content.</li>
 *   </ul>
 * This provides a high-level, user-friendly view of the variant data.
 */
public final class IcebergVariantObjectInspector extends StructObjectInspector {

  private final List<IcebergVariantStructField> structFields;

  public IcebergVariantObjectInspector() {
    // Create the fixed struct fields for a Variant: `metadata` and `value`
    this.structFields = ImmutableList.of(
        new IcebergVariantStructField("metadata", PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector, 0),
        new IcebergVariantStructField("value", PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector, 1));
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public StructField getStructFieldRef(String name) {
    // Simple lookup since we only have two fixed fields
    for (IcebergVariantStructField field : structFields) {
      if (field.getFieldName().equalsIgnoreCase(name)) {
        return field;
      }
    }
    return null;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    Variant variant = (Variant) data;
    IcebergVariantStructField field = (IcebergVariantStructField) fieldRef;

    switch (field.position) {
      case 0: // "metadata" field
        // Convert VariantMetadata back to bytes
        ByteBuffer metadataBuffer = ByteBuffer.allocate(variant.metadata().sizeInBytes());
        variant.metadata().writeTo(metadataBuffer, 0);
        return metadataBuffer.array(); // Return byte array for javaByteArrayObjectInspector
      case 1: // "value" field (binary)
        ByteBuffer valueBuffer = ByteBuffer.allocate(variant.value().sizeInBytes());
        variant.value().writeTo(valueBuffer, 0);
        return valueBuffer.array();
      default:
        throw new IllegalArgumentException("Unknown field position: " + field.position);
    }
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    Variant variant = (Variant) data;
    ByteBuffer metadataBuffer = ByteBuffer.allocate(variant.metadata().sizeInBytes());
    variant.metadata().writeTo(metadataBuffer, 0);

    ByteBuffer valueBuffer = ByteBuffer.allocate(variant.value().sizeInBytes());
    variant.value().writeTo(valueBuffer, 0);

 // Return the data for our fields in the correct order: metadata, value
    return ImmutableList.of(metadataBuffer.array(), // Return byte array for javaByteArrayObjectInspector
        valueBuffer.array());
  }

  @Override
  public String getTypeName() {
    return "struct<metadata:binary,value:binary>";
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IcebergVariantObjectInspector that = (IcebergVariantObjectInspector) o;
    // Two Variant OIs are equal because their structure is always identical
    return structFields.equals(that.structFields);
  }

  @Override
  public int hashCode() {
    return structFields.hashCode();
  }

  private static class IcebergVariantStructField implements StructField {

    private final String fieldName;
    private final ObjectInspector fieldObjectInspector;
    private final int position;

    IcebergVariantStructField(String fieldName, ObjectInspector fieldObjectInspector, int position) {
      this.fieldName = fieldName;
      this.fieldObjectInspector = fieldObjectInspector;
      this.position = position;
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    @Override
    public int getFieldID() {
      return position; // Use position as ID
    }

    @Override
    public String getFieldComment() {
      return null; // No comment for internal fields
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IcebergVariantStructField that = (IcebergVariantStructField) o;
      return position == that.position && fieldName.equals(that.fieldName) && fieldObjectInspector.equals(
          that.fieldObjectInspector);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName, fieldObjectInspector, position);
    }
  }
}
