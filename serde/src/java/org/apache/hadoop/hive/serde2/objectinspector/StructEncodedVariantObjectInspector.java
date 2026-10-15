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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantEncoding;
import org.apache.hadoop.io.BytesWritable;

/**
 * Presents a variant that a serde stores as its own struct of the two binaries, metadata first
 * ({@link org.apache.hadoop.hive.serde2.variant.VariantEncoding}). The serde keeps its struct objects and its struct
 * inspector; the engine sees VARIANT.
 */
public class StructEncodedVariantObjectInspector implements VariantObjectInspector {

  private final StructObjectInspector encoded;
  private final StructField metadataField;
  private final StructField valueField;

  public StructEncodedVariantObjectInspector(StructObjectInspector encoded) {
    this.encoded = encoded;
    List<? extends StructField> fields = encoded.getAllStructFieldRefs();
    this.metadataField = fields.get(VariantEncoding.METADATA_INDEX);
    this.valueField = fields.get(VariantEncoding.VALUE_INDEX);
  }

  public StructObjectInspector getEncodedObjectInspector() {
    return encoded;
  }

  @Override
  public Variant getVariant(Object data) {
    if (data == null) {
      return null;
    }
    return new Variant(copy(valueField, data), copy(metadataField, data));
  }

  private byte[] copy(StructField field, Object data) {
    BytesWritable bytes = ((BinaryObjectInspector) field.getFieldObjectInspector())
        .getPrimitiveWritableObject(encoded.getStructFieldData(data, field));
    return Arrays.copyOf(bytes.getBytes(), bytes.getLength());
  }

  /** Equal when they wrap the same encoded inspector, so lazy inspector caches keyed on it can share entries. */
  @Override
  public boolean equals(Object o) {
    return this == o || o != null && getClass() == o.getClass()
        && encoded.equals(((StructEncodedVariantObjectInspector) o).encoded);
  }

  @Override
  public int hashCode() {
    return encoded.hashCode();
  }
}
