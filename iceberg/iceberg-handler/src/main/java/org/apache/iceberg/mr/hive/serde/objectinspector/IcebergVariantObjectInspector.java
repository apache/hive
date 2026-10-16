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
import java.nio.ByteOrder;
import org.apache.hadoop.hive.serde2.objectinspector.VariantObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;

/**
 * Inspects Iceberg {@link org.apache.iceberg.variants.Variant} values as Hive VARIANT: each value is serialized
 * into its metadata and value binaries on access.
 */
public final class IcebergVariantObjectInspector implements VariantObjectInspector {

  private static final IcebergVariantObjectInspector INSTANCE = new IcebergVariantObjectInspector();

  private IcebergVariantObjectInspector() {
  }

  public static IcebergVariantObjectInspector get() {
    return INSTANCE;
  }

  @Override
  public Variant getVariant(Object data) {
    if (data == null) {
      return null;
    }
    org.apache.iceberg.variants.Variant variant = (org.apache.iceberg.variants.Variant) data;
    ByteBuffer metadata = ByteBuffer.allocate(variant.metadata().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    variant.metadata().writeTo(metadata, 0);
    ByteBuffer value = ByteBuffer.allocate(variant.value().sizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
    variant.value().writeTo(value, 0);
    return new Variant(value.array(), metadata.array());
  }
}
