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

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.variant.Variant;

/**
 * Inspects a VARIANT value: a metadata binary and a value binary in the Iceberg/Parquet variant encoding.
 * <p>
 * VARIANT is a category of its own, at the same level as STRUCT, MAP and LIST. Each serde inspects its own
 * objects; {@link StandardVariantObjectInspector} holds {@link Variant} objects.
 */
public interface VariantObjectInspector extends ObjectInspector {

  @Override
  default Category getCategory() {
    return Category.VARIANT;
  }

  @Override
  default String getTypeName() {
    return serdeConstants.VARIANT_TYPE_NAME;
  }

  /**
   * The variant held by {@code data}, or null when {@code data} is null.
   * The result shares no mutable buffer with {@code data}.
   */
  Variant getVariant(Object data);
}
