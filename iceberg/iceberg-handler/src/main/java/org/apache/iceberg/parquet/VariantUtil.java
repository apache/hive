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

package org.apache.iceberg.parquet;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;

public class VariantUtil {

  private VariantUtil() {
  }

  /**
   * Create a VariantShreddingFunction if variant shredding is enabled and the schema has variant columns.
   *
   * @param schema        The Iceberg schema
   * @param sampleRecord  A sample record to infer variant schemas from actual data (can be null)
   * @param properties    Table properties to check if variant shredding is enabled
   * @return An Optional containing the VariantShreddingFunction if applicable
   */
  public static Optional<VariantShreddingFunction> variantShreddingFunc(
      Schema schema,
      Supplier<Record> sampleRecord,
      Map<String, String> properties) {

    // Preconditions: must have variant columns + property enabled
    if (!hasVariantColumns(schema) || !isVariantShreddingEnabled(properties)) {
      return Optional.empty();
    }

    VariantShreddingFunction fn =
        constructVariantShreddingFunc(sampleRecord.get(), schema);

    return Optional.of(fn);
  }

  private static VariantShreddingFunction constructVariantShreddingFunc(
      Record sampleRecord, Schema schema) {

    return (id, name) -> {
      // Validate the field exists and is a variant type
      Types.NestedField field = schema.findField(id);

      if (field == null || !(field.type() instanceof Types.VariantType)) {
        return null; // Not a variant field, no shredding
      }

      // If we have a sample record, try to generate schema from actual data
      if (sampleRecord != null) {
        try {
          Object variantValue = sampleRecord.getField(name);
          if (variantValue instanceof Variant variant) {
            // Use ParquetVariantUtil to generate schema from actual variant value
            return ParquetVariantUtil.toParquetSchema(variant.value());
          }
        } catch (Exception e) {
          // Fall through to default schema
        }
      }
      return null;
    };
  }

  /**
   * Check if the schema contains any variant columns.
   */
  private static boolean hasVariantColumns(Schema schema) {
    return schema.columns().stream()
        .anyMatch(field -> field.type() instanceof Types.VariantType);
  }

  /**
   * Check if variant shredding is enabled via table properties.
   */
  private static boolean isVariantShreddingEnabled(Map<String, String> properties) {
    String shreddingEnabled = properties.get(InputFormatConfig.VARIANT_SHREDDING_ENABLED);
    return Boolean.parseBoolean(shreddingEnabled);
  }

}
