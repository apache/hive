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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.Variant;

/**
 * Utilities for variant shredding support in Parquet writers.
 *
 * <p>This includes:
 * <ul>
 *   <li>Detecting whether shredding should be enabled for a schema</li>
 *   <li>Discovering eligible VARIANT fields (top-level or nested in structs; not in lists/maps)</li>
 *   <li>Building a {@link VariantShreddingFunction} for data-driven typed_value schema inference</li>
 * </ul>
 */
public class VariantUtil {

  private VariantUtil() {
  }

  /**
   * A VARIANT field in an Iceberg {@link Schema} that can be used for shredding.
   *
   * <p>Shredding is supported for top-level VARIANT columns or VARIANT fields nested in structs.
   * VARIANT fields stored inside lists or maps are excluded because schema accessors do not retrieve
   * list/map contents.
   */
  public record VariantField(int fieldId, Accessor<StructLike> accessor, String[] path) {
  }

  /**
   * Check if variant shredding is enabled via table properties.
   */
  public static boolean isVariantShreddingEnabled(Map<String, String> properties) {
    String shreddingEnabled = properties.get(InputFormatConfig.VARIANT_SHREDDING_ENABLED);
    return Boolean.parseBoolean(shreddingEnabled);
  }

  public static boolean isShreddable(Object value) {
    if (value instanceof Variant variant) {
      return variant.value().type() != PhysicalType.NULL;
    }
    return false;
  }

  public static List<VariantField> variantFieldsForShredding(
      Map<String, String> properties, Schema schema) {
    if (!isVariantShreddingEnabled(properties)) {
      return List.of();
    }
    return variantFieldsForShredding(schema);
  }

  /**
   * Returns all VARIANT fields that are eligible for shredding: top-level VARIANT columns and VARIANT
   * fields nested in structs (excluding lists/maps).
   */
  private static List<VariantField> variantFieldsForShredding(Schema schema) {
    List<VariantField> results = Lists.newArrayList();
    new VariantFieldVisitor(schema).collect(results);
    return results;
  }

  public static boolean shouldUseVariantShredding(Map<String, String> properties, Schema schema) {
    return isVariantShreddingEnabled(properties) && hasVariantFields(schema);
  }

  private static boolean hasVariantFields(Schema schema) {
    return new VariantFieldVisitor(schema).hasVariantField();
  }

  public static VariantShreddingFunction variantShreddingFunc(
      Record sampleRecord, Schema schema) {

    return (id, ignoredName) -> {
      // Validate the field exists and is a variant type
      Types.NestedField field = schema.findField(id);

      if (field == null || !field.type().isVariantType()) {
        return null; // Not a variant field, no shredding
      }

      // If we have a sample record, try to generate schema from actual data
      if (sampleRecord != null) {
        try {
          // NOTE: Parquet conversion passes the field's local name, not the full path.
          // Use an accessor to support variant fields nested in structs.
          Accessor<StructLike> accessor = schema.accessorForField(id);
          Object variantValue = accessor != null ? accessor.get(sampleRecord) : null;
          if (variantValue instanceof Variant variant) {
            return ParquetVariantUtil.toParquetSchema(variant.value());
          }
        } catch (RuntimeException e) {
          // Fall through to default schema
        }
      }
      return null;
    };
  }

  private static final class VariantFieldVisitor {
    private final Schema schema;

    private VariantFieldVisitor(Schema schema) {
      this.schema = schema;
    }

    private boolean hasVariantField() {
      return hasVariantField(schema.asStruct());
    }

    private boolean hasVariantField(Types.StructType struct) {
      for (Types.NestedField field : struct.fields()) {
        if (field.type().isVariantType()) {
          // Accessors don't retrieve values contained in lists/maps so this enforces the "struct-only"
          // nesting rule for shredding.
          if (schema.accessorForField(field.fieldId()) != null) {
            return true;
          }
        } else if (field.type().isStructType() && hasVariantField(field.type().asStructType())) {
          return true;
        }
        // Do not recurse into List or Map (shredding is not supported there)
      }
      return false;
    }

    private void collect(List<VariantField> results) {
      collect(schema.asStruct(), Lists.newArrayList(), results);
    }

    private void collect(
        Types.StructType struct,
        List<String> parents,
        List<VariantField> results) {

      for (Types.NestedField field : struct.fields()) {
        if (field.type().isVariantType()) {
          // Accessors don't retrieve values contained in lists/maps so this enforces the "struct-only"
          // nesting rule for shredding.
          Accessor<StructLike> accessor = schema.accessorForField(field.fieldId());
          if (accessor == null) {
            continue;
          }

          String[] path = new String[parents.size() + 1];
          for (int i = 0; i < parents.size(); i++) {
            path[i] = parents.get(i);
          }
          path[parents.size()] = field.name();

          results.add(new VariantField(field.fieldId(), accessor, path));

        } else if (field.type().isStructType()) {
          parents.add(field.name());
          collect(field.type().asStructType(), parents, results);
          parents.removeLast();
        }
        // Do not recurse into List or Map (shredding is not supported there)
      }
    }
  }
}
