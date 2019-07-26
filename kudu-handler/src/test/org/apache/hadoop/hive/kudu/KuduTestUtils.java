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
package org.apache.hadoop.hive.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

import java.util.Arrays;

/**
 * Utilities for shared kudu-handler testing logic.
 */
public final class KuduTestUtils {

  private KuduTestUtils() {}

  public static Schema getAllTypesSchema() {
    return new org.apache.kudu.Schema(Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT8).key(true)
            .comment("The key column.").build(),
        new ColumnSchema.ColumnSchemaBuilder("int16", Type.INT16).build(),
        new ColumnSchema.ColumnSchemaBuilder("int32", Type.INT32).build(),
        new ColumnSchema.ColumnSchemaBuilder("int64", Type.INT64).build(),
        new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build(),
        new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build(),
        new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build(),
        new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build(),
        new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY).build(),
        new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS).build(),
        new ColumnSchema.ColumnSchemaBuilder("decimal", Type.DECIMAL)
            .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                .precision(5).scale(3).build())
            .build(),
        new ColumnSchema.ColumnSchemaBuilder("null", Type.STRING).nullable(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("default", Type.INT32).defaultValue(1).build()
    ));
  }

}
