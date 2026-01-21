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

package org.apache.iceberg.mr.hive;

import java.util.List;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;

public class SchemaUtils {

  private static final String UNSUPPORTED_TRANSFORM = "Unsupported transform: %s";

  private SchemaUtils() {
  }

  public static UnboundTerm<Object> toTerm(TransformSpec spec) {
    if (spec == null) {
      return null;
    }

    return switch (spec.getTransformType()) {
      case YEAR -> Expressions.year(spec.getColumnName());
      case MONTH -> Expressions.month(spec.getColumnName());
      case DAY -> Expressions.day(spec.getColumnName());
      case HOUR -> Expressions.hour(spec.getColumnName());
      case TRUNCATE -> Expressions.truncate(spec.getColumnName(), spec.getTransformParam());
      case BUCKET -> Expressions.bucket(spec.getColumnName(), spec.getTransformParam());
      case IDENTITY -> Expressions.ref(spec.getColumnName());
      default -> throw new UnsupportedOperationException(
          UNSUPPORTED_TRANSFORM.formatted(spec.getTransformType()));
    };
  }

  public static PartitionSpec createPartitionSpec(Schema schema, List<TransformSpec> partitionBy) {
    if (partitionBy.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);

    partitionBy.forEach(spec -> {
      switch (spec.getTransformType()) {
        case IDENTITY -> specBuilder.identity(spec.getColumnName().toLowerCase());
        case YEAR -> specBuilder.year(spec.getColumnName());
        case MONTH -> specBuilder.month(spec.getColumnName());
        case DAY -> specBuilder.day(spec.getColumnName());
        case HOUR -> specBuilder.hour(spec.getColumnName());
        case TRUNCATE -> specBuilder.truncate(spec.getColumnName(), spec.getTransformParam());
        case BUCKET -> specBuilder.bucket(spec.getColumnName(), spec.getTransformParam());
        default -> throw new UnsupportedOperationException(
            UNSUPPORTED_TRANSFORM.formatted(spec.getTransformType()));
      }
    });

    return specBuilder.build();
  }
}
