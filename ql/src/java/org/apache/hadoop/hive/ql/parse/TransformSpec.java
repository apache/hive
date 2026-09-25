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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;

import java.util.List;
import java.util.Set;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.ql.metadata.HiveUtils.unparseIdentifier;

public class TransformSpec {

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)]");

  public enum TransformType {
    IDENTITY, YEAR, MONTH, DAY, HOUR, TRUNCATE, BUCKET, VOID
  }

  private String columnName;
  private TransformType transformType;
  private Integer transformParam;

  public TransformSpec() {
  }

  public TransformSpec(String columnName, TransformType transformType, Integer transformParam) {
    this.columnName = columnName;
    this.transformType = transformType;
    this.transformParam = transformParam;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public TransformType getTransformType() {
    return transformType;
  }

  public void setTransformType(TransformType transformType) {
    this.transformType = transformType;
  }

  public Integer getTransformParam() {
    return transformParam;
  }

  public void setTransformParam(Integer transformParam) {
    this.transformParam = transformParam;
  }


  public String transformTypeString() {
    if (transformType == null) {
      return null;
    }
    return transformType.name() + Optional.ofNullable(transformParam).map(width ->
        "[" + width + "]").orElse("");
  }
    
  /**
   * The partition transforms the statement being compiled declared, or null if it declared none.
   * A CREATE has to be read this way: the table it describes does not exist to be asked yet.
   */
  @SuppressWarnings("unchecked")
  public static List<TransformSpec> fromQueryState(Configuration conf) {
    return SessionStateUtil.getResource(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC)
        .map(spec -> (List<TransformSpec>) spec)
        .orElse(null);
  }


  /**
   * Builds the struct of source values a stats gather carries alongside each group. The transforms
   * decide which rows form a partition, but only the values they were applied to let the table name
   * it, and every row of a group belongs to one partition, so any of them answers for the group.
   *
   * <p>{@code identity(p), month(ts), bucket(id, 4)} yields
   * <pre>named_struct('p', `p`, 'ts', min(`ts`), 'id', min(`id`))</pre>
   */
  public static String toSourceStructExpr(List<TransformSpec> partTransformSpec, Configuration conf) {
    Set<String> groupedColumns = partTransformSpec.stream()
        .filter(spec -> spec.getTransformType() == TransformType.IDENTITY)
        .map(TransformSpec::getColumnName)
        .collect(Collectors.toSet());

    return partTransformSpec.stream()
        .map(TransformSpec::getColumnName).distinct()
        .map(columnName -> {
          String identifier = unparseIdentifier(columnName, conf);
          // an identity transform groups by the column itself, which already answers for the group
          return "'" + columnName + "', " +
              (groupedColumns.contains(columnName) ? identifier : "min(" + identifier + ")");
        })
        .collect(Collectors.joining(", ", "named_struct(", ")"));
  }
  
  public String toHiveExpr(Configuration conf) {
    return toHiveExpr(unparseIdentifier(columnName, conf));
  }

  /** The transform applied to an operand, which is a column of its own table or a value of one. */
  public String toHiveExpr(String operand) {
    if (transformType == TransformSpec.TransformType.IDENTITY) {
      return operand;
    }
    String fn = "iceberg_" + transformType.name().toLowerCase() + "(" + operand;
    switch (transformType) {
      case BUCKET:
      case TRUNCATE:
        fn += ", " + transformParam;
    }
    return  fn + ")";
  }

  public static TransformType fromString(String transformString) {
    Matcher widthMatcher = HAS_WIDTH.matcher(transformString);
    if (widthMatcher.matches()) {
      transformString = widthMatcher.group(1);
    }
    return TransformType.valueOf(transformString.toUpperCase(Locale.ROOT));
  }

  public static TransformSpec fromString(String transfromString, String columnName) {
    Matcher widthMatcher = HAS_WIDTH.matcher(transfromString);
    if (widthMatcher.matches()) {
      transfromString = widthMatcher.group(1);
      int width = Integer.parseInt(widthMatcher.group(2));
      return new TransformSpec(columnName, TransformType.valueOf(transfromString.toUpperCase(Locale.ROOT)),
          width);
    }
    return new TransformSpec(columnName, TransformType.valueOf(transfromString.toUpperCase(Locale.ROOT)),
        null);
  }

  public static TransformSpec fromStringWithColumnName(String transformString) {
    if (transformString == null || !transformString.contains("(")) {
      return new TransformSpec(transformString, TransformType.IDENTITY, null);
    }
    transformString = transformString.trim();

    // Extract transform type
    String transformName = transformString.split("\\(")[0].toLowerCase(Locale.ROOT);
    String innerContent = transformString.split("\\(")[1].split("\\)")[0].trim();

    // Normalize transform name (convert "years" -> "year", "months" -> "month", etc.)
    transformName =
        transformName.endsWith("s") ? transformName.substring(0, transformName.length() - 1) : transformName;

    // Handle transforms with width (truncate, bucket)
    if (transformName.equals("truncate") || transformName.equals("bucket")) {
      String[] parts = innerContent.split(",");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid format for " + transformName + ": " + transformString);
      }
      int width = Integer.parseInt(parts[0].trim()); // First is width
      String columnName = parts[1].trim(); // Second is column
      return new TransformSpec(columnName, TransformType.valueOf(transformName.toUpperCase(Locale.ROOT)),
          width);
    }

    // Handle other cases (year, month, day, hour)
    return new TransformSpec(innerContent, TransformType.valueOf(transformName.toUpperCase(Locale.ROOT)),
        null);
  }
}
