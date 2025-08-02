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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
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

  private String fieldName;

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

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String transformTypeString() {
    if (transformType == null) {
      return null;
    }
    return transformType.name() + Optional.ofNullable(transformParam).map(width ->
        "[" + width + "]").orElse("");
  }
    
  public static String toNamedStruct(List<TransformSpec> partTransformSpec, Configuration conf) {
    return "named_struct(" +
      partTransformSpec.stream().map(spec ->
          "'" + spec.getFieldName() + "', " + spec.toHiveExpr(conf))
        .collect(Collectors.joining(", ")) +
      ")";
  }
  
  public String toHiveExpr(Configuration conf) {
    String identifier = unparseIdentifier(columnName, conf);
    if (transformType == TransformSpec.TransformType.IDENTITY) {
      return identifier;
    }
    String fn = "iceberg_" + transformType.name().toLowerCase() + "(" + identifier;
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
