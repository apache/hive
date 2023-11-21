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

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransformSpec {

  private static final Pattern HAS_WIDTH = Pattern.compile("(\\w+)\\[(\\d+)\\]");
  public enum TransformType {
    IDENTITY, YEAR, MONTH, DAY, HOUR, TRUNCATE, BUCKET, VOID
  }

  private String columnName;
  private TransformType transformType;
  private Optional<Integer> transformParam;

  public TransformSpec() {
  }

  public TransformSpec(String columnName, TransformType transformType, Optional<Integer> transformParam) {
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

  public Optional<Integer> getTransformParam() {
    return transformParam;
  }

  public void setTransformParam(Optional<Integer> transformParam) {
    this.transformParam = transformParam;
  }

  public static TransformType fromString(String transformString) {
    Matcher widthMatcher = HAS_WIDTH.matcher(transformString);
    if (widthMatcher.matches()) {
      transformString = widthMatcher.group(1);
    }
    return TransformType.valueOf(transformString.toUpperCase(Locale.ROOT));
  }
}
