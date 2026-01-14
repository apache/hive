/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * When using msck repair table with custom partitioning patterns, we need to capture
 * the partition keys from the pattern, and use those to construct a regex which will
 * match the paths and extract the partition key values.*/
public final class DynamicPartitioningCustomPattern {

  private final String customPattern;
  private final Pattern partitionCapturePattern;
  private final List<String> partitionColumns;

    // regex of the form: ${column name}. Following characters are not allowed in column name:
    // whitespace characters, /, {, }, \
  public static final Pattern customPathPattern = Pattern.compile("(\\$\\{)([^\\s/\\{\\}\\\\]+)(\\})");

  private DynamicPartitioningCustomPattern(String customPattern, Pattern partitionCapturePattern,
                                           List<String> partitionColumns) {
    this.customPattern = customPattern;
    this.partitionCapturePattern = partitionCapturePattern;
    this.partitionColumns = partitionColumns;
  }

  /**
   * @return stored custom pattern string
   * */
  public String getCustomPattern() {
    return customPattern;
  }

  /**
   * @return stored custom pattern regex matcher
   * */
  public Pattern getPartitionCapturePattern() {
    return partitionCapturePattern;
  }

  /**
   * @return list of partition key columns
   * */
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public static class Builder {
    private String customPattern;

    public Builder setCustomPattern(String customPattern) {
      this.customPattern = customPattern;
      return this;
    }

    /**
     * Constructs the regex to match the partition values in a path based on the custom pattern.
     *
     * @return custom partition pattern matcher */
    public DynamicPartitioningCustomPattern build() {
      StringBuffer sb = new StringBuffer();
      Matcher m = customPathPattern.matcher(customPattern);
      List<String> partColumns = new ArrayList<>();
      while (m.find()) {
        m.appendReplacement(sb, "(.*)");
        partColumns.add(m.group(2));
      }
      m.appendTail(sb);
      return new DynamicPartitioningCustomPattern(customPattern, Pattern.compile(sb.toString()), partColumns);
    }
  }
}
