/**
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

package org.apache.hive.hcatalog.mapreduce;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

public class HCatFileUtil {

  // regex of the form: ${column name}. Following characters are not allowed in column name:
  // whitespace characters, /, {, }, \
  private static final Pattern customPathPattern = Pattern.compile("(\\$\\{)([^\\s/\\{\\}\\\\]+)(\\})");

  // This method parses the custom dynamic path and replaces each occurrence
  // of column name within regex pattern with its corresponding value, if provided
  public static String resolveCustomPath(OutputJobInfo jobInfo,
      Map<String, String> dynPartKVs, boolean createRegexPath) {
    // get custom path string
    String customPath = jobInfo.getCustomDynamicPath();
    // create matcher for custom path
    Matcher matcher = customPathPattern.matcher(customPath);
    // get the set of all partition columns in custom path
    HashSet<String> partColumns = new HashSet<String>();
    Map<String, String> partKVs = dynPartKVs != null ? dynPartKVs :
      jobInfo.getPartitionValues();

    // build the final custom path string by replacing each column name with
    // its value, if provided
    StringBuilder sb = new StringBuilder();
    int previousEndIndex = 0;
    while (matcher.find()) {
      // append the path substring since previous match
      sb.append(customPath.substring(previousEndIndex, matcher.start()));
      if (createRegexPath) {
        // append the first group within pattern: "${"
        sb.append(matcher.group(1));
      }

      // column name is the second group from current match
      String columnName = matcher.group(2).toLowerCase();
      partColumns.add(columnName);

      // find the value of matched column
      String columnValue = partKVs.get(columnName);
      // if column value is provided, replace column name with value
      if (columnValue != null) {
        sb.append(columnValue);
      } else {
        sb.append("__HIVE_DEFAULT_PARTITION__");
      }

      if (createRegexPath) {
        // append the third group within pattern: "}"
        sb.append(matcher.group(3));
      }

      // update startIndex
      previousEndIndex = matcher.end();
    }

    // append the trailing path string, if any
    if (previousEndIndex < customPath.length()) {
      sb.append(customPath.substring(previousEndIndex, customPath.length()));
    }

    // validate that the set of partition columns found in custom path must match
    // the set of dynamic partitions
    if (partColumns.size() != jobInfo.getDynamicPartitioningKeys().size()) {
      throw new IllegalArgumentException("Unable to configure custom dynamic location, "
          + " mismatch between number of dynamic partition columns obtained[" + partColumns.size()
          + "] and number of dynamic partition columns required["
          + jobInfo.getDynamicPartitioningKeys().size() + "]");
    }

    return sb.toString();
  }

  public static void getPartKeyValuesForCustomLocation(Map<String, String> partSpec,
      OutputJobInfo jobInfo, String partitionPath) {
    // create matchers for custom path string as well as actual dynamic partition path created
    Matcher customPathMatcher = customPathPattern.matcher(jobInfo.getCustomDynamicPath());
    Matcher dynamicPathMatcher = customPathPattern.matcher(partitionPath);

    while (customPathMatcher.find() && dynamicPathMatcher.find()) {
      // get column name from custom path matcher and column value from dynamic path matcher
      partSpec.put(customPathMatcher.group(2), dynamicPathMatcher.group(2));
    }

    // add any partition key values provided as part of job info
    partSpec.putAll(jobInfo.getPartitionValues());
  }

  public static void setCustomPath(String customPathFormat, OutputJobInfo jobInfo) {
    // find the root of all custom paths from custom pattern. The root is the
    // largest prefix in input pattern string that doesn't match customPathPattern
    Path customPath = new Path(customPathFormat);
    URI customURI = customPath.toUri();
    while (customPath != null && !customPath.toString().isEmpty()) {
      Matcher m = customPathPattern.matcher(customPath.toString());
      if (!m.find()) {
        break;
      }
      customPath = customPath.getParent();
    }

    URI rootURI = customPath.toUri();
    URI childURI = rootURI.relativize(customURI);
    jobInfo.setCustomDynamicLocation(rootURI.getPath(), childURI.getPath());
  }
}
