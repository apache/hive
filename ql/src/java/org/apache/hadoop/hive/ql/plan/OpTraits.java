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

package org.apache.hadoop.hive.ql.plan;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;

public class OpTraits {

  private final List<List<String>> bucketColNames;
  private final List<CustomBucketFunction> customBucketFunctions;
  private List<List<String>> sortColNames;
  private int numBuckets;
  private int numReduceSinks;

  public OpTraits(List<List<String>> bucketColNames, List<CustomBucketFunction> customBucketFunctions, int numBuckets,
      List<List<String>> sortColNames,
      int numReduceSinks) {
    if (bucketColNames == null) {
      Preconditions.checkArgument(customBucketFunctions == null);
    } else {
      Preconditions.checkArgument(bucketColNames.size() == customBucketFunctions.size());
    }
    this.bucketColNames = bucketColNames;
    this.customBucketFunctions = customBucketFunctions;
    this.numBuckets = numBuckets;
    this.sortColNames = sortColNames;
    this.numReduceSinks = numReduceSinks;
  }

  public List<List<String>> getBucketColNames() {
    return bucketColNames;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public void setSortColNames(List<List<String>> sortColNames) {
    this.sortColNames = sortColNames;
  }

  public List<List<String>> getSortCols() {
    return sortColNames;
  }

  public List<CustomBucketFunction> getCustomBucketFunctions() {
    return customBucketFunctions;
  }

  public boolean hasCustomBucketFunction() {
    return customBucketFunctions != null && customBucketFunctions.stream().anyMatch(Objects::nonNull);
  }

  public void setNumReduceSinks(int numReduceSinks) {
    this.numReduceSinks = numReduceSinks;
  }

  public int getNumReduceSinks() {
    return this.numReduceSinks;
  }

  @Override
  public String toString() {
    return "{ bucket column names: " + bucketColNames + "; custom partition functions: " + customBucketFunctions
        + "; sort column names: " + sortColNames + "; bucket count: " + numBuckets + "; num reduce sinks: "
        + numReduceSinks + "}";
  }
}
