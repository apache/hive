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

import java.util.List;

public class OpTraits {

  private List<List<String>> bucketColNames;
  private List<List<String>> sortColNames;
  private int numBuckets;
  private int numReduceSinks;
  private int bucketingVersion;

  public OpTraits(List<List<String>> bucketColNames, int numBuckets,
      List<List<String>> sortColNames, int numReduceSinks,
                  int bucketingVersion) {
    this.bucketColNames = bucketColNames;
    this.numBuckets = numBuckets;
    this.sortColNames = sortColNames;
    this.numReduceSinks = numReduceSinks;
    this.bucketingVersion = bucketingVersion;
  }

  public List<List<String>> getBucketColNames() {
    return bucketColNames;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setBucketColNames(List<List<String>> bucketColNames) {
    this.bucketColNames = bucketColNames;
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


  public void setNumReduceSinks(int numReduceSinks) {
    this.numReduceSinks = numReduceSinks;
  }

  public int getNumReduceSinks() {
    return this.numReduceSinks;
  }

  public void setBucketingVersion(int bucketingVersion) {
    this.bucketingVersion = bucketingVersion;
  }

  public int getBucketingVersion() {
    return bucketingVersion;
  }

  @Override
  public String toString() {
    return "{ bucket column names: " + bucketColNames + "; sort column names: "
        + sortColNames + "; bucket count: " + numBuckets + "; bucketing version: " + bucketingVersion + " }";
  }
}
