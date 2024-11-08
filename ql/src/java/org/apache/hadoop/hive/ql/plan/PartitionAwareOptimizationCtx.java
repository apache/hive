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

import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;

/**
 * A context to support Partition-Aware Optimization leveraging storage layouts.
 */
@Unstable
public class PartitionAwareOptimizationCtx {
  private final CustomBucketFunction bucketFunction;

  /**
   * Constructs a new PartitionAwareOptimizationCtx.
   *
   * @param bucketFunction the bucket function
   */
  @Unstable
  public PartitionAwareOptimizationCtx(CustomBucketFunction bucketFunction) {
    this.bucketFunction = bucketFunction;
  }

  /**
   * Returns the bucket function for Partition-Aware Optimization.
   */
  @Unstable
  public CustomBucketFunction getBucketFunction() {
    return bucketFunction;
  }
}
