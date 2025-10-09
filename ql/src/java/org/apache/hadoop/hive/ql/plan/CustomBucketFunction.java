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

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A bucketing logic based on flexible transformations, e.g. partition transforms of Apache Iceberg.
 */
@Unstable
public interface CustomBucketFunction extends Serializable {
  /**
   * Creates a new CustomBucketFunction which accepts only the given columns.
   *
   * @param retainedColumns the flags of retained columns
   * @return a new CustomBucketFunction accepting the retained columns
   *         Optional.empty() if the subset can't generate a bucket id
   */
  @Unstable
  Optional<CustomBucketFunction> select(boolean[] retainedColumns);

  /**
   * Initializes this bucket function with the given ObjectInspectors.
   *
   * @param bucketFieldInspectors the ObjectInspectors which decode the bucketFields
   */
  @Unstable
  void initialize(ObjectInspector[] bucketFieldInspectors);

  /**
   * Returns the column names in the source table.
   */
  @Unstable
  List<String> getSourceColumnNames();

  /**
   * Returns the number of buckets.
   */
  @Unstable
  int getNumBuckets();

  /**
   * Produces a hash code from the given arguments.
   *
   * @param bucketFields the source fields
   * @return the bucket hash code
   */
  @Unstable
  int getBucketHashCode(Object[] bucketFields);
}
