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
import java.util.OptionalInt;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A partition function of a specific storage, e.g. the bucket partition transform of Iceberg.
 */
public interface CustomPartitionFunction {

  /**
   * Returns the column names which this partition function processes.
   */
  List<String> getColumnNames();

  /**
   * Returns the number of buckets when it is a bucketing function. Otherwise, OptionalInt.empty().
   */
  OptionalInt getNumBuckets();

  /**
   * Returns the ObjectInspector which interprets the partition value.
   */
  ObjectInspector getObjectInspector();

  /**
   * Extracts a partition value from the given objects.
   *
   * @param partitionFields the objects retaining the source values
   * @return the partition value
   */
  Object getPartitionValue(Object[] partitionFields);

  /**
   * Initializes this partition function with the given schema.
   *
   * @param partitionFieldInspectors the ObjectInspectors which interprets the partitionFields
   */
  void initialize(ObjectInspector[] partitionFieldInspectors);
}
