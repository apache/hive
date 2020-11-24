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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.localcache.HMSPartitionConverter;
import org.apache.hadoop.hive.metastore.localcache.HMSPartitionNamesConverter;
import org.apache.hadoop.hive.metastore.localcache.HMSTableConverter;

import java.util.List;

/**
 * Interface to get specific object converters to help with HMS caching.
 */
public interface HMSConverter {

  /**
   * Get the partition converter.
   */
  public HMSPartitionConverter getPartitionConverter(GetPartitionsByNamesRequest rqst,
      Table table);

  /**
   * Get the Table converter.
   */
  public HMSTableConverter getTableConverter(GetTableRequest rqst);

  /**
   * Get the PartitionNames converter.
   */
  public HMSPartitionNamesConverter getPartitionNamesConverter(GetPartitionNamesPsRequest rqst,
      GetPartitionNamesPsResponse result);
}
