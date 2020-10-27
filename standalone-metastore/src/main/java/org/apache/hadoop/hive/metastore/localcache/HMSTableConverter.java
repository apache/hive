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

package org.apache.hadoop.hive.metastore.localcache;

import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;

import java.util.List;

/**
 * Interface to convert a HMS fetched result into an engine specific converted
 * Result.
 */
public interface HMSTableConverter {
  /**
   * Convert a Table return an HMS Result.
   * @param table The Result fetched from the HMS server that needs to be converted.
   */
  public GetTableResult convertTable(GetTableResult result);

  /**
   * Clone a table result with the given GetTableResult
   * @param table The Result to be cloned.
   */
  public GetTableResult cloneTableResult(GetTableResult result);
}
