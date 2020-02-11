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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

/**
 * The proxy interface that metastore uses for variety of QL operations (metastore can't depend
 * on QL because QL depends on metastore; creating metastore-client module would be a proper way
 * to solve this problem).
 */
public interface PartitionExpressionProxy {

  /**
   * Converts serialized Hive expression into filter in the format suitable for Filter.g.
   * The isnull and isnotnull expressions are converted to = and != default partition respectively
   * for push down to metastore SQL.
   * @param exprBytes Serialized expression.
   * @param defaultPartitionName Default partition name.
   * @return Filter string.
   */
  String convertExprToFilter(byte[] exprBytes, String defaultPartitionName) throws MetaException;

  /**
   * Filters the partition names via serialized Hive expression.
   * @param partColumns Partition columns in the underlying table.
   * @param expr Serialized expression.
   * @param defaultPartitionName Default partition name from job or server configuration.
   * @param partitionNames Partition names; the list is modified in place.
   * @return Whether there were any unknown partitions preserved in the name list.
   */
  boolean filterPartitionsByExpr(List<FieldSchema> partColumns,
      byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException;

  /**
   * Determines the file metadata type from input format of the source table or partition.
   * @param inputFormat Input format name.
   * @return The file metadata type.
   */
  FileMetadataExprType getMetadataType(String inputFormat);

  /**
   * Gets a separate proxy that can be used to call file-format-specific methods.
   * @param type The file metadata type.
   * @return The proxy.
   */
  FileFormatProxy getFileFormatProxy(FileMetadataExprType type);

  /**
   * Creates SARG from serialized representation.
   * @param expr SARG, serialized as Kryo.
   * @return SARG.
   */
  SearchArgument createSarg(byte[] expr);
}
