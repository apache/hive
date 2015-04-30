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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Abstract class that allows authorization of operations on partition-sets.
 */
public abstract class HiveMultiPartitionAuthorizationProviderBase extends HiveAuthorizationProviderBase {

  /**
   * Authorization method for partition sets.
   * @param table The table in question
   * @param partitions An Iterable representing the partition-set
   * @param requiredReadPrivileges Read-privileges required
   * @param requiredWritePrivileges Write-privileges required
   * @throws HiveException
   * @throws AuthorizationException
   */
  public abstract void authorize(Table table, Iterable<Partition> partitions,
                                 Privilege[] requiredReadPrivileges, Privilege[] requiredWritePrivileges)
      throws HiveException, AuthorizationException;
}
