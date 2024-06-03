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

package org.apache.hadoop.hive.ql.ddl;

/**
 * Marker interface for all DDL operation descriptors.
 */
public interface DDLDesc {
  /**
   * DDL Desc for operation which needs write id.
   */
  interface DDLDescWithWriteId extends DDLDesc {
    void setWriteId(long writeId);
    String getFullTableName();
    // We have to advance the Write Id during DDL for transactional tables, so that
    // we can provide strong consistency when serving the metadata from the cache.
    // Override this method, only if you are sure that advancing the write Id is not required for your DDL.
    default boolean mayNeedWriteId() {
      return true;
    }
  }
}
