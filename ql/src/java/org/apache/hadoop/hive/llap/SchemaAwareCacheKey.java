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

package org.apache.hadoop.hive.llap;

import java.util.Objects;

/**
 * Cache key that stores schema hash. Useful caching serdes that do not have a self-describing schema built in.
 */
public class SchemaAwareCacheKey {

  public static final int NO_SCHEMA_HASH = -1;

  private final Object fileId;
  private final int schemaHash;

  private SchemaAwareCacheKey(Object fileId, int schemaHash) {
    this.fileId = fileId;
    this.schemaHash = schemaHash;
  }

  public static Object buildCacheKey(Object fileId, int schemaHash) {
    if (schemaHash == NO_SCHEMA_HASH) {
      return fileId;
    } else {
      return new SchemaAwareCacheKey(fileId, schemaHash);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaAwareCacheKey that = (SchemaAwareCacheKey) o;
    return schemaHash == that.schemaHash && Objects.equals(fileId, that.fileId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileId, schemaHash);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{fileId=").append(fileId).append(", schemaHash=").append(schemaHash).append('}');
    return sb.toString();
  }
}
