/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

/**
 * Task counters of the LLAP metadata cache lookups and puts made by
 * {@link org.apache.iceberg.data.CachingDeleteLoader}. PUT and PUT_BYTES count what is offered to the cache: a put
 * that races with another one keeps the existing entry.
 */
public enum DeleteCacheCounters {
  DELETE_CACHE_HIT,
  DELETE_CACHE_MISS,
  DELETE_CACHE_PUT,
  DELETE_CACHE_PUT_BYTES,
  DELETE_CACHE_PUT_FAILED,
  DELETE_CACHE_PUT_FAILED_BYTES
}
