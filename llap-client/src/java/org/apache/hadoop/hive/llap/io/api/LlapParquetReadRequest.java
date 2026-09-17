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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap.io.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;

/**
 * Everything that identifies a single Parquet split served through the LLAP data cache: the file
 * (its optional {@code fileKey}, {@code path} and {@code tag}), the split ({@code offset} and
 * {@code length}), the projection ({@code tableIncludedCols}), and the defaults for columns
 * absent from the file ({@code initialDefaults}). Kept together so
 * {@link LlapIo#llapVectorizedParquetReaderForPath} stays a three-parameter call with the job
 * conf and reporter alongside.
 */
public record LlapParquetReadRequest(
    Object fileKey,
    Path path,
    CacheTag tag,
    List<Integer> tableIncludedCols,
    long offset,
    long length,
    Map<String, Object> initialDefaults) {
}
