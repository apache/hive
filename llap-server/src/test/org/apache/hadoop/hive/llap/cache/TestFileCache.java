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
package org.apache.hadoop.hive.llap.cache;

import com.google.common.base.Function;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class TestFileCache {

  @Test
  public void testFileCacheMetadata() {
    ConcurrentHashMap<Object, FileCache<Object>> cache = new ConcurrentHashMap<>();
    Object fileKey = 1234L;
    Function<Void, Object> f = a -> new Object();
    CacheTag tag = CacheTag.build("test_table");

    FileCache<Object> result = FileCache.getOrAddFileSubCache(cache, fileKey, f, tag);

    assertEquals(fileKey, result.getFileKey());
    assertEquals(tag, result.getTag());
  }
}
