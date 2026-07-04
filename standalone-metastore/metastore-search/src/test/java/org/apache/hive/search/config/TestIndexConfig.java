/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestIndexConfig {

  @Test
  public void usesDefaultsWhenUnset() {
    IndexConfig config = new IndexConfig(new Configuration(false));
    assertEquals(IndexConfig.INDEX_NAME_DEFAULT, config.indexName());
    assertEquals(IndexConfig.INDEX_RAM_SIZE_DEFAULT, config.getWriteBufferSize());
    assertEquals(Duration.ofMillis(IndexConfig.FLUSH_INTERVAL_MS_DEFAULT), config.getFlushInterval());
  }

  @Test
  public void readsConfiguredValues() {
    Configuration conf = new Configuration(false);
    conf.set(IndexConfig.INDEX_NAME, "custom_index");
    conf.setInt(IndexConfig.BOOTSTRAP_BATCH_SIZE, 500);
    IndexConfig config = new IndexConfig(conf);
    assertEquals("custom_index", config.indexName());
    assertEquals(500, config.getBootstrapBatchSize());
  }
}
