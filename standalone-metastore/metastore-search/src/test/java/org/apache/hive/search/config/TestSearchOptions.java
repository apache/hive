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
import org.apache.hive.search.exception.SearchException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestSearchOptions {

  @Test
  public void usesDefaultsWhenUnset() throws SearchException {
    SearchOptions config = new SearchOptions(new Configuration(false));
    assertEquals(Duration.ofSeconds(SearchOptions.REFRESH_INTERVAL_SECONDS_DEFAULT),
        config.getRefreshInterval());
    assertEquals(SearchOptions.DEFAULT_LIMIT_DEFAULT, config.getDefaultLimit());
    assertEquals(SearchOptions.HYBRID_SEMANTIC_WEIGHT_DEFAULT, config.getHybridSemanticWeight(), 0.001f);
  }

  @Test
  public void readsConfiguredValues() throws SearchException  {
    Configuration conf = new Configuration(false);
    conf.setInt(SearchOptions.DEFAULT_LIMIT, 50);
    conf.setFloat(SearchOptions.HYBRID_SEMANTIC_WEIGHT, 0.2f);
    SearchOptions config = new SearchOptions(conf);
    assertEquals(50, config.getDefaultLimit());
    assertEquals(0.8f, config.getHybridMatchWeight(), 0.001f);
  }
}
