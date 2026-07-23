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

package org.apache.hive.search.search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.exception.SearchException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestHybridSearch {

  @Test
  public void resolvedQueryUsesDefaultSemanticWeightFromSearchConfig() throws SearchException {
    HybridQuery resolved = new HybridQuery("sales", null, null);
    SearchOptions searchConfig = new SearchOptions(new Configuration(false));
    assertEquals(searchConfig.getHybridSemanticWeight(), resolved.semanticWeight(searchConfig), 0.001f);
    assertEquals(searchConfig.getHybridMatchWeight(), 1.0f - resolved.semanticWeight(searchConfig), 0.001f);
  }

  @Test
  public void resolvedQueryKeepsExplicitSemanticWeight() throws SearchException {
    HybridQuery resolved = new HybridQuery("sales", null, 0.3f);
    SearchOptions searchConfig = new SearchOptions(new Configuration(false));
    assertEquals(0.3f, resolved.semanticWeight(searchConfig), 0.001f);
  }
}
